package main

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"math/rand"
	"path"
	"sync"
	"time"
)

const (
	// MaxList is the maximum number of keys to accept from a call to LIST an s3
	// prefix.
	MaxList    = 10000
	RetryLimit = 10
)

func awsBucket(a awsConfig) *s3.Bucket {
	return s3.New(
		aws.Auth{
			AccessKey: a.AccessKey,
			SecretKey: a.SecretKey,
		}, aws.Regions[a.Region],
	).Bucket(a.Bucket)
}

type verifier struct {
	cfg   *config
	abort <-chan struct{}
	src   *s3.Bucket
	dst   *s3.Bucket

	model bucketModel
}

func newVerifier(cfg *config, model bucketModel, abort <-chan struct{}) (*verifier, error) {
	if model.name != cfg.Source.Bucket {
		return nil, fmt.Errorf("can't verify bucket %q with a model built for bucket %q",
			cfg.Source.Bucket, model.name)
	}

	return &verifier{
		cfg:   cfg,
		abort: abort,
		src:   awsBucket(cfg.Source),
		dst:   awsBucket(cfg.Destination),
		model: model,
	}, nil
}

func (v *verifier) execute() error {
	tick := time.NewTicker(v.cfg.CheckFrequency)
	r := rand.New(rand.NewSource(v.cfg.RandomSeed))

	log.Info("starting verifier")
	for {
		now := time.Now()
		log.Info("starting an audit")
		if err := v.verifySamples(r, now); err != nil {
			return err
		}
		select {
		case <-v.abort:
			log.Warn("verifier aborting")
			return nil
		case <-tick.C:
		}
	}
}

func (v *verifier) verifySamples(r *rand.Rand, now time.Time) error {
	oldest := now.Add(-v.cfg.CheckOldest)
	youngest := now.Add(-v.cfg.CheckYoungest)

	constraint := func(k s3.Key) bool {
		modtime, err := time.Parse(time.RFC3339Nano, k.LastModified)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"key":   k.Key,
			}).Error("couldn't parse LastModified time for this key")
			return false
		}
		llog := log.WithField("modtime", modtime)
		if !modtime.After(oldest) {
			llog.Debug("decided it's too old")
			return false
		}
		if !modtime.Before(youngest) {
			llog.Debug("decided it's too young")
			return false
		}
		llog.Debug("right time range")
		return true
	}

	log.Infof("randomly sampling %d keys from bucket %q", v.cfg.CheckCount, v.src.Name)
	keys, err := v.sampleKeysWithConstraint(r, constraint)
	if err != nil {
		log.WithField("error", err).Error("couldn't sample keys from source bucket")
		return err
	}

	log.Infof("verifying all keys match in bucket %q", v.dst.Name)
	if err := v.verifyKeysMatch(keys); err != nil {
		log.WithField("error", err).Error("couldn't sample keys from source bucket")
		return err
	}
	return nil
}

func (v *verifier) sampleKeysWithConstraint(r *rand.Rand, accept func(s3.Key) bool) ([]s3.Key, error) {
	count := v.cfg.CheckCount
	set := make(map[s3.Key]struct{}, count)

	for len(set) != count {
		var wg sync.WaitGroup
		sampleC := make(chan s3.Key, count)
		errC := make(chan error, count)

		select {
		case <-v.abort:
			log.Warn("verifier: aborting keys sampling")
			return nil, nil
		default:
		}
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				log.WithField("samples", len(set)).Debug("sampling a random key")
				sample, err := v.sampleRandomKey(r, accept)
				if err != nil {
					errC <- err
				} else {
					sampleC <- *sample
				}
			}()
		}

		wg.Wait()
		close(sampleC)
		close(errC)
		for sample := range sampleC {
			set[sample] = struct{}{}
		}

		if err := <-errC; err != nil {
			return nil, err
		}
		log.WithField("samples", len(set)).Debug("found samples")

	}
	keys := make([]s3.Key, 0, count)
	for k := range set {
		keys = append(keys, k)
	}
	return keys, nil
}

func (v *verifier) sampleRandomKey(r *rand.Rand, accept func(s3.Key) bool) (*s3.Key, error) {

	// TODO: find a real answer to the question
	//   - How to uniformly select a random node in a tree without knowing in
	//     advance the structure of the tree, and if it's not practical to
	//     traverse the whole tree?
	maybePickKey := func(depth int, key s3.Key) bool {
		p := v.probThatKeyAtDepth(depth)
		dice := r.Float64()
		accepted := dice <= p
		log.WithFields(log.Fields{
			"dice":     dice,
			"p":        p,
			"accepted": accepted,
		}).Warn("rolling dice")
		return accepted
	}

	var walkNode func(depth int, prefix string) (*s3.Key, bool, error)

	walkNode = func(depth int, prefix string) (*s3.Key, bool, error) {

		select {
		case <-v.abort:
			log.WithField("depth", depth).Warn("verifier: aborting bucket random walk")
			return nil, false, nil
		default:
		}
		log.WithFields(log.Fields{
			"depth":  depth,
			"prefix": prefix,
		}).Debug("walking a depth")

		// enumerate the keys and the children from here
		resp, err := listBkt(v.src, normalizePath(prefix), MaxList)
		if err != nil {
			return nil, false, err
		}

		candidates, err := filterKeys(resp.Contents, accept)
		if err != nil {
			return nil, false, err
		}
		shuffleKeys(r, candidates)

		log.WithFields(log.Fields{
			"initial": len(resp.Contents),
			"left":    len(candidates),
		}).Debug("applied constraint")

		// maybe stop recursing
		for _, key := range candidates {
			if picked := maybePickKey(depth, key); picked {
				return &key, true, nil
			}
		}
		log.WithFields(log.Fields{
			"depth":  depth,
			"prefix": prefix,
		}).Debug("rejected all candidates")

		// otherwise traverse to a random children
		shuffle(r, resp.CommonPrefixes)
		for _, pfx := range resp.CommonPrefixes {
			key, found, err := walkNode(depth+1, pfx)
			if err != nil {
				return nil, false, err
			}
			if found {
				return key, true, err
			}
		}
		return nil, false, nil
	}

	k, found, err := walkNode(0, "/")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("traversed whole bucket without choosing a key")
	}
	return k, nil
}

func (v *verifier) verifyKeysMatch(keys []s3.Key) error {
	for _, key := range keys {
		select {
		case <-v.abort:
			log.Warn("verifier: aborting verification that keys match")
			return nil
		default:
		}
		if err := v.verifyKey(key); err != nil {
			return err
		}
	}
	return nil
}

func listBkt(bkt *s3.Bucket, path string, limit int) (*s3.ListResp, error) {
	var resp *s3.ListResp
	var err error
	for i := 0; i < RetryLimit; i++ {
		resp, err = bkt.List(path, "/", "", limit)
		if err != nil {
			return resp, err
		}
	}
	return resp, err
}

func (v *verifier) verifyKey(want s3.Key) error {
	log.WithField("key", want.Key).Debug("verifying a key")

	res, err := listBkt(v.dst, want.Key, 1)
	if err != nil {
		return err
	}
	if len(res.Contents) == 0 {

	}
	switch {
	case len(res.Contents) == 0:
		log.WithField("key", want.Key).Error("mismatch at key, no match in destination")
		return nil

	case len(res.Contents) > 1:
		log.WithField("key", want.Key).Error("mismatch at key, more than one match in destination")
		return nil
	}

	got := res.Contents[0]
	logFields := log.Fields{}
	if want.ETag != got.ETag {
		logFields["want.etag"] = want.ETag
		logFields["got.etag"] = got.ETag
	}
	if want.Size != got.Size {
		logFields["want.size"] = want.Size
		logFields["got.size"] = got.Size
	}
	if len(logFields) != 0 {
		logFields["key"] = want.Key
		log.WithFields(logFields).Error("mismatch at key, different properties")
	}
	return nil
}

func (v *verifier) probThatKeyAtDepth(depth int) float64 {
	if depth >= len(v.model.depths) {
		log.WithField("depth", depth).Warn("depth not predictable by model")
		return 0.0
	}
	keysAtDepth := v.model.depths[depth]
	return float64(keysAtDepth) / float64(v.model.keyCount)
}

func normalizePath(p string) string {
	if path.IsAbs(p) {
		return p[1:]
	}
	return p
}

func filterKeys(candidates []s3.Key, accept func(s3.Key) bool) ([]s3.Key, error) {
	var valids []s3.Key
	for _, k := range candidates {
		if ok := accept(k); ok {
			valids = append(valids, k)
		}
	}
	return valids, nil
}

func shuffle(r *rand.Rand, arr []string) {
	for i := range arr {
		j := r.Intn(i + 1)
		arr[i], arr[j] = arr[j], arr[i]
	}
}

func shuffleKeys(r *rand.Rand, arr []s3.Key) {
	for i := range arr {
		j := r.Intn(i + 1)
		arr[i], arr[j] = arr[j], arr[i]
	}
}
