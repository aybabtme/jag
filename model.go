package main

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"launchpad.net/goamz/s3"
	"strings"
)

type bucketModel struct {
	// TODO add age of keys per bucket/per depth
	name     string
	depths   []int
	keyCount int
}

func (b bucketModel) MarshalJSON() ([]byte, error) {
	type depthLevel struct {
		Level int `json:"level"`
		Count int `json:"count"`
	}
	depths := make([]depthLevel, len(b.depths))
	for i, count := range b.depths {
		depths[i] = depthLevel{
			Level: i,
			Count: count,
		}
	}
	return json.MarshalIndent(struct {
		Name     string       `json:"bucket_name"`
		Depth    []depthLevel `json:"depths"`
		KeyCount int          `json:"key_count"`
	}{Name: b.name, Depth: depths, KeyCount: b.keyCount}, "", "   ")
}

func (b *bucketModel) UnmarshalJSON(p []byte) error {
	type depthLevel struct {
		Level int `json:"level"`
		Count int `json:"count"`
	}
	var d struct {
		Name     string       `json:"bucket_name"`
		Depth    []depthLevel `json:"depths"`
		KeyCount int          `json:"key_count"`
	}
	err := json.Unmarshal(p, &d)
	b.name = d.Name
	b.depths = make([]int, len(d.Depth))
	for _, depthL := range d.Depth {
		b.depths[depthL.Level] = depthL.Count
	}
	b.keyCount = d.KeyCount
	return err
}

func buildModel(name string, keys <-chan interface{}, abort <-chan struct{}) *bucketModel {
	log.Info("computing model...")
	defer log.Info("done!")
	depthMap := make(map[int]int)
	count := 0
	maxDepth := 0
loop:
	for key := range keys {
		select {
		case <-abort:
			log.Warn("aborting build of model")
			break loop
		default:
		}
		count++
		depth := strings.Count(key.(*s3.Key).Key, "/")
		depthMap[depth]++
		if depth > maxDepth {
			maxDepth = depth
		}
	}

	depths := make([]int, maxDepth+1)
	for d, count := range depthMap {
		depths[d] = count
	}

	return &bucketModel{
		name:     name,
		depths:   depths,
		keyCount: count,
	}
}
