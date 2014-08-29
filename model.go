package main

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"launchpad.net/goamz/s3"
	"strings"
)

type bucketModel struct {
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
		Depth    []depthLevel `json:"depths"`
		KeyCount int          `json:"key_count"`
	}{Depth: depths, KeyCount: b.keyCount}, "", "   ")
}

func buildModel(keys <-chan *s3.Key, abort <-chan struct{}) *bucketModel {
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
		depth := strings.Count(key.Key, "/")
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
		depths:   depths,
		keyCount: count,
	}
}
