package main

import (
	"encoding/json"
	"errors"
	"io"
	"time"
)

type awsConfig struct {
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

type config struct {
	RandomSeed     int64
	CheckCount     int
	CheckYoungest  time.Duration
	CheckOldest    time.Duration
	CheckFrequency time.Duration
	Source         awsConfig
	Destination    awsConfig
}

func loadConfig(r io.Reader) (*config, error) {
	var d struct {
		RandomSeed     int64     `json:"random_seed"`
		CheckCount     uint      `json:"check_count"`
		CheckYoungest  string    `json:"check_youngest"`
		CheckOldest    string    `json:"check_oldest"`
		CheckFrequency string    `json:"check_frequency"`
		Source         awsConfig `json:"source"`
		Destination    awsConfig `json:"destination"`
	}
	err := json.NewDecoder(r).Decode(&d)
	if err != nil {
		return nil, err
	}

	c := &config{
		CheckCount:  int(d.CheckCount),
		Source:      d.Source,
		Destination: d.Destination,
	}
	c.CheckYoungest, err = time.ParseDuration(d.CheckYoungest)
	if err != nil {
		return nil, err
	}
	c.CheckOldest, err = time.ParseDuration(d.CheckOldest)
	if err != nil {
		return nil, err
	}
	if c.CheckOldest <= c.CheckYoungest {
		return nil, errors.New("cannot look for events where oldest is less or equal to youngest")
	}

	c.CheckFrequency, err = time.ParseDuration(d.CheckFrequency)
	if err != nil {
		return nil, err
	}

	return c, err
}

func (c *config) MarshalJSON() ([]byte, error) {
	return json.MarshalIndent(struct {
		RandomSeed     int64     `json:"random_seed"`
		CheckCount     uint      `json:"check_count"`
		CheckYoungest  string    `json:"check_youngest"`
		CheckOldest    string    `json:"check_oldest"`
		CheckFrequency string    `json:"check_frequency"`
		Source         awsConfig `json:"source"`
		Destination    awsConfig `json:"destination"`
	}{
		RandomSeed:     c.RandomSeed,
		CheckCount:     uint(c.CheckCount),
		CheckYoungest:  c.CheckYoungest.String(),
		CheckOldest:    c.CheckOldest.String(),
		CheckFrequency: c.CheckFrequency.String(),
		Source:         c.Source,
		Destination:    c.Destination,
	}, "", "   ")
}
