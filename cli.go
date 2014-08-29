package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/aybabtme/parajson"
	"github.com/codegangsta/cli"
	"io"
	"launchpad.net/goamz/s3"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func newApp(abort <-chan struct{}) *cli.App {
	app := cli.NewApp()
	app.Name = "jag"
	app.Author = "Antoine Grondin"
	app.Email = "antoinegrondin@gmail.com"
	app.Usage = "Audits brigade to see if it does its work properly."
	app.Version = "0.1"
	app.Commands = []cli.Command{
		createConfigCommand(),
		auditCommand(abort),
		printModelCommand(abort),
	}

	return app
}

func createConfigCommand() cli.Command {
	cfgFlag := cli.StringFlag{
		Name:  "cfg",
		Usage: "path to the JSON config file",
		Value: "config.json",
	}

	doCreateConfig := func(ctx *cli.Context) {
		filename := mustString(ctx, cfgFlag)
		cfg := config{
			RandomSeed:     42,
			CheckCount:     30,
			CheckYoungest:  time.Hour * 24 * 2,
			CheckOldest:    time.Hour * 24 * 14,
			CheckFrequency: time.Minute * 20,
			Source: awsConfig{
				Bucket:    "my_bucket",
				Region:    "us-east-1",
				AccessKey: "something",
				SecretKey: "somethingelse",
			},
			Destination: awsConfig{
				Bucket:    "my_bucket",
				Region:    "us-east-1",
				AccessKey: "something",
				SecretKey: "somethingelse",
			},
		}
		file, err := os.Create(filename)
		if err != nil {
			fail(ctx, "error: can't create file %q: %v", filename, err)
		}
		defer func() { _ = file.Close() }()
		data, err := cfg.MarshalJSON()
		if err != nil {
			fail(ctx, "bug: can't create sample JSON: %v", err)
		}
		if _, err := file.Write(data); err != nil {
			fail(ctx, "bug: can't write config to file: %v", err)
		}
	}

	return cli.Command{
		Name:   "makeconfig",
		Usage:  "Create a sample config file at the specified path.",
		Flags:  []cli.Flag{cfgFlag},
		Action: doCreateConfig,
	}
}

func auditCommand(abort <-chan struct{}) cli.Command {
	cfgFlag := cli.StringFlag{
		Name:  "cfg",
		Usage: "path to the JSON config file",
	}
	buildModelFlag := cli.StringFlag{
		Name:  "build-model",
		Usage: "path to a gzip'd JSON file representing all the keys in the source bucket",
	}
	modelFlag := cli.StringFlag{
		Name:  "model",
		Usage: "path to a JSON file representing model of the keys in the source bucket",
	}

	doAudit := func(ctx *cli.Context) {

		go func() {
			time.Sleep(time.Second)
			// exposes pprof
			addr := "127.0.0.1:6060"
			log.Infof("listening on http://%s/debug/pprof", addr)
			http.ListenAndServe(addr, nil)
		}()

		cfg := mustConfig(ctx, cfgFlag)
		var model *bucketModel
		if ctx.String(buildModelFlag.Name) != "" {
			model = mustBuildModel(ctx, buildModelFlag, abort)
		} else {
			model = mustRetrieveModel(ctx, modelFlag)
		}

		v := newVerifier(cfg, *model, abort)
		if err := v.execute(); err != nil {
			log.Fatalln(err)
		}
	}

	return cli.Command{
		Name:  "audit",
		Usage: "Continuously samples keys in two buckets, check that they match.",
		Description: strings.TrimSpace(`
Audits the keys of two buckets match, picking keys to audit randomly based on
a model built from an existing list of the source bucket.`),
		Flags:  []cli.Flag{cfgFlag, modelFlag, buildModelFlag},
		Action: doAudit,
	}
}

func printModelCommand(abort <-chan struct{}) cli.Command {
	modelFlag := cli.StringFlag{
		Name:  "file",
		Usage: "path to a gzip'd JSON file representing all the keys in the source bucket",
	}

	doPrintModel := func(ctx *cli.Context) {
		model := mustBuildModel(ctx, modelFlag, abort)
		data, err := model.MarshalJSON()
		if err != nil {
			fail(ctx, "bug: can't create model JSON: %v", err)
		}
		if _, err := os.Stdout.Write(data); err != nil {
			fail(ctx, "bug: can't write model to stdout: %v", err)
		}
	}

	return cli.Command{
		Name:  "model",
		Usage: "Computes and prints a model for the given bucket listing.",
		Description: strings.TrimSpace(`
Takes the listing of a bucket, in JSON form, and computes statistical data
about it, then prints them.`),
		Flags:  []cli.Flag{modelFlag},
		Action: doPrintModel,
	}
}

func mustString(c *cli.Context, f cli.StringFlag) string {
	s := c.String(f.Name)
	if s == "" && f.Value == "" {
		fail(c, "required: flag %q must have a value", f.Name)
	}
	return s
}

func mustOpen(ctx *cli.Context, filename string) *os.File {
	f, err := os.Open(filename)
	if err != nil {
		fail(ctx, "error: can't open file %q: %v", filename, err)
	}
	return f
}

func mustConfig(ctx *cli.Context, f cli.StringFlag) *config {
	filename := mustString(ctx, f)
	file := mustOpen(ctx, filename)
	defer func() { _ = file.Close() }()
	cfg, err := loadConfig(file)
	if err != nil {
		fail(ctx, "can't create config from file %q: %v", filename, err)
	}
	return cfg
}

func mustBuildModel(ctx *cli.Context, f cli.StringFlag, abort <-chan struct{}) *bucketModel {
	filename := mustString(ctx, f)
	file := mustOpen(ctx, filename)
	defer func() { _ = file.Close() }()
	var rd io.Reader
	if filepath.Ext(filename) == ".gz" {
		var err error
		rd, err = gzip.NewReader(file)
		if err != nil {
			fail(ctx, "can't create gzip stream from file %q: %v", filename, err)
		}
	} else {
		rd = file
	}

	n := runtime.NumCPU()
	ifaceC, errc := parajson.Decode(rd, n, func() interface{} {
		return &s3.Key{}
	})

	sem := make(chan struct{}, 1)
	go func() {
		for err := range errc {
			fail(ctx, "error: reading keys from list: %v", err)
		}
		sem <- struct{}{}
	}()
	model := buildModel(ifaceC, abort)
	<-sem
	return model
}

func mustRetrieveModel(ctx *cli.Context, f cli.StringFlag) *bucketModel {
	filename := mustString(ctx, f)
	file := mustOpen(ctx, filename)
	defer func() { _ = file.Close() }()
	var model bucketModel
	err := json.NewDecoder(file).Decode(&model)
	if err != nil {
		fail(ctx, "error: can't load model from file %q: %v", filename, err)
	}
	return &model
}

func fail(c *cli.Context, format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	cli.ShowCommandHelp(c, c.Command.Name)
	os.Exit(1)
}
