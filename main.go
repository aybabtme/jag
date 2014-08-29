package main

import (
	log "github.com/Sirupsen/logrus"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {

	n := runtime.NumCPU()
	runtime.GOMAXPROCS(n)

	abort := make(chan struct{}, 0)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	go func() {
		<-sig
		log.Warn("received SIGTERM, aborting")
		close(abort)
	}()

	newApp(abort).Run(os.Args)
}
