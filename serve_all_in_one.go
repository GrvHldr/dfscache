package main

import (
	"flag"
	"github.com/GrvHldr/dfscache/server"
	"github.com/GrvHldr/dfscache/config"
)

func init() {
	var fname string
	flag.StringVar(&fname, "config", "config.json", "Server JSON config file name")
	flag.Parse()
	config.Initialize(fname)
}

func main() {
	//ZMQ downloader server
	go server.BindZMqDownloader()

	//ZMQ uploader server
	go server.BindZMqUploader()

	// Run GC
	go server.GarbageCollector()

	// Run HTTP server
	server.Run()
}
