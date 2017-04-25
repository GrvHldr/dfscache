package main

import (
	"flag"
	"github.com/GrvHldr/dfscache/server"
	"github.com/GrvHldr/dfscache/config"
)

func init() {
	var cfgfile string
	var listenUpload, listenDownload config.SetFlagString
	var pipeline, workers config.SetFlagInt

	flag.StringVar(&cfgfile, "config", "config.json", "Server JSON config file name")
	flag.Var(&listenUpload, "listen_uploader", "Listen ZMQ uploader on specified address. E.g: 'tcp://0.0.0.0:5555'")
	flag.Var(&listenDownload, "listen_downloader", "Listen ZMQ downloader on specified address. E.g: 'tcp://0.0.0.0:6666'")
	flag.Var(&pipeline, "download_pipeline", "Downloader pipeline buffer size")
	flag.Var(&workers, "uploader_workers_num", "Number of uploader workers")
	flag.Parse()
	config.Initialize(cfgfile)

	if listenUpload.IsSet {
		config.Config.ZMQ_OPTIONS.LISTEN_UPLOAD = listenUpload.Val
	}
	if listenDownload.IsSet {
		config.Config.ZMQ_OPTIONS.LISTEN_DOWNLOAD = listenDownload.Val
	}
	if pipeline.IsSet {
		config.Config.ZMQ_OPTIONS.DOWNLOAD_PIPELINE = pipeline.Val
	}
	if workers.IsSet {
		config.Config.ZMQ_OPTIONS.NUM_UPLOAD_WORKERS = workers.Val
	}
}

func main() {
	//ZMQ downloader server
	go server.BindZMqDownloader()

	//ZMQ uploader server
	server.BindZMqUploader()
}
