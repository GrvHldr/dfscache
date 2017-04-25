package main

import (
	"flag"
	"github.com/GrvHldr/dfscache/server"
	"github.com/GrvHldr/dfscache/config"
)

func init() {
	var cfgfile string
	var interval config.SetFlagInt

	flag.StringVar(&cfgfile, "config", "config.json", "Server JSON config file name")
	flag.Var(&interval, "interval", "Garbage Collector interval time")
	flag.Parse()
	config.Initialize(cfgfile)

	if interval.IsSet {
		config.Config.CEPH_OPTIONS.GC_RUN_INTERVAL = interval.Val
	}
}

func main() {
	server.GarbageCollector()
}
