package main

import (
	"flag"
	"github.com/GrvHldr/dfscache/server"
	"github.com/GrvHldr/dfscache/config"
)

func init() {
	var cfgfile string
	var listen, contentFieldName, certFile, certKey config.SetFlagString
	var maxmem config.SetFlagInt64

	flag.StringVar(&cfgfile, "config", "config.json", "Server JSON config file name")
	flag.Var(&listen, "http_listen", "Listen HTTP on specified address. E.g: '0.0.0.0:8080'")
	flag.Var(&contentFieldName, "post_content_field_name", "Specify POST multipart form field name")
	flag.Var(&maxmem, "max_mem_usage", "Max memory usage on POST multipart form data")
	flag.Var(&certFile, "cert_file", "x509 public key file path")
	flag.Var(&certKey, "cert_key", "x509 private key file path")
	flag.Parse()
	config.Initialize(cfgfile)

	if listen.IsSet {
		config.Config.HTTP_OPTIONS.LISTEN = listen.Val
	}
	if contentFieldName.IsSet {
		config.Config.HTTP_OPTIONS.HTTP_UPLOAD_CONTENT_FIELD_NAME = contentFieldName.Val
	}
	if maxmem.IsSet {
		config.Config.HTTP_OPTIONS.MAX_MEMORY_FORM_PARSE = maxmem.Val
	}
	if certFile.IsSet {
		config.Config.HTTP_OPTIONS.CERT_FILE = certFile.Val
	}
	if certKey.IsSet {
		config.Config.HTTP_OPTIONS.CERT_KEY_FILE = certKey.Val
	}
}

func main() {
	server.Run()
}
