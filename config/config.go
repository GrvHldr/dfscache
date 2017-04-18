package config

import (
	"encoding/json"
	"github.com/GrvHldr/dfscache/logger"
	"os"
)

type zmqConfig struct {
	LISTEN_DOWNLOAD   string
	LISTEN_UPLOAD     string
	DOWNLOAD_PIPELINE int
}

type httpConfig struct {
	LISTEN                         string
	MAX_MEMORY_FORM_PARSE          int64
	HTTP_UPLOAD_CONTENT_FIELD_NAME string
}

type cephConfig struct {
	CONFIG_FILE       string
	POOL_NAMES_PREFIX string
	OBJECT_TTL        int
	GC_RUN_INTERVAL   int
	RW_BUFFER_SIZE    int
}

type serverConfig struct {
	CEPH_OPTIONS cephConfig
	ZMQ_OPTIONS  zmqConfig
	HTTP_OPTIONS httpConfig
}

var Config = new(serverConfig)

func Initialize(cfgFile string) {
	fd, err := os.Open(cfgFile)
	if err != nil {
		logger.Log.Fatal("Can't open config file:", err)
	}

	decoder := json.NewDecoder(fd)
	if err = decoder.Decode(Config); err != nil {
		logger.Log.Fatal("Configuration error:", err)
	}
}
