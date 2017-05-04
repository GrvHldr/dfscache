package config

import (
	"encoding/json"
	"github.com/GrvHldr/dfscache/logger"
	"os"
	"strconv"
)

type zmqConfig struct {
	LISTEN_DOWNLOAD       string
	LISTEN_UPLOAD         string
	DOWNLOAD_PIPELINE     int
	NUM_UPLOAD_WORKERS    int
	Z85_PUBLIC_KEY        string
	Z85_PRIVATE_KEY       string
	Z85_PUBLIC_CLIENT_KEY string
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

type SetFlagString struct {
	IsSet bool
	Val   string
}

func (f *SetFlagString) String() string {
	return f.Val
}

func (f *SetFlagString) Set(s string) error {
	f.IsSet = true
	f.Val = s

	return nil
}

type SetFlagInt struct {
	IsSet bool
	Val   int
}

func (f *SetFlagInt) String() string {
	return string(f.Val)
}

func (f *SetFlagInt) Set(s string) error {
	f.IsSet = true
	v, err := strconv.ParseInt(s, 0, 64)
	f.Val = int(v)

	return err
}

type SetFlagInt64 struct {
	IsSet bool
	Val   int64
}

func (f *SetFlagInt64) String() string {
	return string(f.Val)
}

func (f *SetFlagInt64) Set(s string) error {
	f.IsSet = true
	v, err := strconv.ParseInt(s, 0, 64)
	f.Val = v

	return err
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
