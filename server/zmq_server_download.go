package server

import (
	"github.com/GrvHldr/dfscache/cephutils"
	"github.com/GrvHldr/dfscache/config"
	"github.com/GrvHldr/dfscache/logger"
	zmq "github.com/pebbe/zmq4"
	"github.com/satori/go.uuid"
	"strconv"
)

func BindZMqDownloader() {
	router, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		logger.Log.Error(err)
		return
	}
	defer router.Close()
	router.SetRcvhwm(config.Config.ZMQ_OPTIONS.DOWNLOAD_PIPELINE * 2)
	router.SetSndhwm(config.Config.ZMQ_OPTIONS.DOWNLOAD_PIPELINE * 2)

	err = router.Bind(config.Config.ZMQ_OPTIONS.LISTEN_DOWNLOAD)
	if err != nil {
		logger.Log.Error(err)
		return
	}

	logger.Log.Infof("Started ZMQ downloader on %s", config.Config.ZMQ_OPTIONS.LISTEN_DOWNLOAD)

	for {
		msg, err := router.RecvMessage(0)
		if err != nil {
			logger.Log.Error(err)
			break
		}
		identity, stroid, stroffset, strchunksize := msg[0], msg[1], msg[2], msg[3]

		var oid uuid.UUID
		err = oid.Scan(stroid)
		if err != nil {
			logger.Log.Error("Invalid OID: ", err)
			router.SendMessage(identity, []byte{})
			continue
		}

		offset, err := strconv.ParseInt(stroffset, 10, 64)
		if err != nil {
			logger.Log.Error("Invalid offset: ", err)
			router.SendMessage(identity, []byte{})
			continue
		}

		chunksize, err := strconv.Atoi(strchunksize)
		if err != nil {
			logger.Log.Error("Invalid offset: ", err)
			router.SendMessage(identity, []byte{})
			continue
		}

		pool := config.Config.CEPH_OPTIONS.POOL_NAMES_PREFIX + string(stroid[:2])
		obj, err := cephutils.ExistingRadosObj(pool, oid)
		if err != nil {
			logger.Log.Errorf("Rados object (%s) fetch error: %s", stroid, err)
			router.SendMessage(identity, []byte{})
			continue
		}

		chunk := make([]byte, chunksize)
		n, _ := obj.ReadAt(chunk, offset)
		obj.Destroy()

		_, err = router.SendMessage(identity, chunk[:n])
		if err != nil {
			logger.Log.Errorf("ZMQ send message error: %s", err)
			continue
		}
	}
}
