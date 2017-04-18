package server

import (
	"encoding/binary"
	"errors"
	"github.com/GrvHldr/dfscache/cephutils"
	"github.com/GrvHldr/dfscache/config"
	"github.com/GrvHldr/dfscache/logger"
	zmq "github.com/pebbe/zmq4"
	"sync"
)

var mu sync.Mutex
var zClientsMap = make(zClients)

type zClients map[string]*cephutils.LockRadosObj

func (z zClients) IsRegistered(zid string) bool {
	_, ok := z[zid]
	return ok
}

func (z zClients) RegisterNew(zid, filename string, filesize uint64) error {
	mu.Lock()
	defer mu.Unlock()

	if b := z.IsRegistered(zid); b {
		return errors.New("ZMQ client already registered")
	}

	obj, err := cephutils.NewRadosObj(filename)
	if err != nil {
		return err
	}
	obj.Size = filesize // set total file size

	z[zid] = &cephutils.LockRadosObj{RadosObj: *obj}

	logger.Log.Debugf("Registered ZMQ client %s; filename: %s, file size: %d", z[zid].Oid, filename, filesize)

	return nil
}

func (z zClients) Unregister(zid string) error {
	mu.Lock()
	defer mu.Unlock()

	if b := z.IsRegistered(zid); !b {
		return errors.New("ZMQ client is not registered")
	}

	obj := z[zid]
	obj.Destroy()
	logger.Log.Debugf("Unregistered ZMQ client %s", z[zid].Oid)
	delete(z, zid)

	return nil
}

func BindZMqUploader() {
	// Listen frontend
	frontend, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		logger.Log.Error(err)
		return
	}
	defer frontend.Close()
	frontend.SetRcvhwm(1)
	frontend.SetSndhwm(1)
	err = frontend.Bind(config.Config.ZMQ_OPTIONS.LISTEN_UPLOAD)
	if err != nil {
		logger.Log.Error(err)
		return
	}

	// Listen backend
	backend, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		logger.Log.Error(err)
		return
	}
	defer backend.Close()
	err = backend.Bind("inproc://backend")
	if err != nil {
		logger.Log.Error(err)
		return
	}

	// Start backend workers
	for i := 0; i < 5; i++ {
		z := i
		go backendWorker(z)
	}

	logger.Log.Infof("Started ZMQ uploader on %s", config.Config.ZMQ_OPTIONS.LISTEN_UPLOAD)

	// Start ZMQ proxy between backend and frontend
	err = zmq.Proxy(frontend, backend, nil)
	logger.Log.Fatal(err)
}

func backendWorker(i int) {
	intbuf := make([]byte, 8)
	sock, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		logger.Log.Error(err)
		return
	}
	defer sock.Close()
	err = sock.Connect("inproc://backend")
	if err != nil {
		logger.Log.Error(err)
		return
	}

	for {
		parts, err := sock.RecvMessageBytes(0)
		if err != nil {
			logger.Log.Error(err)
			return
		}

		identity := string(parts[0])
		if !zClientsMap.IsRegistered(identity) {
			// Client is not registered. Header received
			size := binary.LittleEndian.Uint64(parts[2])
			if err = zClientsMap.RegisterNew(identity, string(parts[1]), size); err == nil {
				sock.SendMessage(identity, "ACK")
			} else {
				sock.SendMessage(identity, "NAK")
			}
			continue
		}

		// Client is registered. Data chunks
		chunk := parts[1]
		// In current implementation chunks go one by one in series
		o := zClientsMap[identity]
		o.Lock()
		_, err = o.Write(chunk)
		if err != nil {
			logger.Log.Error("Can't write chunk to Ceph", err)
			o.Unlock()
			sock.SendMessage(identity, "NAK")
			zClientsMap.Unregister(identity)
			continue
		}
		progress := o.WriteProgress()
		binary.LittleEndian.PutUint64(intbuf, progress)
		sock.SendMessage(identity, intbuf)
		o.Unlock()

		if progress == o.Size {
			logger.Log.Infof("Transfer finished for %s", o.Oid)
			err = o.SyncAttributes()
			if err != nil {
				logger.Log.Error("Can't sync Rados attrs:", err)
			}
			zClientsMap.Unregister(identity)
		}
	}
}
