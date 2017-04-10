package main

import (
	"github.com/GrvHldr/dfscache/server"
	"github.com/GrvHldr/dfscache/cephutils"
	"github.com/GrvHldr/dfscache/logger"
	"time"
	"github.com/ceph/go-ceph/rados"
)

// Goroutine looking for expired object in storage and deletes expired
func garbage_collector() {
	conn, err := cephutils.NewRadosConn()
	if err != nil {
		logger.Log.Errorf("Can't create new Rados connection: %s. Exiting", err)
		return
	}
	defer conn.Shutdown()

	logger.Log.Info("Started")

	delObj := func(ioctx *rados.IOContext, oid string) {
		err = ioctx.Delete(oid)
		if err != nil {
			logger.Log.Errorf("Can't delete object %s: %s", oid, err)
			return
		}
		logger.Log.Infof("Deleted object %s", oid)
	}

	ticker := time.NewTicker(time.Duration(5) * time.Second)
	for {
		select {
		case <-ticker.C:
			pools, err := conn.ListPools()
			if err != nil {
				logger.Log.Error("Can't get pool list: ", err)
				continue
			}

			for _, pool := range pools {
				ioctx, err := conn.OpenIOContext(pool)
				if err != nil {
					logger.Log.Errorf("Can't opent pool (%s): %s", pool, err)
					continue
				}

				err = ioctx.ListObjects(func(oid string) {
					ttl, err := cephutils.GetObjTTL(ioctx, oid)
					if err != nil {
						// Do nothing if no TTL attr
						return
					}

					now := time.Duration(time.Now().UTC().Unix())
					if now > ttl && !cephutils.IsObjectLocked(ioctx, oid) {
						delObj(ioctx, oid)
					}
				})
				if err != nil {
					logger.Log.Errorf("Can't list objects within pool (%s): %s", pool, err)
					ioctx.Destroy()
					continue
				}

				ioctx.Destroy()
			}
		}
	}
}

func main() {
	//ZMQ server
	go server.BindZMq()

	// Run GC
	go garbage_collector()

	// Run HTTP server
	server.Run()

}
