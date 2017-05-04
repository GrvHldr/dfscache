// ZeroMQ client example

package main

import "fmt"
import (
	"flag"
	"github.com/GrvHldr/dfscache/logger"
	zmq "github.com/pebbe/zmq4"
	"os"
)

func main() {
	const (
		ZMQCHUNKSIZE = 25000
		SERVER_PUBLIC_KEY = "3>v/vSk6K(WoH?&[lNt@PKBJbj&13xL^B3Gi@^zY"
		PUBLIC_CLIENT_KEY = "2(]@b)A5u}(p&p.xtQ>l.Y>Fzi)NDF*6GqE23zPY"
		PRIVATE_CLIENT_KEY = "qlBVy1z/?5PA&4w(hF7F&qOH{0yz.@0&9z!ZK2yL"
	)

	var stroid string
	var credit, chunks, offset int
	var total int64
	flag.StringVar(&stroid, "oid", "", "Rados ObjectId")
	flag.Parse()

	// Download pipeline
	credit = 10

	dealer, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		logger.Log.Error(err)
		return
	}
	defer dealer.Close()
	dealer.ClientAuthCurve(SERVER_PUBLIC_KEY, PUBLIC_CLIENT_KEY, PRIVATE_CLIENT_KEY)

	err = dealer.Connect("tcp://127.0.0.1:5555")
	if err != nil {
		logger.Log.Error(err)
		return
	}

	fd, err := os.OpenFile(stroid, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		logger.Log.Error(err)
		return
	}
	defer fd.Close()

	for {
		for credit > 0 {
			//  Ask for next chunk
			dealer.SendMessage(stroid, offset, ZMQCHUNKSIZE)
			offset += ZMQCHUNKSIZE
			credit--
		}
		chunk, err := dealer.RecvBytes(0)
		if err != nil {
			break //  Shutting down, quit
		}
		_, err = fd.WriteAt(chunk, total)
		if err != nil {
			logger.Log.Error(err)
			return
		}

		chunks++
		credit++
		size := len(chunk)
		total += int64(size)
		if size < ZMQCHUNKSIZE {
			break //  Last chunk received; exit
		}
	}
	fmt.Printf("%v chunks received, %v bytes\n", chunks, total)
}
