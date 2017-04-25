// ZeroMQ client example

package main

import "fmt"
import (
	"flag"
	"github.com/GrvHldr/dfscache/logger"
	"github.com/GrvHldr/dfscache/server"
	zmq "github.com/pebbe/zmq4"
	"os"
)

const ZMQCHUNKSIZE = 25000

func main() {
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
