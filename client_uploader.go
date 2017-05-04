package main

import (
	"flag"
	"github.com/GrvHldr/dfscache/logger"
	zmq "github.com/pebbe/zmq4"
	"os"
	"path/filepath"
	"io"
	"encoding/binary"
)

func main() {
	const (
		CHUNKSIZE = 25000 // Chunk size in bytes
		SERVER_PUBLIC_KEY = "3>v/vSk6K(WoH?&[lNt@PKBJbj&13xL^B3Gi@^zY"
		PUBLIC_CLIENT_KEY = "2(]@b)A5u}(p&p.xtQ>l.Y>Fzi)NDF*6GqE23zPY"
		PRIVATE_CLIENT_KEY = "qlBVy1z/?5PA&4w(hF7F&qOH{0yz.@0&9z!ZK2yL"
	)

	var filename string
	var offset int64

	flag.StringVar(&filename, "file_name", "", "File name to upload")
	flag.Parse()

	if filename == "" {
		logger.Log.Error("File name to upload is not provided")
		return
	}

	fd, err := os.Open(filename)
	if err != nil {
		logger.Log.Error("Can't open file for reading: ", err)
		return
	}
	defer fd.Close()

	fileStat, err := fd.Stat()
	if err != nil {
		logger.Log.Error("Can't get file stat: ", err)
		return
	}

	// ZMQ connect to server
	dealer, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		logger.Log.Error(err)
		return
	}
	defer dealer.Close()

	dealer.ClientAuthCurve(SERVER_PUBLIC_KEY, PUBLIC_CLIENT_KEY, PRIVATE_CLIENT_KEY)
	err = dealer.Connect("tcp://127.0.0.1:6666")
	if err != nil {
		logger.Log.Error(err)
		return
	}

	fileBasename := filepath.Base(filename)
	bFileSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(bFileSize, uint64(fileStat.Size()))

	logger.Log.Info("Sending filename:", fileBasename, "; size:", fileStat.Size())

	// Send initial header
	_, err = dealer.SendMessage(fileBasename, bFileSize)
	if err != nil {
		logger.Log.Error(err)
		return
	}

	parts, err := dealer.RecvMessage(0)
	if parts[0] == "ACK" {
		logger.Log.Info("Established connection, OID:", parts[1])
	} else {
		logger.Log.Error("Can't establish ZMQ connection")
		return
	}

	buf := make([]byte, CHUNKSIZE)

	for {
		read, err := fd.ReadAt(buf, offset)
		if err != nil && err != io.EOF  {
			logger.Log.Error("Read error:", err)
			return
		}

		// Send chunk
		_, err = dealer.SendMessage(buf[:read])
		if err != nil {
			logger.Log.Error(err)
			return
		}

		// Receive status
		parts, err = dealer.RecvMessage(0)
		if err != nil {
			logger.Log.Error(err)
			return
		}
		if parts[0] == "NAK" {
			logger.Log.Error("Upstream internal error")
			return
		}

		written := binary.LittleEndian.Uint64([]byte(parts[0]))
		logger.Log.Debug(written)

		offset += int64(read)
		if read < CHUNKSIZE {  // Read last chunk
			break
		}
	}
}
