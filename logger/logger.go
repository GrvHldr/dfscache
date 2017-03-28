package logger

import (
"github.com/op/go-logging"
"os"
)

var Log *logging.Logger

func init() {
	// Setup logger
	Log = logging.MustGetLogger("DFSCacheLog")
	formatter := logging.MustStringFormatter(`%{level} - %{time} - %{shortfunc} ▶ %{message}`)
	backend := logging.NewLogBackend(os.Stdout, "", 0)
	backend_formatter := logging.NewBackendFormatter(backend, formatter)
	logging.SetBackend(backend_formatter)
}
