package server

import (
	"bufio"
	"github.com/GrvHldr/dfscache/logger"
	"github.com/julienschmidt/httprouter"
	"net"
	"net/http"
	"time"
)

func logRequestContent(w *customResponseWriter, req *http.Request) {
	var username = "-"
	if req.URL.User != nil {
		if name := req.URL.User.Username(); name != "" {
			username = name
		}
	}
	logger.Log.Infof("%s - %s \"%s %s %s\" %d %d (%s)",
		req.RemoteAddr,
		username,
		req.Method,
		req.URL.RequestURI(),
		req.Proto,
		w.bytesCount,
		w.statusCode,
		w.requestDuration,
	)
}

type customRouter struct {
	httprouter.Router
}

type customResponseWriter struct {
	responseWriter  http.ResponseWriter
	bytesCount      int
	requestDuration time.Duration
	statusCode      int
}

func (w *customResponseWriter) Header() http.Header {
	return w.responseWriter.Header()
}

func (w *customResponseWriter) Write(b []byte) (int, error) {
	res, err := w.responseWriter.Write(b)
	w.bytesCount += res
	return res, err
}

func (w *customResponseWriter) WriteHeader(i int) {
	w.statusCode = i
	w.responseWriter.WriteHeader(i)
}

func (w *customResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, _ := w.responseWriter.(http.Hijacker)
	return hj.Hijack()
}

func (r *customRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	m := &customResponseWriter{w, 0, 0, 0}
	start := time.Now()
	r.Router.ServeHTTP(m, req)
	m.requestDuration = time.Since(start)
	logRequestContent(m, req)
}
