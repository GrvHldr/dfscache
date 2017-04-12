package server

import (
	"encoding/json"
	"fmt"
	"github.com/GrvHldr/dfscache/cephutils"
	"github.com/GrvHldr/dfscache/logger"
	"github.com/julienschmidt/httprouter"
	"github.com/satori/go.uuid"
	"mime/multipart"
	"net/http"
	"strings"
	"strconv"
)

const (
	maxMemoryFormParse   = 131072
	bytesHeader          = "bytes="
	contentFormFieldName = "content"
	httpListenOn         = "0.0.0.0:8080"
)

type httpRange struct {
	start  int64
	length int64
}

// https://github.com/pkg4go/httprange/blob/master/util.go
func parseRange(s string, maxsize int64) ([]httpRange, error) {
	if s == "" {
		return nil, nil
	}

	if !strings.HasPrefix(s, bytesHeader) {
		return nil, fmt.Errorf("invalid range")
	}

	var ranges []httpRange

	for _, ra := range strings.Split(s[len(bytesHeader):], ",") {
		ra = strings.TrimSpace(ra)
		if ra == "" {
			continue
		}
		i := strings.Index(ra, "-")
		if i < 0 {
			return nil, fmt.Errorf("invalid range")
		}
		start, end := strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
		var r httpRange
		if start == "" {
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid range")
			}
			if i > maxsize {
				i = maxsize
			}
			r.start = maxsize - i
			r.length = maxsize - r.start
		} else {
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i >= maxsize || i < 0 {
				return nil, fmt.Errorf("invalid range")
			}
			r.start = i
			if end == "" {
				// If no end is specified, range extends to end of the file.
				r.length = maxsize - r.start
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.start > i {
					return nil, fmt.Errorf("invalid range")
				}
				if i >= maxsize {
					i = maxsize - 1
				}
				r.length = i - r.start + 1
			}
		}
		ranges = append(ranges, r)
	}

	return ranges, nil
}

func getContentMultipartFormData(r *http.Request) (*multipart.FileHeader, error) {
	if err := r.ParseMultipartForm(maxMemoryFormParse); err != nil {
		return nil, err
	}
	if _, ok := r.MultipartForm.File[contentFormFieldName]; !ok {
		return nil, fmt.Errorf("'%s' not found in uploaded data", contentFormFieldName)
	}

	return r.MultipartForm.File[contentFormFieldName][0], nil
}

func serveIndex(_ http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	//	Dummy index - just stub
}

func serveFileUpload(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fh, err := getContentMultipartFormData(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fd, err := fh.Open()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer fd.Close()

	newObj, err := cephutils.NewRadosObj()
	if err != nil {
		logger.Log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer newObj.Destroy()

	_, err = newObj.WriteFromReader(fd)
	if err != nil {
		logger.Log.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result, err := json.Marshal(
		cephutils.NewUriRadosObj(
			cephutils.BaseRadosObj{
				Pool: newObj.Pool,
				Oid:  newObj.Oid,
				Size: newObj.Size,
				TTL:  newObj.TTL,
			},
		),
	)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		logger.Log.Error(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
}

func serveFileDownload(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	obj, err, rc := retrieveRadosObj(p)
	if err != nil {
		http.Error(w, err.Error(), rc)
		logger.Log.Error(err)
		return
	}
	defer obj.Destroy()

	w.Header().Set("Content-Disposition", "attachment; filename="+obj.Oid.String())
	w.Header().Set("Content-Type", r.Header.Get("Content-Type"))
	w.Header().Set("Accept-Ranges", "bytes")

	strRange := r.Header.Get("Range")
	if strRange == "" {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
		if _, err = obj.ReadToWriter(w, 0, int64(obj.Size)); err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			logger.Log.Error(err)
			return
		}
		return
	}
	// If range download
	ranges, err := parseRange(strRange, int64(obj.Size))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	off, len := ranges[0].start, ranges[0].length
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", off, off+len-1))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len))
	if _, err = obj.ReadToWriter(w, off, len); err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		logger.Log.Error(err)
		return
	}

}

func serveFileDelete(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	obj, err, rc := retrieveRadosObj(p)
	if err != nil {
		http.Error(w, err.Error(), rc)
		logger.Log.Error(err)
		return
	}
	defer obj.Destroy()

	err = obj.Delete()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
		return
	}
}

func retrieveRadosObj(p httprouter.Params) (obj *cephutils.RadosObj, err error, rc int) {
	poolName := p.ByName("pool")
	stroid := p.ByName("oid")
	if !strings.HasPrefix(poolName, cephutils.PoolNamesPreffix) {
		err, rc = fmt.Errorf("Invalid data pool name"), http.StatusBadRequest
		return
	}

	oid, err := uuid.FromString(stroid)
	if err != nil {
		err, rc = fmt.Errorf("Invalid OID"), http.StatusBadRequest
		return
	}

	obj, err = cephutils.ExistingRadosObj(poolName, oid)
	if err != nil {
		err, rc = err, http.StatusNotFound
		return
	}

	return
}

func Run() {
	router := &customRouter{*httprouter.New()}

	// HTTP resources
	router.GET("/", serveIndex)
	router.POST("/upload", serveFileUpload)
	router.GET("/download/:pool/:oid", serveFileDownload)
	router.DELETE("/delete/:pool/:oid", serveFileDelete)

	logger.Log.Infof("HTTP Listening on '%s'", httpListenOn)
	logger.Log.Fatal(http.ListenAndServe(httpListenOn, router))
}
