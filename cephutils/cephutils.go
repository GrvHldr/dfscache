package cephutils

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/satori/go.uuid"
	"io"
	"time"
	"sync"
)

const (
	PoolNamesPreffix = "dsfcache-"
	objectTTL        = time.Duration(1 * time.Hour)
	ttlAttrName      = "TTL"
	fnameArrtName    = "FILENAME"
	bufferSize       = 8192
	radosObjLockName = "lock"
)

type BaseRadosObj struct {
	Pool     string        `json:"pool"`
	Oid      uuid.UUID     `json:"oid"`
	Size     uint64        `json:"size"`
	TTL      time.Duration `json:"exparation"`
	FileName string        `json:"file_name"`
}

type RadosObj struct {
	BaseRadosObj
	conn         *rados.Conn
	ioctx        *rados.IOContext
	bytesWritten uint64
	bytesRead    uint64
}

type LockRadosObj struct {
	sync.Mutex
	RadosObj
}

type UriRadosObj struct {
	BaseRadosObj
	Uri string `json:"uri"`
}

// Instantiate new Rados obj w/ defaults
func NewRadosObj(fname string) (*RadosObj, error) {
	newOid := uuid.NewV4()
	pool := PoolNamesPreffix + newOid.String()[:2]
	conn, err := NewRadosConn()
	if err != nil {
		return nil, err
	}

	ioctx, err := GetIoctx(conn, pool)
	if err != nil {
		return nil, err
	}

	return &RadosObj{
		BaseRadosObj: BaseRadosObj{
			Pool:     pool,
			Oid:      newOid,
			TTL:      time.Duration(time.Now().UTC().Add(objectTTL).Unix()),
			FileName: fname,
		},
		conn:  conn,
		ioctx: ioctx,
	}, nil
}

// Rados object for JSON serializer
func NewUriRadosObj(o BaseRadosObj) *UriRadosObj {
	return &UriRadosObj{
		BaseRadosObj: o,
		Uri:          "/download/" + o.Pool + "/" + o.Oid.String(),
	}
}

// Retrieve Rados object from Ceph storage
func ExistingRadosObj(pool string, oid uuid.UUID) (obj *RadosObj, err error) {
	conn, err := NewRadosConn()
	if err != nil {
		return nil, err
	}

	ioctx, err := GetIoctx(conn, pool)
	if err != nil {
		conn.Shutdown()
		return nil, err
	}

	stat, err := ioctx.Stat(oid.String())
	if err != nil {
		ioctx.Destroy()
		conn.Shutdown()
		return
	}

	ttl, err := GetObjTTL(ioctx, oid.String())
	if err != nil {
		ioctx.Destroy()
		conn.Shutdown()
		return
	}

	fname, err := GetObjFileName(ioctx, oid.String())
	if err != nil {
		ioctx.Destroy()
		conn.Shutdown()
		return
	}

	obj = &RadosObj{
		BaseRadosObj: BaseRadosObj{
			Pool:     pool,
			Oid:      oid,
			Size:     stat.Size,
			TTL:      ttl,
			FileName: fname,
		},
		conn:  conn,
		ioctx: ioctx,
	}

	return
}

// Must be called on operations finish w/ Rados object
func (o *RadosObj) Destroy() {
	o.ioctx.Destroy()
	o.conn.Shutdown()
}

// Sync object attributes to Ceph storage
func (o *RadosObj) SyncAttributes() error {
	// Save TTL
	buf := make([]byte, 10)
	binary.LittleEndian.PutUint64(buf, uint64(o.TTL))

	if err := o.ioctx.SetXattr(o.Oid.String(), ttlAttrName, buf); err != nil {
		return err
	}

	// Save FileName
	if err := o.ioctx.SetXattr(o.Oid.String(), fnameArrtName, []byte(o.FileName)); err != nil {
		return err
	}

	return nil
}

func (o *RadosObj) WriteFromReader(rd io.Reader) (uint64, error) {
	o.LockRados()
	defer o.UnlockRados()

	o.bytesWritten = 0
	bufrw := bufio.NewReadWriter(
		bufio.NewReaderSize(rd, bufferSize),
		bufio.NewWriterSize(o, bufferSize),
	)
	written, err := io.Copy(bufrw.Writer, bufrw.Reader)
	if err != nil {
		return 0, err
	}
	bufrw.Writer.Flush()

	// Save attributes
	err = o.SyncAttributes()
	if err != nil {
		return 0, err
	}

	o.Size = uint64(written)

	return o.Size, nil
}

func (o *RadosObj) ReadToWriter(wr io.Writer, off, len int64) (uint64, error) {
	o.bytesRead = 0
	//bufReader := bufio.NewReaderSize(o, bufferSize)
	//written, err := io.Copy(wr, bufReader)
	//if err != nil {
	//	return 0, err
	//}
	//
	//return uint64(written), nil

	o.LockRados()
	defer o.UnlockRados()

	reader := io.NewSectionReader(o, off, len)
	written, err := io.Copy(wr, reader)
	if err != nil {
		return 0, err
	}
	o.bytesRead = uint64(written)

	return o.bytesRead, nil
}

// Writer interface implementation
func (o *RadosObj) Write(p []byte) (n int, err error) {
	oid := o.Oid.String()

	if o.bytesWritten == 0 {
		err = o.ioctx.WriteFull(oid, p)
		if err == nil {
			n = len(p)
			o.bytesWritten += uint64(n)
			return
		}
		return
	}

	err = o.ioctx.Write(oid, p, o.bytesWritten)
	if err != nil {
		return
	}
	n = len(p)
	o.bytesWritten += uint64(n)

	return
}

func (o *RadosObj) WriteProgress() uint64 {
	return o.bytesWritten
}

func (o *RadosObj) ReadProgress() uint64 {
	return o.bytesRead
}

// Reader interface implementation
func (o *RadosObj) Read(p []byte) (n int, err error) {
	oid := o.Oid.String()

	n, err = o.ioctx.Read(oid, p, o.bytesRead)
	if err != nil {
		return
	}
	o.bytesRead += uint64(n)

	if n == 0 {
		err = io.EOF
	}

	return
}

// ReaderAt interface implementation
func (o *RadosObj) ReadAt(p []byte, off int64) (n int, err error) {
	oid := o.Oid.String()
	n, err = o.ioctx.Read(oid, p, uint64(off))
	if err != nil {
		return
	}
	if n == 0 {
		err = io.EOF
	}

	return
}

// Lock Rados object
func (o *RadosObj) LockRados() error {
	ret, err := o.ioctx.LockExclusive(
		o.Oid.String(),
		radosObjLockName,
		radosObjLockName,
		radosObjLockName,
		0,
		nil,
	)
	if err != nil {
		return err
	}

	if ret != 0 {
		return fmt.Errorf("%s already locked", o.Oid)
	}

	return nil
}

// Unlock Rados object
func (o *RadosObj) UnlockRados() error {
	_, err := o.ioctx.Unlock(o.Oid.String(), radosObjLockName, radosObjLockName)
	return err
}

// Unregister object from Ceph storage
func (o *RadosObj) Delete() error {
	oid := o.Oid.String()
	if IsObjectLocked(o.ioctx, oid) {
		return fmt.Errorf("Object %s is locked", oid)
	}

	return o.ioctx.Delete(o.Oid.String())
}

// New connection to Ceph cluster
func NewRadosConn() (*rados.Conn, error) {
	conn, err := rados.NewConn()
	if err != nil {
		return nil, fmt.Errorf("Unable to create new connection: ", err)
	}

	if err = conn.ReadDefaultConfigFile(); err != nil {
		return nil, fmt.Errorf("Can't read default config file: ", err)
	}

	if err = conn.Connect(); err != nil {
		return nil, fmt.Errorf("Can't conenct to Ceph cluster: ", err)
	}

	return conn, nil
}

// Get IO context
func GetIoctx(c *rados.Conn, pool string) (ioctx *rados.IOContext, err error) {
	contains := func(list []string, elem string) bool {
		for _, i := range list {
			if i == elem {
				return true
			}
		}
		return false
	}

	pools, err := c.ListPools()
	if err != nil {
		return
	}

	if !contains(pools, pool) {
		err = c.MakePool(pool)
		if err != nil {
			return
		}
	}

	return c.OpenIOContext(pool)
}

// Get object TTL attribute
func GetObjTTL(ioctx *rados.IOContext, oid string) (time.Duration, error) {
	buf := make([]byte, 10)
	_, err := ioctx.GetXattr(oid, ttlAttrName, buf)
	if err != nil {
		return 0, err
	}
	return time.Duration(binary.LittleEndian.Uint64(buf)), nil
}

// Get object FileName attribute
func GetObjFileName(ioctx *rados.IOContext, oid string) (string, error) {
	buf := make([]byte, 255)
	_, err := ioctx.GetXattr(oid, fnameArrtName, buf)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

// Check if object is locked
func IsObjectLocked(ioctx *rados.IOContext, oid string) bool {
	lock, err := ioctx.ListLockers(oid, radosObjLockName)
	if err != nil {
		return true
	}
	return lock.NumLockers != 0
}
