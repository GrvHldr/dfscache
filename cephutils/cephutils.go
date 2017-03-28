package cephutils

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/satori/go.uuid"
	"io"
	"time"
)

const (
	PoolNamesPreffix = "dsfcache-"
	objectTTL        = time.Duration(1 * time.Hour)
	ttlAttrName      = "TTL"
	bufferSize       = 8192
	radosObjLockName = "lock"
)

type BaseRadosObj struct {
	Pool string        `json:"pool"`
	Oid  uuid.UUID     `json:"oid"`
	Size uint64        `json:"size"`
	TTL  time.Duration `json:"exparation"`
}

type RadosObj struct {
	BaseRadosObj
	conn         *rados.Conn
	ioctx        *rados.IOContext
	bytesWritten uint64
	bytesRead    uint64
}

type UriRadosObj struct {
	BaseRadosObj
	Uri string `json:"uri"`
}

func NewRadosObj() (*RadosObj, error) {
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
			Pool: pool,
			Oid:  newOid,
			TTL:  time.Duration(time.Now().UTC().Add(objectTTL).Unix()),
		},
		conn:  conn,
		ioctx: ioctx,
	}, nil
}

func NewUriRadosObj(o BaseRadosObj) *UriRadosObj {
	return &UriRadosObj{
		BaseRadosObj: o,
		Uri:          "/download/" + o.Pool + "/" + o.Oid.String(),
	}
}

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

	obj = &RadosObj{
		BaseRadosObj: BaseRadosObj{
			Pool: pool,
			Oid:  oid,
			Size: stat.Size,
			TTL:  ttl,
		},
		conn:  conn,
		ioctx: ioctx,
	}

	return
}

func (o *RadosObj) Destroy() {
	o.ioctx.Destroy()
	o.conn.Shutdown()
}

func (o *RadosObj) WriteFromReader(rd io.Reader) (uint64, error) {
	o.Lock()
	defer o.Unlock()
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

	// Set TTL attribute
	err = SetObjTTL(o.ioctx, o.Oid.String(), o.TTL)
	if err != nil {
		return 0, err
	}
	o.Size = uint64(written)

	return o.Size, nil
}

func (o *RadosObj) ReadToWriter(wr io.Writer) (uint64, error) {
	o.bytesRead = 0
	//bufReader := bufio.NewReaderSize(o, bufferSize)
	//written, err := io.Copy(wr, bufReader)
	//if err != nil {
	//	return 0, err
	//}
	//
	//return uint64(written), nil

	o.Lock()
	defer o.Unlock()

	reader := io.NewSectionReader(o, 0, int64(o.Size))
	written, err := io.Copy(wr, reader)
	if err != nil {
		return 0, err
	}
	o.bytesRead = uint64(written)

	return o.bytesRead, nil
}

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

func (o *RadosObj) Lock() error {
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

func (o *RadosObj) Unlock() error {
	_, err := o.ioctx.Unlock(o.Oid.String(), radosObjLockName, radosObjLockName)
	return err
}

func (o *RadosObj) Delete() error {
	oid := o.Oid.String()
	if IsObjectLocked(o.ioctx, oid) {
		return fmt.Errorf("Object %s is locked", oid)
	}

	return o.ioctx.Delete(o.Oid.String())
}

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

func SetObjTTL(ioctx *rados.IOContext, oid string, ttl time.Duration) error {
	buf := make([]byte, 10)
	binary.LittleEndian.PutUint64(buf, uint64(ttl))

	return ioctx.SetXattr(oid, ttlAttrName, buf)
}

func GetObjTTL(ioctx *rados.IOContext, oid string) (time.Duration, error) {
	buf := make([]byte, 10)
	_, err := ioctx.GetXattr(oid, ttlAttrName, buf)
	if err != nil {
		return 0, err
	}
	return time.Duration(binary.LittleEndian.Uint64(buf)), nil
}

func IsObjectLocked(ioctx *rados.IOContext, oid string) bool {
	lock, err := ioctx.ListLockers(oid, radosObjLockName)
	if err != nil {
		return true
	}
	return lock.NumLockers != 0
}
