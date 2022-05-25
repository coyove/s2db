package s2pkg

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/coyove/s2db/clock"
	"github.com/sirupsen/logrus"
)

type Storage interface {
	Get([]byte) ([]byte, io.Closer, error)
	Set(key, value []byte, opts *pebble.WriteOptions) error
	Delete(key []byte, _ *pebble.WriteOptions) error
	DeleteRange(start, end []byte, _ *pebble.WriteOptions) error
	NewIter(*pebble.IterOptions) *pebble.Iterator
}

type LogTx struct {
	OutLogtail *uint64
	InLogtail  *uint64
	LogPrefix  []byte
	Storage
}

func GetKey(db Storage, key []byte) ([]byte, error) {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	defer rd.Close()
	return Bytes(buf), nil
}

func GetKeyNumber(db Storage, key []byte) (float64, uint64, bool, error) {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	defer rd.Close()
	if len(buf) != 8 {
		return 0, 0, false, fmt.Errorf("invalid number bytes (8)")
	}
	return BytesToFloat(buf), BytesToUint64(buf), true, nil
}

func IncrKey(db Storage, key []byte, v int64) error {
	buf, rd, err := db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return db.Set(key, Uint64ToBytes(uint64(v)), pebble.Sync)
		}
		return err
	}
	old := int64(BytesToUint64(buf))
	rd.Close()
	old += v
	if old == 0 {
		return db.Delete(key, pebble.Sync)
	}
	return db.Set(key, Uint64ToBytes(uint64(old)), pebble.Sync)
}

type BuoyTimeoutError struct {
	Value interface{}
}

func (bte *BuoyTimeoutError) Error() string { return "buoy wait timeout" }

type buoyCell struct {
	watermark uint64
	ts        int64
	ch        chan interface{}
	v         interface{}
}

type BuoySignal struct {
	mu      sync.Mutex
	list    []buoyCell
	closed  bool
	metrics *Survey
}

func NewBuoySignal(timeout time.Duration, metrics *Survey) *BuoySignal {
	bs := &BuoySignal{metrics: metrics}
	var watcher func()
	watcher = func() {
		if bs.closed {
			return
		}

		bs.mu.Lock()
		defer func() {
			bs.mu.Unlock()
			time.AfterFunc(timeout/2, watcher)
		}()

		now := time.Now().UnixNano()
		for len(bs.list) > 0 {
			cell := bs.list[0]
			if time.Duration(now-cell.ts) > timeout {
				cell.ch <- &BuoyTimeoutError{Value: cell.v}
				bs.list = bs.list[1:]
			} else {
				break
			}
		}
	}
	time.AfterFunc(timeout/2, watcher)
	return bs
}

func (bs *BuoySignal) Close() {
	bs.closed = true
}

func (bs *BuoySignal) WaitAt(watermark uint64, ch chan interface{}, v interface{}) int {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	c := buoyCell{
		watermark: watermark,
		ch:        ch,
		v:         v,
		ts:        time.Now().UnixNano(),
	}
	if len(bs.list) > 0 {
		last := bs.list[len(bs.list)-1]
		if watermark <= last.watermark {
			panic("buoy watermark error")
		}
	}
	bs.list = append(bs.list, c)
	return len(bs.list)
}

func (bs *BuoySignal) RaiseTo(watermark uint64) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	now := time.Now().UnixNano()
	for len(bs.list) > 0 {
		if cell := bs.list[0]; cell.watermark <= watermark {
			cell.ch <- cell.v
			bs.list = bs.list[1:]
			bs.metrics.Incr((now - cell.ts) / 1e6)
		} else {
			break
		}
	}
}

func (bs *BuoySignal) Len() int {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return len(bs.list)
}

func (bs *BuoySignal) String() string {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if len(bs.list) == 0 {
		return "0-0"
	}
	return fmt.Sprintf("%d-%d", bs.list[0].watermark, bs.list[len(bs.list)-1].watermark)
}

type VFS struct {
	FS    vfs.FS
	Files sync.Map

	DumpVDir string
	DumpTo   string
}

type vfsFileRecord struct {
	Name       string
	Count      int64
	Start, End int64
	Closed     bool
}

type vfsFile struct {
	fs   *VFS
	name string
	vfs.File
}

func (vf vfsFile) Close() error {
	if r, ok := vf.fs.Files.Load(vf.name); ok {
		r.(*vfsFileRecord).Closed = true
	}
	return vf.File.Close()
}

func (fs *VFS) Create(name string) (vfs.File, error) {
	if strings.HasPrefix(name, fs.DumpVDir) && strings.HasSuffix(name, ".sst") {
		return NewVFSVirtualDumpFile(filepath.Base(name), fs.DumpTo)
	}
	return fs.FS.Create(name)
}

func (fs *VFS) Link(string, string) error {
	return fmt.Errorf("NoLinkFS")
}

func (fs *VFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	fn := filepath.Base(name)
	ctr, loaded := fs.Files.LoadOrStore(fn, new(vfsFileRecord))
	vf := ctr.(*vfsFileRecord)
	atomic.AddInt64(&vf.Count, 1)
	vf.Name = name
	vf.End = clock.UnixNano()
	if !loaded {
		vf.Start = clock.UnixNano()
	}
	f, err := fs.FS.Open(name, opts...)
	if f != nil {
		return vfsFile{fs, fn, f}, err
	}
	return f, err
}

func (fs *VFS) OpenDir(name string) (vfs.File, error) {
	return fs.FS.OpenDir(name)
}

func (fs *VFS) Remove(name string) error {
	fs.Files.Delete(filepath.Base(name))
	return fs.FS.Remove(name)
}

func (fs *VFS) RemoveAll(name string) error {
	return fs.FS.RemoveAll(name)
}

func (fs *VFS) Rename(oldname, newname string) error {
	return fs.FS.Rename(oldname, newname)
}

func (fs *VFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	return fs.FS.ReuseForWrite(oldname, newname)
}

func (fs *VFS) MkdirAll(dir string, perm os.FileMode) error {
	return fs.FS.MkdirAll(dir, perm)
}

func (fs *VFS) Lock(name string) (io.Closer, error) {
	return fs.FS.Lock(name)
}

func (fs *VFS) List(dir string) ([]string, error) {
	return fs.FS.List(dir)
}

func (fs *VFS) Stat(name string) (os.FileInfo, error) {
	return fs.FS.Stat(name)
}

func (fs *VFS) PathBase(path string) string {
	return fs.FS.PathBase(path)
}

func (fs *VFS) PathJoin(elem ...string) string {
	return fs.FS.PathJoin(elem...)
}

func (fs *VFS) PathDir(path string) string {
	return fs.FS.PathDir(path)
}

func (fs *VFS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	return fs.FS.GetDiskUsage(path)
}

func (fs *VFS) SSTOpenCount() (res []vfsFileRecord) {
	fs.Files.Range(func(key, value interface{}) bool {
		k := key.(string)
		if !strings.HasSuffix(k, ".sst") {
			return true
		}
		res = append(res, *value.(*vfsFileRecord))
		return true
	})
	sort.Slice(res, func(i, j int) bool { return res[i].Count < res[j].Count })
	return
}

func (vf vfsFileRecord) String() string {
	return fmt.Sprintf("%s (%d in %ds)", vf.Name, vf.Count, (vf.End-vf.Start)/1e9)
}

type VFSVirtualDumpFile struct {
	start  time.Time
	name   string
	h      hash.Hash32
	w      *io.PipeWriter
	reqSig chan int
}

func NewVFSVirtualDumpFile(name string, dest string) (*VFSVirtualDumpFile, error) {
	if !strings.HasPrefix(dest, "http") {
		dest = "http://" + dest
	}

	logrus.Infof("VFSVirtualDumpFile started, dump %q to %q", name, dest)

	r, w := io.Pipe()
	req, err := http.NewRequest("PUT", dest, r)
	if err != nil {
		return nil, err
	}
	req.Header.Add("X-Path", name)

	reqSig := make(chan int)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			w.Close()
			logrus.Infof("VFSVirtualDumpFile %q http error: %v", name, err)
			reqSig <- -1
		} else {
			resp.Body.Close()
			reqSig <- resp.StatusCode
		}
	}()

	return &VFSVirtualDumpFile{
		start:  clock.Now(),
		name:   name,
		h:      crc32.NewIEEE(),
		w:      w,
		reqSig: reqSig,
	}, nil
}

func (vf *VFSVirtualDumpFile) Close() error {
	_, err := vf.w.Write(vf.h.Sum(nil))
	vf.w.Close()
	logrus.Infof("VFSVirtualDumpFile %q finished in %v, code=%v", vf.name, clock.Now().Sub(vf.start), <-vf.reqSig)
	return err
}

func (vf *VFSVirtualDumpFile) Read([]byte) (int, error) {
	return 0, fmt.Errorf("VFSVirtualDumpFile.Read not supported")
}

func (vf *VFSVirtualDumpFile) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, fmt.Errorf("VFSVirtualDumpFile.ReadAt not supported")
}

func (vf *VFSVirtualDumpFile) Stat() (os.FileInfo, error) {
	return nil, fmt.Errorf("VFSVirtualDumpFile.Stat not supported")
}

func (vf *VFSVirtualDumpFile) Write(p []byte) (int, error) {
	vf.h.Write(p)
	return vf.w.Write(p)
}

func (vf *VFSVirtualDumpFile) Sync() error {
	return nil
}
