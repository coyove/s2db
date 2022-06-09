package extdb

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/coyove/s2db/clock"
	"github.com/sirupsen/logrus"
)

type VFS struct {
	FS       vfs.FS
	Files    sync.Map
	DumpVDir string

	DumpTx struct {
		HTTPEndpoint string
		Logger       *logrus.Entry
		Counter      int
		TotalFiles   int
	}
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
		if fs.DumpTx.HTTPEndpoint == "" {
			return nil, fmt.Errorf("vdf HTTP endpoint not found")
		}
		return NewVFSVirtualDumpFile(filepath.Base(name), fs.DumpTx.HTTPEndpoint, fs)
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
	vfs    *VFS
	start  time.Time
	name   string
	h      hash.Hash32
	w      *io.PipeWriter
	reqSig chan [2]int
}

func NewVFSVirtualDumpFile(name string, dest string, vfs *VFS) (*VFSVirtualDumpFile, error) {
	if !strings.HasPrefix(dest, "http") {
		dest = "http://" + dest
	}

	vfs.DumpTx.Counter++
	vfs.DumpTx.Logger.Infof("vdf: %q started, %d of ~%d", name, vfs.DumpTx.Counter, vfs.DumpTx.TotalFiles)

	r, w := io.Pipe()
	req, err := http.NewRequest("PUT", dest, r)
	if err != nil {
		return nil, err
	}
	req.Header.Add("X-Path", name)

	reqSig := make(chan [2]int)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			w.Close()
			vfs.DumpTx.Logger.Infof("vdf: put %q http error: %v", name, err)
			reqSig <- [2]int{-1, -1}
		} else {
			resp.Body.Close()
			sz, _ := strconv.Atoi(resp.Header.Get("X-Dump-Size"))
			reqSig <- [2]int{resp.StatusCode, int(sz)}
		}
	}()

	return &VFSVirtualDumpFile{
		start:  clock.Now(),
		name:   name,
		h:      crc32.NewIEEE(),
		w:      w,
		vfs:    vfs,
		reqSig: reqSig,
	}, nil
}

func (vf *VFSVirtualDumpFile) Close() error {
	if _, err := vf.w.Write(vf.h.Sum(nil)); err != nil {
		return err
	}
	vf.w.Close()

	code := <-vf.reqSig
	vf.vfs.DumpTx.Logger.Infof("vdf: %q finished in %v, [code, size]=%v", vf.name, clock.Now().Sub(vf.start), code)
	if code[0] != 200 {
		return fmt.Errorf("vdf remote error: %v", code)
	}
	return nil
}

func (vf *VFSVirtualDumpFile) Read([]byte) (int, error) {
	return 0, fmt.Errorf("internal: vdf.Read not supported")
}

func (vf *VFSVirtualDumpFile) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, fmt.Errorf("internal: vdf.ReadAt not supported")
}

func (vf *VFSVirtualDumpFile) Stat() (os.FileInfo, error) {
	return nil, fmt.Errorf("internal: vdf.Stat not supported")
}

func (vf *VFSVirtualDumpFile) Write(p []byte) (int, error) {
	vf.h.Write(p)
	return vf.w.Write(p)
}

func (vf *VFSVirtualDumpFile) Sync() error {
	return nil
}
