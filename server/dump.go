package server

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/coyove/s2db/s2"
	"github.com/coyove/sdss/future"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

func (s *Server) startCronjobs() {
	var run func(time.Duration, byte)
	run = func(d time.Duration, m byte) {
		if s.Closed {
			return
		}
		time.AfterFunc(d, func() { run(d, m) })
		switch m {
		case 1:
			if err := s.appendMetricsPairs(time.Hour * 24 * 30); err != nil {
				log.Error("AppendMetricsPairs: ", err)
			}
		default:
			s.runScriptFunc("cronjob" + strconv.Itoa(int(d.Seconds())))
		}
	}
	run(time.Second*30, 0)
	run(time.Second*60, 0)
	run(time.Second*60, 1)
	s.l6Deduper()
	s.l6Purger()
}

func (s *Server) DumpWire(dest string) {
	if !strings.HasPrefix(dest, "http") {
		dest = "http://" + dest
	}

	start := time.Now()
	log := log.WithField("shard", "dw")

	if !s.dumpWireLock.TryLock() {
		log.Info("wire dumping already started")
		return
	}
	defer s.dumpWireLock.Unlock()

	dbDir, err := os.Open(s.DBPath)
	if err != nil {
		log.Errorf("failed to read data dir: %v", err)
		return
	}
	defer dbDir.Close()
	dataFiles, err := dbDir.Readdirnames(-1)
	if err != nil {
		log.Errorf("failed to read data dir: %v", err)
		return
	}

	vfs := s.DBOptions.FS.(*VFS)
	log.Infof("DUMPWIRE started, clear virtual dump dir %q: %v", vfs.DumpVDir, os.RemoveAll(vfs.DumpVDir))

	vfs.DumpTx.HTTPEndpoint = dest
	vfs.DumpTx.Counter = 0
	vfs.DumpTx.TotalFiles = len(dataFiles)
	vfs.DumpTx.Logger = log
	defer func() { vfs.DumpTx.HTTPEndpoint = "" }()

	if err := s.DB.Checkpoint(vfs.DumpVDir, pebble.WithFlushedWAL()); err != nil {
		log.Errorf("failed to dump (checkpoint): %v", err)
		return
	}

	files, err := ioutil.ReadDir(vfs.DumpVDir)
	if err != nil {
		log.Errorf("failed to dump (readdir): %v", err)
		return
	}

	for _, f := range files {
		if err := func() error {
			src, err := os.Open(filepath.Join(vfs.DumpVDir, f.Name()))
			if err != nil {
				return err
			}
			defer src.Close()
			vf, err := NewVFSVirtualDumpFile(f.Name(), dest, vfs)
			if err != nil {
				return err
			}
			defer vf.Close()
			if _, err = io.Copy(vf, src); err != nil {
				return err
			}
			return vf.Sync()
		}(); err != nil {
			log.Errorf("failed to write metadata %q, %v", f.Name(), err)
			return
		}
	}

	vf, err := NewVFSVirtualDumpFile("EOT", dest, vfs)
	if err != nil {
		log.Errorf("failed to create end marker file: %v", err)
		return
	}
	vf.Close()

	log.Infof("DUMPWIRE all cleared in %v", time.Since(start))
}

func StartDumpReceiver(listen, dest string) error {
	if files, _ := os.ReadDir(dest); len(files) > 0 {
		return fmt.Errorf("%q is not empty", dest)
	}

	if err := os.MkdirAll(dest, 0666); err != nil {
		return err
	}

	log := log.WithField("shard", "dump")
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := r.Header.Get("X-Path")
		if p == "" {
			w.WriteHeader(400)
			return
		}

		var out io.Writer
		if p != "EOT" {
			fn, err := os.Create(filepath.Join(dest, p))
			if err != nil {
				log.Errorf("failed to create local file %q: %v", p, err)
				w.WriteHeader(500)
				return
			}
			defer fn.Close()
			out = fn
			log.Infof("put file %q", p)
		} else {
			out = ioutil.Discard
		}

		start := time.Now()
		size, ok, err := s2.CopyCrc32(out, r.Body, func(int) {})
		if err != nil || !ok {
			log.Errorf("failed to copy %q: %v, checksum=%v", p, err, ok)
			w.WriteHeader(410)
			return
		}

		if p == "EOT" {
			if err := func() error {
				db, err := pebble.Open(dest, &pebble.Options{})
				if err != nil {
					return err
				}
				defer db.Close()
				size, err := db.EstimateDiskUsage([]byte{0}, []byte{0xff})
				if err != nil {
					return err
				}
				if err := db.Set([]byte("config__servername"), []byte(""), pebble.Sync); err != nil {
					return err
				}
				log.Infof("EstimateDiskUsage=%d", size)
				return nil
			}(); err != nil {
				log.Errorf("failed to open database: %v", err)
				w.WriteHeader(510)
				return
			}
			log.Infof("received EOT, gracefully exit")
			time.AfterFunc(time.Second, func() { os.Exit(0) })
		} else {
			diff := time.Since(start)
			log.Infof("finished receiving %q (%.1f K) in %v", p, float64(size)/1024, diff)
			w.Header().Add("X-Dump-Size", strconv.Itoa(size))
		}

		w.WriteHeader(200)
	})
	log.Infof("dump-receiver listen: %v -> %v, waiting...", listen, dest)
	return http.ListenAndServe(listen, nil)
}

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
	vf.End = future.UnixNano()
	if !loaded {
		vf.Start = future.UnixNano()
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
	start  int64
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
		start:  future.UnixNano(),
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
	vf.vfs.DumpTx.Logger.Infof("vdf: %q finished in %v, [code, size]=%v", vf.name, time.Duration(future.UnixNano()-vf.start), code)
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
