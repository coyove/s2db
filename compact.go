package main

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/coyove/s2db/extdb"
	s2pkg "github.com/coyove/s2db/s2pkg"
	log "github.com/sirupsen/logrus"
)

func (s *Server) startCronjobs() {
	var run func(time.Duration, bool)
	run = func(d time.Duration, m bool) {
		if s.Closed {
			return
		}
		time.AfterFunc(d, func() { run(d, m) })
		if m {
			if err := s.appendMetricsPairs(time.Hour * 24 * 30); err != nil {
				log.Error("AppendMetricsPairs: ", err)
			}
		} else {
			s.runScriptFunc("cronjob" + strconv.Itoa(int(d.Seconds())))
		}
	}
	run(time.Second*30, false)
	run(time.Second*60, false)
	run(time.Second*60, true)
	run(time.Second*300, false)
}

func getTTLByName(ttls []s2pkg.Pair, name string) int {
	idx := sort.Search(len(ttls), func(i int) bool { return ttls[i].Member >= name })
	if idx < len(ttls) && name == ttls[idx].Member {
		return int(ttls[idx].Score)
	}
	if idx > 0 && idx <= len(ttls) && strings.HasPrefix(name, ttls[idx-1].Member) {
		return int(ttls[idx-1].Score)
	}
	return -1
}

func (s *Server) DumpWire(dest string) {
	if !strings.HasPrefix(dest, "http") {
		dest = "http://" + dest
	}

	start := time.Now()
	log := log.WithField("shard", "dw")

	s.dumpWireLock.Lock(func() { log.Info("wire dumping already started") })
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

	vfs := s.DBOptions.FS.(*extdb.VFS)
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
			vf, err := extdb.NewVFSVirtualDumpFile(f.Name(), dest, vfs)
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

	vf, err := extdb.NewVFSVirtualDumpFile("EOT", dest, vfs)
	if err != nil {
		log.Errorf("failed to create end marker file: %v", err)
		return
	}
	vf.Close()

	log.Infof("DUMPWIRE all cleared in %v", time.Since(start))
}

func dumpReceiver(dest string) {
	if files, _ := os.ReadDir(dest); len(files) > 0 {
		errorExit(dest + " is not empty")
	}

	if err := os.MkdirAll(dest, 0666); err != nil {
		errorExit(err.Error())
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
		size, ok, err := s2pkg.CopyCrc32(out, r.Body, func(int) {})
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
				if err := db.Set([]byte("config__slave"), []byte(""), pebble.Sync); err != nil {
					return err
				}
				if err := db.Set([]byte("config__markmaster"), []byte("0"), pebble.Sync); err != nil {
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
	log.Infof("dump-receiver listen: %v -> %v, waiting...", *listenAddr, dest)
	http.ListenAndServe(*listenAddr, nil)
}
