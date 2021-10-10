package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/s2db/calc"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

func (s *Server) compactShard(shard int) {
	log := log.WithField("shard", strconv.Itoa(shard))

	if shard == 0 {
		s.configMu.Lock()
		defer s.configMu.Unlock()
	}

	x := &s.db[shard]
	path := x.DB.Path()
	compactPath := path + ".compact"
	dumpPath := path + ".dump"
	if s.CompactTmpDir != "" {
		compactPath = filepath.Join(s.CompactTmpDir, "shard"+strconv.Itoa(shard)+".redir.compact")
		dumpPath = filepath.Join(s.CompactTmpDir, "shard"+strconv.Itoa(shard)+".redir.dump")
	}

	log.Info("STAGE 0: begin compaction, compactDB=", compactPath, ", dumpDB=", dumpPath)

	// STAGE 1: dump the shard, open a temp database for compaction
	os.Remove(compactPath)
	dumpFile, err := os.Create(dumpPath)
	if err != nil {
		log.Error("open dump file: ", err)
		return
	}
	if err := x.DB.View(func(tx *bbolt.Tx) error {
		_, err := tx.WriteTo(dumpFile)
		return err
	}); err != nil {
		dumpFile.Close()
		log.Error("dump DB: ", err)
		return
	}
	dumpSize, _ := dumpFile.Seek(0, 2)
	log.Info("STAGE 0: dump finished: ", dumpSize)
	dumpFile.Close()

	compactDB, err := bbolt.Open(compactPath, 0666, bboltOptions)
	if err != nil {
		log.Error("open compactDB: ", err)
		return
	}
	dumpDB, err := bbolt.Open(dumpPath, 0666, bboltReadonlyOptions)
	if err != nil {
		log.Error("open dumpDB: ", err)
		return
	}
	if err := s.defragdb(shard, dumpDB, compactDB); err != nil {
		dumpDB.Close()
		compactDB.Close()
		log.Error("defragdb: ", err)
		return
	}
	dumpDB.Close()
	removeDumpErr := os.Remove(dumpPath)
	log.Infof("STAGE 1: point-in-time compaction finished, size=%d, removeDumpErr=%v", compactDB.Size(), removeDumpErr)

	// STAGE 2: for any changes happened during the compaction, write them into compactDB
	compactTail := func() (tail uint64, err error) {
		err = compactDB.View(func(tx *bbolt.Tx) error {
			bk := tx.Bucket([]byte("wal"))
			if bk != nil {
				if k, _ := bk.Cursor().Last(); len(k) == 8 {
					tail = binary.BigEndian.Uint64(k)
				}
			}
			return nil
		})
		return
	}
	var ct, mt uint64
	for {
		ct, err = compactTail()
		if err != nil {
			log.Error("get compactDB tail: ", err)
			return
		}
		mt, err = s.myLogTail(shard)
		if err != nil {
			log.Error("get shard tail: ", err)
			return
		}
		if ct > mt {
			log.Errorf("fatal error: compactDB tail exceeds shard tail: %d>%d", ct, mt)
			return
		}
		if mt-ct <= uint64(s.CompactTxSize) {
			break // the gap is close enough, it is time to move on to the next stage
		}
		logs, err := s.responseLog(shard, ct+1, false)
		if err != nil {
			log.Error("responseLog: ", err)
			return
		}
		if _, err := runLog(logs, compactDB, s.FillPercent); err != nil {
			log.Error("runLog: ", err)
			return
		}
	}
	log.Infof("STAGE 2: incremental logs replayed, ct=%d, mt=%d, diff=%d, compactSize=%d", ct, mt, mt-ct, compactDB.Size())

	// STAGE 3: now compactDB almost (or already) catch up with onlineDB, we make onlineDB readonly so no more new changes can be made
	x.compactReplacing = true
	defer func() { x.compactReplacing = false }()

	x.DB.Close()
	roDB, err := bbolt.Open(path, 0666, bboltReadonlyOptions)
	if err != nil {
		// Worst case, this shard goes offline completely
		log.Error("CAUTION: open roDB: ", err)
		return
	}
	x.DB = roDB
	log.Info("STAGE 3: make online database rw -> ro")

	// STAGE 4: for any changes happened during STAGE 2+3 before readonly, write them to compactDB (should be few)
	logs, err := s.responseLog(shard, ct+1, true)
	if err != nil {
		log.Error("responseLog: ", err)
		return
	}
	if _, err := runLog(logs, compactDB, s.FillPercent); err != nil {
		log.Error("runLog: ", err)
		return
	}
	log.Infof("STAGE 4: final logs replayed, count=%d, size: %d>%d", len(logs), roDB.Size(), compactDB.Size())

	// STAGE 5: now compactDB and onlineDB are identical, swap them to make compactDB officially online
	roDB.Close()

	if s.CompactNoBackup == 1 {
		log.Info("CAUTION: compact no backup")
		if err := os.Remove(path); err != nil {
			log.Error("delete original (online) DB: ", err)
			return
		}
	} else {
		if err := os.Rename(path, path+".bak"); err != nil {
			log.Error("backup original (online) DB: ", err)
			return
		}
	}

	if s.CompactTmpDir != "" {
		of, err := os.Create(path)
		if err != nil {
			log.Error("open dump file for compactDB: ", err)
			return
		}
		defer of.Close()
		err = compactDB.View(func(tx *bbolt.Tx) error {
			_, err := tx.WriteTo(of)
			return err
		})
		if err != nil {
			log.Error("dump compactDB to location: ", err)
			return
		}
		compactDB.Close()
		os.Remove(compactPath)
	} else {
		compactDB.Close()
		if err := os.Rename(compactPath, path); err != nil {
			log.Error("rename compactDB to online DB: ", err)
			return
		}
	}

	db, err := bbolt.Open(path, 0666, bboltOptions)
	if err != nil {
		log.Error("open compactDB as online DB: ", err)
		return
	}
	x.DB = db

	log.Info("STAGE 5: swap compacted database to online")
}

func (s *Server) schedPurge() {
	for !s.closed {
		if s.SchedCompactJob == "" {
			time.Sleep(time.Minute)
			continue
		}

		oks := [ShardNum]bool{}
		for i := 0; i < ShardNum; i++ {
			ok, err := calc.Eval(s.SchedCompactJob, 's', float64(i))
			if err != nil {
				log.Error("scheduled purgelog invalid job string: ", err)
			} else if ok != 0 {
				oks[i] = true
			}
		}
		for i, ok := range oks {
			if ok {
				log.Info("scheduleCompaction(", i, ")")
				s.compactShard(i)
			}
		}

		// Do compaction on shard with large freelist
		maxFreelistSize := 0
		maxFreelistDB := -1
		for i := range s.db {
			sz := s.db[i].FreelistSize()
			if sz > maxFreelistSize {
				maxFreelistSize = sz
				maxFreelistDB = i
			}
		}
		if maxFreelistDB >= 0 && s.CompactFreelistLimit > 0 && maxFreelistSize > s.CompactFreelistLimit {
			log.Info("freelistCompaction(", maxFreelistDB, ")")
			s.compactShard(maxFreelistDB)
		}

		time.Sleep(time.Minute)
	}
}

func (s *Server) defragdb(shard int, odb, tmpdb *bbolt.DB) error {
	log := log.WithField("shard", strconv.Itoa(shard))

	tmp, err := getPendingUnlinks(odb)
	if err != nil {
		return err
	}
	unlinkp := make(map[string]bool, len(tmp))
	for _, n := range tmp {
		unlinkp[n] = true
	}

	// open a tx on tmpdb for writes
	tmptx, err := tmpdb.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tmptx.Rollback()
		}
	}()

	// open a tx on old db for read
	tx, err := odb.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	c := tx.Cursor()

	var slaveMinWal uint64
	var useSlaveWal bool
	{
		var min uint64 = math.MaxUint64
		s.slaves.Foreach(func(si *serverInfo) {
			if si.LogTails[shard] < min {
				min = si.LogTails[shard]
			}
		})
		if min != math.MaxUint64 {
			// If master have any slaves, it can't purge logs which slaves don't have yet
			// This is the best effort we can make because slaves maybe offline so it is still possible to over-purge
			slaveMinWal = min
			useSlaveWal = true
		} else if s.MasterMode {
			log.Info("STAGE 0.1: master mode: failed to collect info from slaves, no log compaction will be made")
			slaveMinWal = 0
			useSlaveWal = true
		}
	}

	count := 0
	total := 0
	for next, _ := c.First(); next != nil; next, _ = c.Next() {
		nextStr := string(next)
		if nextStr == "unlink" { // pending unlinks will be cleared during every compaction
			continue
		}
		isQueue := strings.HasPrefix(nextStr, "q.")
		if strings.HasPrefix(nextStr, "zset.score.") && unlinkp[string(next[11:])] ||
			strings.HasPrefix(nextStr, "zset.") && unlinkp[string(next[5:])] ||
			isQueue && unlinkp[string(next[2:])] {
			continue
		}

		b := tx.Bucket(next)
		if b == nil {
			return fmt.Errorf("backend: cannot defrag bucket %q", string(next))
		}

		tmpb, berr := tmptx.CreateBucketIfNotExists(next)
		if berr != nil {
			return berr
		}
		tmpb.FillPercent = 0.9 // for seq write in for each
		tmpb.SetSequence(b.Sequence())

		var walStartBuf []byte
		if nextStr == "wal" {
			walStart := decUint64(slaveMinWal, uint64(s.CompactLogHead))
			if !useSlaveWal {
				walStart = decUint64(b.Sequence(), uint64(s.CompactLogHead))
			} else if walStart >= b.Sequence() {
				log.Infof("STAGE 0.1: dumping took too long, slave logs surpass dumped logs: slave log: %d, log tail: %d", slaveMinWal, b.Sequence())
				walStart = decUint64(b.Sequence(), uint64(s.CompactLogHead))
			}
			log.Infof("STAGE 0.1: truncate logs using start: %d, slave tail: %d, log tail: %d", walStart, slaveMinWal, b.Sequence())
			walStartBuf = make([]byte, 8)
			binary.BigEndian.PutUint64(walStartBuf, walStart)
		}

		now := time.Now().UnixNano()
		if err = b.ForEach(func(k, v []byte) error {
			if len(walStartBuf) > 0 && bytes.Compare(k, walStartBuf) < 0 {
				return nil
			}
			if isQueue && len(k) == 16 && s.QueueTTLSec > 0 {
				ts := int64(binary.BigEndian.Uint64(k[8:]))
				if (now-ts)/1e9 > int64(s.QueueTTLSec) {
					return nil
				}
			}

			count++
			total++
			if count > s.CompactTxSize {
				err = tmptx.Commit()
				if err != nil {
					return err
				}
				tmptx, err = tmpdb.Begin(true)
				if err != nil {
					return err
				}
				tmpb = tmptx.Bucket(next)
				tmpb.FillPercent = 0.9 // for seq write in for each

				count = 0
			}
			return tmpb.Put(k, v)
		}); err != nil {
			return err
		}

		if len(walStartBuf) > 0 {
			k, _ := tmpb.Cursor().Last()
			if len(k) != 8 {
				log.Infof("STAGE 0.2: truncate logs double check: buffer: %v, tail: %v, seq: %d, count: %d", walStartBuf, k, tmpb.Sequence(), total)
				return fmt.Errorf("FATAL")
			} else {
				log.Infof("STAGE 0.2: truncate logs double check: tail: %d, seq: %d, count: %d", binary.BigEndian.Uint64(k), tmpb.Sequence(), total)
			}
		}

		if isQueue {
			k, _ := tmpb.Cursor().Last()
			if len(k) == 0 {
				tmptx.DeleteBucket(next)
			}
		}
	}

	return tmptx.Commit()
}

func decUint64(v uint64, d uint64) uint64 {
	if v > d {
		return v - d
	}
	return 0
}

func (s *Server) dumpShard(shard int, path string) (int64, error) {
	of, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer of.Close()
	var c int64
	err = s.db[shard].DB.View(func(tx *bbolt.Tx) error {
		c, err = tx.WriteTo(of)
		return err
	})
	return c, err
}

func getPendingUnlinks(db *bbolt.DB) (names []string, err error) {
	if err := db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("unlink"))
		if bk == nil {
			return nil
		}
		bk.ForEach(func(k, v []byte) error {
			if bytes.Equal(v, []byte("unlink")) {
				names = append(names, string(k))
			}
			return nil
		})
		return nil
	}); err != nil {
		return nil, err
	}
	return
}

func requestDumpShardOverWire(remote, output string, shard int) {
	// TODO: password auth
	log.Infof("request dumping shard #%d from %s to %s", shard, remote, output)
	start := time.Now()

	of, err := os.Create(output)
	if err != nil {
		log.Panic("output: ", err)
	}
	defer of.Close()

	conn, err := net.Dial("tcp", remote)
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte(fmt.Sprintf("DUMPSHARD %d WIRE\r\n", shard)))
	if err != nil {
		log.Panic("write command: ", err)
	}

	buf := make([]byte, 32*1024)
	rd, err := gzip.NewReader(conn)
	if err != nil {
		log.Panic("read gzip header: ", err)
	}
	written := 0
	for {
		nr, er := rd.Read(buf)
		if nr > 0 {
			nw, ew := of.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = fmt.Errorf("write output invalid")
				}
			}
			written += nw
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	if err != nil {
		log.Panic("failed to dump: ", err)
	}
	log.Info("dumping finished in ", time.Since(start), ", acquired ", written, "b")
}

func checkDumpWireFile(path string) {
	log.Info("check dump-over-wire file: ", path)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		log.Error("open: ", err)
		os.Exit(1)
	}
	defer f.Close()
	_, err = f.Seek(-32, 2)
	if err != nil {
		log.Error("seek: ", err)
		os.Exit(1)
	}
	buf := make([]byte, 32)
	n, err := io.ReadFull(f, buf)
	if n != 32 {
		log.Error("read: ", n, " ", err)
		os.Exit(1)
	}

	if buf[30] != '\r' || buf[31] != '\n' {
		log.Errorf("invalid tail: %q", buf)
		os.Exit(1)
	}

	if bytes.HasSuffix(buf, []byte("-HALT\r\n")) {
		log.Errorf("remote reported error, dumping failed")
		os.Exit(2)
	}

	idx := bytes.LastIndexByte(buf, ':')
	if idx == -1 {
		log.Errorf("invalid tail: %q", buf)
		os.Exit(1)
	}

	sz, _ := strconv.ParseInt(string(buf[idx+1:30]), 10, 64)
	log.Info("reported shard size: ", sz)

	fsz, err := f.Seek(-int64(32-idx), 2)
	if err != nil {
		log.Error("seek2: ", err)
		os.Exit(1)
	}

	if fsz != sz {
		log.Errorf("unmatched size: %d and %d", fsz, sz)
		os.Exit(3)
	}

	if err := f.Truncate(fsz); err != nil {
		log.Error("failed to truncate: ", err)
		os.Exit(4)
	}
	log.Info("checking finished")
}
