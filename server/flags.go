package server

import (
	"flag"
	"sync"

	log "github.com/sirupsen/logrus"
)

var (
	pebbleMemtableSize       = flag.Int("pebble.memtablesize", 128, "[pebble] memtable size in megabytes")
	pebbleCacheSize          = flag.Int("pebble.cachesize", 1024, "[pebble] cache size in megabytes")
	pebbleMaxOpenFiles       = flag.Int("pebble.maxopenfiles", 1024, "[pebble] max open files")
	influxdb1MetricsEndpoint = flag.String("metrics.influxdb1", "", "")

	testFlag  bool
	testDedup sync.Map
)

func EnableTest() {
	testFlag = true
}

func PrintAllFlags() {
	log.Info("version: ", Version)
	flag.VisitAll(func(f *flag.Flag) {
		if v := f.Value.String(); v != "" && f.Name != "v" {
			log.Info("[flag] ", f.Name, "=", v)
		}
	})
}
