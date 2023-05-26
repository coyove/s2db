module github.com/coyove/s2db

go 1.18

require (
	github.com/cockroachdb/errors v1.8.1
	github.com/cockroachdb/pebble v0.0.0-20220506213004-f8897076324b
	github.com/coyove/nj v0.0.0-20221110084952-c7f8db1065c3
	github.com/coyove/sdss v0.0.0-20230523113413-30422880d2d1
	github.com/go-redis/redis/v8 v8.11.0
	github.com/influxdata/influxdb1-client v0.0.0-20220302092344-a9ab5670611c
	github.com/influxdata/tdigest v0.0.1
	github.com/pierrec/lz4/v4 v4.1.17
	github.com/sirupsen/logrus v1.9.0
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/klauspost/compress v1.11.7 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/exp v0.0.0-20200513190911-00229845015e // indirect
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

// replace github.com/coyove/sdss v0.0.0-20230425082228-f6661bc969b0 => ../sdss
