module github.com/coyove/s2db

go 1.18

require (
	github.com/cockroachdb/errors v1.8.1
	github.com/cockroachdb/pebble v0.0.0-20230728153158-ce43e6535942
	github.com/coyove/nj v0.0.0-20221110084952-c7f8db1065c3
	github.com/coyove/sdss v0.0.0-20230523113413-30422880d2d1
	github.com/go-redis/redis/v8 v8.11.0
	github.com/influxdata/influxdb1-client v0.0.0-20220302092344-a9ab5670611c
	github.com/influxdata/tdigest v0.0.1
	github.com/pierrec/lz4/v4 v4.1.17
	github.com/sirupsen/logrus v1.9.0
	golang.org/x/sys v0.3.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230613231145-182959a1fad6 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/klauspost/compress v1.15.15 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.12.0 // indirect
	github.com/prometheus/client_model v0.2.1-0.20210607210712-147c58e9608a // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	golang.org/x/exp v0.0.0-20200513190911-00229845015e // indirect
	google.golang.org/protobuf v1.27.1 // indirect
)

// replace github.com/coyove/sdss v0.0.0-20230425082228-f6661bc969b0 => ../sdss
