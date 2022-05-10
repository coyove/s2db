module github.com/coyove/s2db

go 1.16

require (
	github.com/RoaringBitmap/roaring v0.9.4
	github.com/adamzy/cedar-go v0.0.0-20170805034717-80a9c64b256d
	github.com/cockroachdb/pebble v0.0.0-20220506213004-f8897076324b
	github.com/coyove/nj v0.0.0-20220509062621-58858f0dd837
	github.com/go-redis/redis/v8 v8.11.0
	github.com/golang/protobuf v1.5.2
	github.com/mmcloughlin/geohash v0.10.0
	github.com/sirupsen/logrus v1.8.1
	go.etcd.io/bbolt v1.3.6
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4 // indirect
	golang.org/x/sys v0.0.0-20220503163025-988cb79eb6c6
	golang.org/x/time v0.0.0-20220411224347-583f2d630306
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace go.etcd.io/bbolt v1.3.6 => ./bbolt
