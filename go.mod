module github.com/coyove/s2db

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1 // indirect
	github.com/coyove/nj v0.0.0-20220129113149-00d0984a01c4
	github.com/go-redis/redis/v8 v8.11.0
	github.com/mmcloughlin/geohash v0.10.0
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.6.1 // indirect
	go.etcd.io/bbolt v1.3.6
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4 // indirect
	golang.org/x/sys v0.0.0-20210330210617-4fbd30eecc44
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.4.0 // indirect

)

replace go.etcd.io/bbolt v1.3.6 => ./bbolt
