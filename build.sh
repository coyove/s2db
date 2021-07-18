SRC='main.go util.go server.go cache.go commands.go range.go metrics.go replication.go update_cmd.go'
VERSION=$(date -u +%Y%m%d.%H.%M)

rm -rf zset.7z

env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o zset $SRC && 7z a zset.7z zset
go build -ldflags "-X main.Version=$VERSION" -o zset_darwin $SRC

mkdir -p slave_dir
cp zset slave_dir/
cp zset_darwin slave_dir/

if [[ "$1" == "upload" ]]; then
    scp zset.7z root@8.212.30.157:/root
    scp zset.7z root@47.243.64.196:/data/zset/
fi
