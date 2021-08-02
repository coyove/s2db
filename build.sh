SRC='main.go util.go server.go cache.go commands.go range.go metrics.go replication.go geo.go preparer.go runner.go'

MONTH=$(date -u +%m)
if [[ "$MONTH" == "10" ]]; then
    MONTH=O
elif [[ "$MONTH" == "11" ]]; then
    MONTH=X
elif [[ "$MONTH" == "12" ]]; then
    MONTH=Z
else
    MONTH=$(echo $MONTH | cut -c 2-2)
fi

VERSION=$(date -u +%y)
VERSION=$VERSION$MONTH$(date -u +%d%H%M | cut -c 1-5)
echo 'building' $VERSION

go build -ldflags "-X main.Version=$VERSION" -o s2db $SRC
mkdir -p slave_dir
cp s2db slave_dir/

if [[ "$1" == "upload" ]]; then
    env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o s2db $SRC 
    rm -rf s2db.7z
    7z a s2db.7z s2db
    scp s2db.7z root@8.212.30.157:/root
    scp s2db.7z root@47.243.64.196:/data/zset/
fi
