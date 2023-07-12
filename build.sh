SRC='main.go'

COMMIT=$(git log --pretty=format:'%h' -n 1)
VERSION=$(($(date -u +%y)-20))
VERSION=${VERSION}.$(date -u +%m%d%H%M).${COMMIT}
echo 'building' $VERSION

if [[ "$1" == "linux" ]]; then
    env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-X github.com/coyove/s2db/server.Version=$VERSION" -o s2db $SRC 
    chmod +x ./s2db
    exit 0
fi

OUT=s2db
if [[ "$1" == "win32" ]]; then
    OUT=s2db.exe
fi

CGO_ENABLED=0 go build -ldflags "-X github.com/coyove/s2db/server.Version=$VERSION" -o $OUT $SRC
