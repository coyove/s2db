env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o s2dbench main.go
chmod +x ./s2dbench
