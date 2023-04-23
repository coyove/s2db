package main

import (
	"bytes"

	"github.com/coyove/s2db/wire"
)

const (
	RunNormal = iota + 1
	RunDefer
	RunSync
)

func parseRunFlag(in *wire.Command) int {
	if len(in.Argv) > 2 {
		if bytes.EqualFold(in.Argv[2], []byte("--defer--")) {
			in.Argv = append(in.Argv[:2], in.Argv[3:]...)
			return RunDefer
		}
		if bytes.EqualFold(in.Argv[2], []byte("--sync--")) {
			in.Argv = append(in.Argv[:2], in.Argv[3:]...)
			return RunSync
		}
		if bytes.EqualFold(in.Argv[2], []byte("--normal--")) {
			in.Argv = append(in.Argv[:2], in.Argv[3:]...)
		}
	}
	return RunNormal
}
