package s2

import (
	"sync"
)

var CPUUsages, DiskUsages sync.Map

func OSWatcher() {
}

func GetOSUsage(dbPaths []string) (cpu []string, diskIOPS map[string][2]string, diskFree map[string][2]uint64) {
	return
}
