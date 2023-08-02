//go:build !windows
// +build !windows

package top

import (
	"bytes"
	"io/ioutil"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/coyove/s2db/s2"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

var CPUUsages, DiskUsages sync.Map

func init() {
	go osWatcher()
}

func osWatcher() {
	if runtime.GOOS != "linux" {
		return
	}

	cpus := map[int][2][2]int64{}
	disks := map[string][2][2]int64{}

	x := 0
	const interval = 10
	for range time.Tick(time.Second * interval) {
		func() {
			defer rec()

			buf, _ := ioutil.ReadFile("/proc/stat")
			x = (x + 1) % 2
			for _, line := range bytes.Split(buf, []byte("\n")) {
				line := string(line)
				if strings.HasPrefix(line, "cpu") && !strings.HasPrefix(line, "cpu ") {
					n := s2.ParseInt(strings.TrimSpace(line[3:5]))
					used, total := parseCPU(line)
					old := cpus[n]
					old[x] = [2]int64{used, total}
					cpus[n] = old
				}
			}

			for n, stats := range cpus {
				usedDiff := stats[0][0] - stats[1][0]
				if usedDiff < 0 {
					usedDiff = -usedDiff
				}
				totalDiff := stats[0][1] - stats[1][1]
				if totalDiff < 0 {
					totalDiff = -totalDiff
				}
				sx, _ := CPUUsages.LoadOrStore(n, new(s2.Survey))
				if usedDiff > 0 && totalDiff > 0 {
					sx.(*s2.Survey).Incr(int64(float64(usedDiff) / float64(totalDiff) * 100))
				}
			}

			fis, _ := ioutil.ReadDir("/sys/block")
			for _, fi := range fis {
				n := fi.Name()
				buf, _ := ioutil.ReadFile("/sys/block/" + n + "/stat")
				r, w := parseDiskIOPS(string(buf))
				old := disks[n]
				old[x] = [2]int64{r, w}
				disks[n] = old
			}

			for n, stats := range disks {
				rDiff := stats[0][0] - stats[1][0]
				if rDiff < 0 {
					rDiff = -rDiff
				}
				wDiff := stats[0][1] - stats[1][1]
				if wDiff < 0 {
					wDiff = -wDiff
				}
				if stats[0][0] > 0 && stats[1][0] > 0 && stats[0][1] > 0 && stats[1][1] > 0 {
					sx, _ := DiskUsages.LoadOrStore(n, new([2]s2.Survey))
					s := sx.(*[2]s2.Survey)
					(*s)[0].Incr(rDiff / interval)
					(*s)[1].Incr(wDiff / interval)
				}
			}
		}()
	}
}

func rec() {
	if r := recover(); r != nil {
		logrus.Error("fatal: ", r, " ", string(debug.Stack()))
	}
}

func parseCPU(line string) (used, total int64) {
	defer rec()
	x := regexp.MustCompile(`[^\w](\d+)`).FindAllStringSubmatch(line, -1)
	user := s2.MustParseInt64(x[0][1])
	nice := s2.MustParseInt64(x[1][1])
	system := s2.MustParseInt64(x[2][1])
	idle := s2.MustParseInt64(x[3][1])
	iowait := s2.MustParseInt64(x[4][1])
	irq := s2.MustParseInt64(x[5][1])
	softirq := s2.MustParseInt64(x[6][1])
	steal := s2.MustParseInt64(x[7][1])
	idleAll := idle + iowait
	nonIdleAll := user + nice + system + irq + softirq + steal
	return nonIdleAll, nonIdleAll + idleAll
}

func parseDiskIOPS(line string) (read, write int64) {
	defer rec()
	x := regexp.MustCompile(`(\d+)`).FindAllStringSubmatch(line, -1)
	return s2.MustParseInt64(x[0][1]), s2.MustParseInt64(x[4][1])
}

func PrintCPU() (cpu []string) {
	for i := 0; ; i++ {
		v, ok := CPUUsages.Load(i)
		if !ok {
			break
		}
		cpu = append(cpu, v.(*s2.Survey).MeanString())
	}
	return
}

func PrintDiskIOPS() (diskIOPS map[string][2]string) {
	diskIOPS = map[string][2]string{}
	DiskUsages.Range(func(k, v interface{}) bool {
		x := v.(*[2]s2.Survey)
		diskIOPS[k.(string)] = [2]string{(*x)[0].MeanString(), (*x)[1].MeanString()}
		return true
	})
	return
}

func PrintDiskFree(dir string) (free, total uint64) {
	var stat unix.Statfs_t
	unix.Statfs(dir, &stat)
	return stat.Bavail * uint64(stat.Bsize), stat.Blocks * uint64(stat.Bsize)
}
