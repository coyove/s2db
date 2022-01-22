//+build !windows

package s2pkg

import (
	"bytes"
	"io/ioutil"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

var CPUUsages, DiskUsages sync.Map

func OSWatcher() {
	if runtime.GOOS != "linux" {
		return
	}

	cpus := map[int][2][2]int64{}
	disks := map[string][2][2]int64{}

	x := 0
	const interval = 10
	for range time.Tick(time.Second * interval) {
		func() {
			defer Recover()

			buf, _ := ioutil.ReadFile("/proc/stat")
			x = (x + 1) % 2
			for _, line := range bytes.Split(buf, []byte("\n")) {
				line := string(line)
				if strings.HasPrefix(line, "cpu") && !strings.HasPrefix(line, "cpu ") {
					n := ParseInt(strings.TrimSpace(line[3:5]))
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
				sx, _ := CPUUsages.LoadOrStore(n, new(Survey))
				if usedDiff > 0 && totalDiff > 0 {
					sx.(*Survey).Incr(int64(float64(usedDiff) / float64(totalDiff) * 100))
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
					sx, _ := DiskUsages.LoadOrStore(n, new([2]Survey))
					s := sx.(*[2]Survey)
					(*s)[0].Incr(rDiff / interval)
					(*s)[1].Incr(wDiff / interval)
				}
			}
		}()
	}
}

func parseCPU(line string) (used, total int64) {
	defer Recover()
	x := regexp.MustCompile(`[^\w](\d+)`).FindAllStringSubmatch(line, -1)
	user := MustParseInt64(x[0][1])
	nice := MustParseInt64(x[1][1])
	system := MustParseInt64(x[2][1])
	idle := MustParseInt64(x[3][1])
	iowait := MustParseInt64(x[4][1])
	irq := MustParseInt64(x[5][1])
	softirq := MustParseInt64(x[6][1])
	steal := MustParseInt64(x[7][1])
	idleAll := idle + iowait
	nonIdleAll := user + nice + system + irq + softirq + steal
	return nonIdleAll, nonIdleAll + idleAll
}

func parseDiskIOPS(line string) (read, write int64) {
	defer Recover()
	x := regexp.MustCompile(`(\d+)`).FindAllStringSubmatch(line, -1)
	return MustParseInt64(x[0][1]), MustParseInt64(x[4][1])
}

func GetOSUsage(dbPaths []string) (cpu []string, diskIOPS map[string][2]string, diskFree map[string][2]uint64) {
	for i := 0; ; i++ {
		v, ok := CPUUsages.Load(i)
		if !ok {
			break
		}
		cpu = append(cpu, v.(*Survey).MeanString())
	}

	diskIOPS = map[string][2]string{}
	DiskUsages.Range(func(k, v interface{}) bool {
		x := v.(*[2]Survey)
		diskIOPS[k.(string)] = [2]string{(*x)[0].MeanString(), (*x)[1].MeanString()}
		return true
	})

	diskFree = make(map[string][2]uint64)
	for _, dir := range dbPaths {
		var stat unix.Statfs_t
		unix.Statfs(dir, &stat)
		diskFree[dir] = [2]uint64{
			stat.Bavail * uint64(stat.Bsize) / 1024 / 1024,
			stat.Blocks * uint64(stat.Bsize) / 1024 / 1024,
		}
	}
	return
}
