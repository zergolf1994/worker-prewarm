//go:build !windows

package queue

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

// getDiskUsage returns total, used, free bytes for the given path.
func getDiskUsage(path string) (total, used, free int64) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		// storage path may not exist yet — fall back to root
		if err := syscall.Statfs("/", &stat); err != nil {
			return 0, 0, 0
		}
	}
	total = int64(stat.Blocks) * int64(stat.Bsize)
	free = int64(stat.Bavail) * int64(stat.Bsize)
	used = total - free
	return
}

// getMemoryUsage returns total and used memory in bytes from /proc/meminfo.
func getMemoryUsage() (total, used int64) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	var memTotal, memAvailable int64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		val, _ := strconv.ParseInt(fields[1], 10, 64)
		val *= 1024 // kB → bytes
		switch fields[0] {
		case "MemTotal:":
			memTotal = val
		case "MemAvailable:":
			memAvailable = val
		}
	}
	return memTotal, memTotal - memAvailable
}

// getCPUPercent — heartbeat reports 0; the dashboard derives trends from
// history. (A real % needs two /proc/stat samples with a sleep between.)
func getCPUPercent() float64 {
	return 0
}
