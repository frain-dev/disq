package disq

import (
	"math/rand"
	"os"
	"strconv"
	"time"
)

func UnixMs(tm time.Time) int64 {
	return tm.UnixNano() / int64(time.Millisecond)
}

func ConsumerName() string {
	s, _ := os.Hostname()
	s += ":pid:" + strconv.Itoa(os.Getpid())
	s += ":" + strconv.Itoa(rand.Int())
	return s
}

func DurEqual(d1, d2 time.Duration, threshold int) bool {
	return (d2 >= d1 && (d2-d1) < time.Duration(threshold)*time.Second)
}
