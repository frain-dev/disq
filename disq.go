package disq

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

var Logger *log.Logger

func init() {
	SetLogger(log.New(os.Stderr, "disq: ", log.LstdFlags|log.Lshortfile))
}

func SetLogger(logger *log.Logger) {
	Logger = logger
}

func UnixMs(tm time.Time) int64 {
	return tm.UnixNano() / int64(time.Millisecond)
}

func ConsumerName() string {
	s, _ := os.Hostname()
	s += ":pid:" + strconv.Itoa(os.Getpid())
	s += ":" + strconv.Itoa(rand.Int())
	return s
}
