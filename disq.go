package disq

import (
	"log"
	"os"
)

var Logger *log.Logger

func init() {
	SetLogger(log.New(os.Stderr, "disq: ", log.LstdFlags|log.Lshortfile))
}

func SetLogger(logger *log.Logger) {
	Logger = logger
}
