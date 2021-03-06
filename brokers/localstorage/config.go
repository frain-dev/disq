package localstorage

import (
	"errors"
	"log"
)

type LocalStorageConfig struct {
	Name       string
	Concurency int32
	BufferSize int
	inited     bool
}

func (ls *LocalStorageConfig) Init() error {
	if ls.inited {
		return errors.New("localstorage config already initiated")
	}

	if ls.Name == "" {
		log.Fatalf("LocalStorageConfig.Name is required")
	}

	if ls.Concurency == 0 {
		ls.Concurency = 100
	}

	if ls.BufferSize == 0 {
		ls.BufferSize = 100000
	}
	ls.inited = true
	return nil
}
