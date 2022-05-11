package redis

import (
	"errors"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/frain-dev/disq"
)

type RedisConfig struct {
	Name               string
	StreamGroup        string
	Concurency         int32
	ReservationSize    int
	WaitTimeout        time.Duration
	ReservationTimeout time.Duration
	BufferSize         int
	Redis              disq.Redis
	inited             bool
}

func (rc *RedisConfig) Init() error {
	if rc.inited {
		return errors.New("redis config already initiated")
	}
	if rc.Name == "" {
		log.Fatalf("RedisConfig.Name is required")
	}

	if rc.StreamGroup == "" {
		rc.StreamGroup = "disq:"
	}

	if rc.Concurency == 0 {
		rc.Concurency = 100
	}

	if rc.ReservationSize == 0 {
		rc.ReservationSize = 1000
	}

	if rc.ReservationTimeout == 0 {
		rc.ReservationTimeout = 10 * time.Minute
	}

	if rc.BufferSize == 0 {
		rc.BufferSize = rc.ReservationSize
	}
	if rc.WaitTimeout == 0 {
		rc.WaitTimeout = 10 * time.Second
	}
	rc.inited = true

	return nil
}
