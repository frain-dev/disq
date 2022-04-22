package disq

import "github.com/frain-dev/disq"

type Server struct {
	port       string
	workerport string
}

func (s *Server) NewWorker(brokers []disq.Broker, workercfg disq.WorkerConfig) *disq.Worker {
	return disq.NewWorker(brokers, workercfg)
}
