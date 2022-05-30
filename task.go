package disq

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

type Task struct {
	opt     *TaskOptions
	handler Handler
}

type TaskOptions struct {
	Name string

	Handler interface{}

	RetryLimit int
}

func (opt *TaskOptions) init() {
	if opt.Name == "" {
		log.Fatalf("TaskOptions.Name is required")
	}
	if opt.RetryLimit == 0 {
		opt.RetryLimit = 3
	}
}

func NewTask(opt *TaskOptions) *Task {
	opt.init()
	return &Task{
		handler: NewHandler(opt.Handler),
		opt:     opt,
	}
}

func (t *Task) Name() string {
	return t.opt.Name
}

func (t *Task) RetryLimit() int {
	return t.opt.RetryLimit
}

func (t *Task) HandleMessage(msg *Message) error {
	return t.handler.HandleMessage(msg)
}

func RegisterTask(opt *TaskOptions) (*Task, error) {
	task, err := Tasks.RegisterTasks(opt)
	if err != nil {
		return nil, err
	}
	return task, nil
}

type TaskMap struct {
	m sync.Map
}

var Tasks TaskMap

func (s *TaskMap) RegisterTasks(opts *TaskOptions) (*Task, error) {
	task := NewTask(opts)

	name := task.Name()
	_, loaded := s.m.LoadOrStore(name, task)
	if loaded {
		return task, fmt.Errorf("task=%q already exists", name)
	}
	return task, nil

}

func (s *TaskMap) LoadTask(name string) (*Task, error) {
	if v, ok := s.m.Load(name); ok {
		return v.(*Task), nil
	}
	if v, ok := s.m.Load("*"); ok {
		return v.(*Task), nil
	}
	return nil, fmt.Errorf("task=%q not found", name)
}

func (s *TaskMap) UpdateTask(name string, task *Task) error {
	if _, ok := s.m.LoadAndDelete(name); ok {
		s.m.Store(name, task)
		Logger.Printf("succesfully updated task=%s", name)
		return nil
	} else {
		Logger.Printf("task=%s not found, adding instead.", name)
		_, loaded := s.m.LoadOrStore(name, task)
		if loaded {
			return fmt.Errorf("task=%q already exists", name)
		}
	}
	return nil
}
