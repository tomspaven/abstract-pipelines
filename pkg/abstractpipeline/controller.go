package abstractpipeline

import (
	"log"
	"sync"
)

type RoutineController struct {
	StartWaitGroup *sync.WaitGroup
	TerminateChan  <-chan bool
	Log            Loggers
}

type Loggers struct {
	outLog *log.Logger
	errLog *log.Logger
}
