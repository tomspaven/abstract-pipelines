package abstractpipeline

import (
	"log"
	"sync"
)

type RoutineController struct {
	StartWaitGroup *sync.WaitGroup
	TerminateChan  chan struct{}
	Log            Loggers
}

type Loggers struct {
	OutLog *log.Logger
	ErrLog *log.Logger
}
