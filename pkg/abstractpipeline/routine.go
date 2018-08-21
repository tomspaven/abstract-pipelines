package abstractpipeline

import (
	"fmt"
	"log"
	"sync"
)

type Routine struct {
	Name string
	Impl Processor
	Cntl RoutineController
}

type Processor interface {
	Initialise() error
	Terminate() error
	Process(data interface{}, outputDataPipe chan<- interface{}) error
	HandleDataProcessError(err error, data interface{}, outputDataPipe <-chan interface{})
}

type RoutineController struct {
	StartWaitGroup *sync.WaitGroup
	TerminateChan  chan struct{}
	Log            Loggers
}

type Loggers struct {
	OutLog *log.Logger
	ErrLog *log.Logger
}

func (routine *Routine) runAndGetOutputPipe(inputPipe <-chan interface{}) <-chan interface{} {

	if err := routine.Impl.Initialise(); err != nil {
		routine.checkAndLogError(err)
		return nil
	}

	outputPipe := make(chan interface{})
	routine.Cntl.StartWaitGroup.Done()

	stdout := routine.Cntl.Log.OutLog
	stdout.Println(fmt.Sprintf("%s procesing pipeline started!", routine.Name))

	go func() {
	routineLoop:
		for {
			select {
			case <-routine.Cntl.TerminateChan:
				close(outputPipe)
				err := routine.Impl.Terminate()
				routine.checkAndLogError(err)

				stdout.Println(fmt.Sprintf("%s procesing pipeline terminated!", routine.Name))
				break routineLoop

			case data := <-inputPipe:
				err := routine.Impl.Process(data, outputPipe)
				routine.checkAndHandleError(err, data, outputPipe)
			}
		}
	}()

	return outputPipe

}

func (routine *Routine) checkAndLogError(err error) {
	if err != nil {
		routine.Cntl.Log.ErrLog.Println(err.Error())
	}
}

func (routine *Routine) checkAndHandleError(err error, data interface{}, outPipe <-chan interface{}) {
	if err != nil {
		routine.Impl.HandleDataProcessError(err, data, outPipe)
	}
}
