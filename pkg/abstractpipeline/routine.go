package abstractpipeline

import (
	"fmt"
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

type Routine struct {
	Name string
	Impl Processor
	Cntl RoutineController
}

func (routine *Routine) runAndGetOutputPipe(inputPipe <-chan interface{}) <-chan interface{} {

	if err := routine.Impl.Initialise(); err != nil {
		err := pipelineErrorFactory(generalError{routine.Name, err}, "initialise")
		return createErrorPipe(err)
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
				routine.checkAndLogError(err, "terminate")

				stdout.Println(fmt.Sprintf("%s procesing pipeline terminated!", routine.Name))
				break routineLoop

			case data := <-inputPipe:
				err := routine.Impl.Process(data, outputPipe)
				routine.checkAndLogError(err, "process")
			}
		}
	}()

	return outputPipe

}

func (routine *Routine) checkAndLogError(err error, operationName string) {
	if err != nil {
		err := pipelineErrorFactory(generalError{routine.Name, err}, operationName)
		routine.Cntl.Log.ErrLog.Println(err.Error())
	}
}

func createErrorPipe(errorData error) <-chan interface{} {
	// Rather than sending back a nil channel in the event of an error
	// send back a single element buffered channel with the error queued up on it.
	errorPipe := make(chan interface{}, 1)
	errorPipe <- errorData
	return errorPipe
}
