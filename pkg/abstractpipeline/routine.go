package abstractpipeline

import (
	"fmt"
	"sync"
)

type Routine struct {
	Name string
	Impl Processor
	id   int
	cntl routineController
}

type Processor interface {
	Initialise() error
	Terminate() error
	Process(data interface{}, outputDataPipe chan<- interface{}) error
	HandleDataProcessError(err error, data interface{}, outputDataPipe chan<- interface{}) error
}

type routineController struct {
	startWaitGroup *sync.WaitGroup
	log            Loggers
}

type outputPipes struct {
	dataOut              chan interface{}
	terminateCallbackOut chan chan struct{}
	errOut               chan error
}

type inputPipes struct {
	dataIn              chan interface{}
	terminateCallbackIn chan chan struct{}
}

func (routine *Routine) startAndGetOutputPipes(inPipes *inputPipes) (*outputPipes, error) {

	outPipes := &outputPipes{
		dataOut:              make(chan interface{}),
		terminateCallbackOut: make(chan chan struct{}),
		errOut:               make(chan error),
	}

	if err := routine.validate(); err != nil {
		return nil, err
	}

	if err := routine.Impl.Initialise(); err != nil {
		return nil, &InitialiseError{NewGeneralError(routine.Name, err)}
	}

	routine.logStarted()
	routine.cntl.startWaitGroup.Done()

	go func() {
	routineLoop:
		for {
			select {
			case terminateSuccessPipe := <-inPipes.terminateCallbackIn:
				routine.logTerminateSignalReceived()
				routine.terminate(outPipes, terminateSuccessPipe)
				break routineLoop

			case data := <-inPipes.dataIn:
				err := routine.Impl.Process(data, outPipes.dataOut)
				routine.checkAndHandleError(err, data, outPipes)
			}
		}
	}()

	return outPipes, nil
}

const unknownRoutineName string = "Unknown"

func (routine *Routine) validate() error {
	routineName := routine.Name
	if routine.Name == "" {
		routineName = unknownRoutineName
	}

	if routine.Impl == nil {
		return &InitialiseError{NewGeneralError(routine.Name, fmt.Errorf("Routine %s failed to initialise as it had no Impl assigned", routineName))}
	}
	return nil
}

func (routine *Routine) terminate(outPipes *outputPipes, terminateSuccessPipe chan struct{}) {
	err := routine.Impl.Terminate()
	checkAndForwardError(err, outPipes.errOut)
	outPipes.terminateCallbackOut <- terminateSuccessPipe // Propogate termination callback downstream

	close(outPipes.dataOut)
	close(outPipes.terminateCallbackOut)
	close(outPipes.errOut)

	routine.logTerminated()
}

func (routine *Routine) logStarted() {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("%s routine started (ID %d)!", routine.Name, routine.id))
}
func (routine *Routine) logTerminateSignalReceived() {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("Terminate signal received for %s Routine ID (%d)", routine.Name, routine.id))
}
func (routine *Routine) logTerminated() {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("%s routine terminated! (ID %d)", routine.Name, routine.id))
}

func (routine *Routine) checkAndLogError(err error) {
	if err != nil {
		routine.cntl.log.ErrLog.Println(err.Error())
	}
}

// For non-processing errors just send these straight to the error dump, the routine
// won't be able to do much.
func checkAndForwardError(err error, errPipe chan<- error) {
	if err != nil {
		errPipe <- err
	}
}

// The routine might be able to handle the error in it's implementation.  If not, feed it to the
// general error output pipe.
func (routine *Routine) checkAndHandleError(err error, data interface{}, outPipes *outputPipes) {
	if err != nil {
		if unhandledError := routine.Impl.HandleDataProcessError(err, data, outPipes.dataOut); unhandledError != nil {
			outPipes.errOut <- unhandledError
		}
	}
}

func (routine *Routine) validateRoutineOutputPipes(outPipes *outputPipes) error {
	if outPipes == nil {
		return &InitialiseError{NewGeneralError(routine.Name, fmt.Errorf("Wiring error: Routine %s didn't return pipes at all", routine.Name))}
	}
	if outPipes.dataOut == nil {
		return &InitialiseError{NewGeneralError(routine.Name, fmt.Errorf("Wiring error: Routine %s didn't return output pipe", routine.Name))}
	}
	return nil
}
