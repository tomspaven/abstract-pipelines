package abstractpipeline

import "fmt"

type RoutineImpl interface {
	Initialise() error
	Terminate() error
	Process(data interface{}, outputDataPipe chan<- interface{}) error
	HandleDataProcessError(err error, data interface{}, outputDataPipe chan<- interface{}) error
}

type routineStarter interface {
	startAndGetOutputPipes(inputPipes *inputPipes) (*outputPipes, error)
}

type routine struct {
	impl         RoutineImpl
	subRoutineID string
	cntl         routineSetController
}

func newRoutine(impl RoutineImpl, subRoutineID string, cntl routineSetController) routineStarter {
	return &routine{impl, subRoutineID, cntl}
}

func (r *routine) startAndGetOutputPipes(inputPipes *inputPipes) (*outputPipes, error) {

	outputPipes := &outputPipes{
		dataOut:              make(dataPipe),
		terminateCallbackOut: make(terminationRqRsChan),
		errOut:               make(errorPipe),
	}

	if err := r.impl.Initialise(); err != nil {
		return nil, &InitialiseError{NewGeneralError(r.subRoutineID, err)}
	}

	r.logStarted()
	r.cntl.startWaitGroup.Done()

	go func() {
	routineLoop:
		for {
			select {
			case terminationSignal := <-inputPipes.terminateCallbackIn:
				r.logTerminateSignalReceived()
				r.terminate(outputPipes, terminationSignal)
				break routineLoop

			case data := <-inputPipes.dataIn:
				err := r.impl.Process(data, outputPipes.dataOut)
				r.checkAndHandleError(err, data, outputPipes)
			}
		}
	}()

	return outputPipes, nil

}

func (r *routine) terminate(outPipes *outputPipes, terminateSuccessPipe terminationResponseChan) {
	err := r.impl.Terminate()
	checkAndForwardError(err, outPipes.errOut)
	outPipes.terminateCallbackOut <- terminateSuccessPipe // Propogate termination callback upstream

	close(outPipes.dataOut)
	close(outPipes.terminateCallbackOut)
	close(outPipes.errOut)

	r.logTerminated()
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
func (r *routine) checkAndHandleError(err error, data interface{}, outPipes *outputPipes) {
	if err != nil {
		if unhandledError := r.impl.HandleDataProcessError(err, data, outPipes.dataOut); unhandledError != nil {
			outPipes.errOut <- unhandledError
		}
	}
}

func (r *routine) logStarted() {
	r.cntl.log.OutLog.Println(fmt.Sprintf("Routine %s started", r.subRoutineID))
}

func (r *routine) logTerminateSignalReceived() {
	r.cntl.log.OutLog.Println(fmt.Sprintf("Terminate signal received for routine %s", r.subRoutineID))
}
func (r *routine) logTerminated() {
	r.cntl.log.OutLog.Println(fmt.Sprintf("Routine %s terminated!", r.subRoutineID))
}
