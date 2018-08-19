package abstractpipeline

import "fmt"

type generalPiplineError struct {
	pipelineName  string
	previousError error
}
type PipelineInitialiseError generalPiplineError
type PipelineProcessError generalPiplineError
type PipelineTerminateError generalPiplineError

func (e generalPiplineError) Error() string {
	return fmt.Sprintf("General problem with pipeline %s %s", e.pipelineName, e.previousError.Error())
}
func (e *PipelineInitialiseError) Error() string {
	return fmt.Sprintf("Couldn't initialise pipline %s %s", e.pipelineName, e.previousError.Error())
}
func (e *PipelineProcessError) Error() string {
	return fmt.Sprintf("Processing error for pipeline %s %s", e.pipelineName, e.previousError.Error())
}
func (e *PipelineTerminateError) Error() string {
	return fmt.Sprintf("Problem when terminating pipline %s %s", e.pipelineName, e.previousError.Error())
}

func pipelineErrorFactory(genError generalPiplineError, errorType string) error {
	switch errorType {
	case "initialise":
		return &PipelineInitialiseError{genError.pipelineName, genError.previousError}
	case "process":
		return &PipelineInitialiseError{genError.pipelineName, genError.previousError}
	case "terminate":
		return &PipelineInitialiseError{genError.pipelineName, genError.previousError}
	default:
		return genError
	}
}

type PipelineProcessor interface {
	Initialise() error
	Terminate() error
	Process(data interface{}, outputDataPipe chan<- interface{}) error
}

type PipelineRoutine struct {
	Name string
	Impl PipelineProcessor
	Cntl RoutineController
}

func (routine *PipelineRoutine) RunAndGetOutputPipe(inputPipe <-chan interface{}) (outputPipe chan<- interface{}, err error) {

	if err := routine.Impl.Initialise(); err != nil {
		err := pipelineErrorFactory(generalPiplineError{routine.Name, err}, "initialise")
		return createErrorPipe(err), err
	}

	outputPipe = make(chan interface{})
	routine.Cntl.StartWaitGroup.Done()

	stdout := routine.Cntl.Log.OutLog
	stdout.Println(fmt.Sprintf("%s procesing pipeline started!", routine.Name))

	go func() {
	routineLoop:
		for {
			select {
			case <-routine.Cntl.TerminateChan:
				close(outputPipe)
				err = routine.Impl.Initialise()
				routine.checkAndLogError(err, "initialise")
				break routineLoop

			case data := <-inputPipe:
				err = routine.Impl.Process(data, outputPipe)
				routine.checkAndLogError(err, "process")
			}
		}
	}()

	stdout.Println(fmt.Sprintf("%s procesing pipeline terminated!", routine.Name))
	return outputPipe, nil

}

func (routine *PipelineRoutine) checkAndLogError(err error, operationName string) {
	if err != nil {
		err := pipelineErrorFactory(generalPiplineError{routine.Name, err}, operationName)
		routine.Cntl.Log.ErrLog.Println(err.Error())
	}
}

func createErrorPipe(errorData error) (pipe chan<- interface{}) {
	// Rather than sending back a nil channel in the event of an error
	// send back a single element buffered channel with the error queued up on it.
	errorPipe := make(chan interface{}, 1)
	errorPipe <- errorData
	return errorPipe
}
