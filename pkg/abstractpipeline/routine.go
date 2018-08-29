package abstractpipeline

import (
	"fmt"
	"sync"
)

type Routine struct {
	name           string
	impl           Processor
	numSubRoutines int
	id             int
	cntl           routineController
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

const unknownRoutineName string = "Unknown"

func NewRoutine(routineName string, routineImpl Processor, numSubRoutines int) (*Routine, error) {

	candidateRoutine := &Routine{
		name:           routineName,
		impl:           routineImpl,
		numSubRoutines: numSubRoutines,
	}

	if err := candidateRoutine.validate(); err != nil {
		return nil, err
	}

	return candidateRoutine, nil
}

func (routine *Routine) validate() error {
	if routine.name == "" {
		routine.name = unknownRoutineName
	}

	if routine.numSubRoutines < 1 {
		routine.numSubRoutines = 1
	}

	if routine.impl == nil {
		return &InitialiseError{NewGeneralError(routine.name, fmt.Errorf("Routine %s failed to initialise as it had no Impl assigned", routine.name))}
	}

	return nil
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

func (routine *Routine) startAndGetOutputPipes(inPipes *inputPipes, pipelineConolidatedErrOutPipe chan error) (*outputPipes, error) {

	if err := routine.validate(); err != nil {
		return nil, err
	}

	allTerminateCallbackPipes := routine.startTerminationBroadcaster(inPipes.terminateCallbackIn)
	allSubRoutineOutPipes := make([]*outputPipes, routine.numSubRoutines)

	for i := 0; i < routine.numSubRoutines; i++ {
		subRoutineID := i + 1
		subRoutineInPipes := &inputPipes{
			inPipes.dataIn,
			allTerminateCallbackPipes[i],
		}

		currentSubRoutineOutputPipes := &outputPipes{
			dataOut:              make(chan interface{}),
			terminateCallbackOut: make(chan chan struct{}),
			errOut:               make(chan error),
		}

		allSubRoutineOutPipes[i] = currentSubRoutineOutputPipes

		if err := routine.impl.Initialise(); err != nil {
			return nil, &InitialiseError{NewGeneralError(routine.name, err)}
		}

		routine.startErrorConsolidatorAndMerge(currentSubRoutineOutputPipes.errOut, pipelineConolidatedErrOutPipe, subRoutineID)

		routine.logStarted(subRoutineID)
		routine.cntl.startWaitGroup.Done()

		go func(subRoutineID int) {
		routineLoop:
			for {
				select {
				case terminationSignal := <-subRoutineInPipes.terminateCallbackIn:
					routine.logTerminateSignalReceived(subRoutineID)
					routine.terminate(currentSubRoutineOutputPipes, terminationSignal, subRoutineID)
					break routineLoop

				case data := <-inPipes.dataIn:
					err := routine.impl.Process(data, currentSubRoutineOutputPipes.dataOut)
					routine.checkAndHandleError(err, data, currentSubRoutineOutputPipes)
				}
			}
		}(subRoutineID)
	}

	mergedOutPipes := routine.startSubRoutineOutPipeMergers(allSubRoutineOutPipes)
	return mergedOutPipes, nil
}

func (routine *Routine) terminate(outPipes *outputPipes, terminateSuccessPipe chan struct{}, subRoutineID int) {
	err := routine.impl.Terminate()
	checkAndForwardError(err, outPipes.errOut)
	outPipes.terminateCallbackOut <- terminateSuccessPipe // Propogate termination callback upstream

	close(outPipes.dataOut)
	close(outPipes.terminateCallbackOut)
	close(outPipes.errOut)

	routine.logTerminated(subRoutineID)
}

const supplementaryRoutines = 2 // broadcaster and merger
func (routine *Routine) numberOfSubroutinesNeedingSynchonisedStart() int {

	if routine.numSubRoutines == 1 {
		return 1
	}
	return routine.numSubRoutines + supplementaryRoutines
}

func (routine *Routine) logStarted(subRoutineID int) {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("%s routine started (ID %d/%d)!", routine.name, subRoutineID, routine.numSubRoutines))
}
func (routine *Routine) logTerminateSignalReceived(subRoutineID int) {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("Terminate signal received for %s Routine ID (%d/%d)", routine.name, subRoutineID, routine.numSubRoutines))
}
func (routine *Routine) logTerminated(subRoutineID int) {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("%s routine terminated! (ID %d/%d)", routine.name, subRoutineID, routine.numSubRoutines))
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
		if unhandledError := routine.impl.HandleDataProcessError(err, data, outPipes.dataOut); unhandledError != nil {
			outPipes.errOut <- unhandledError
		}
	}
}

func (routine *Routine) validateRoutineOutputPipes(outPipes *outputPipes) error {
	if outPipes == nil {
		return &InitialiseError{NewGeneralError(routine.name, fmt.Errorf("Wiring error: Routine %s didn't return pipes at all", routine.name))}
	}
	if outPipes.dataOut == nil {
		return &InitialiseError{NewGeneralError(routine.name, fmt.Errorf("Wiring error: Routine %s didn't return output pipe", routine.name))}
	}
	return nil
}
