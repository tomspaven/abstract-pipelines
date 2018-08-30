package abstractpipeline

import (
	"fmt"
	"sync"
)

type RoutineSet struct {
	name        string
	impl        Routine
	numRoutines int
	id          int
	cntl        routineSetController
}

type Routine interface {
	Initialise() error
	Terminate() error
	Process(data interface{}, outputDataPipe chan<- interface{}) error
	HandleDataProcessError(err error, data interface{}, outputDataPipe chan<- interface{}) error
}

type routineSetController struct {
	startWaitGroup *sync.WaitGroup
	log            Loggers
}

const unknownRoutineName string = "Unknown"

func NewRoutine(routineName string, routineImpl Routine, numSubRoutines int) (*RoutineSet, error) {

	candidateRoutine := &RoutineSet{
		name:        routineName,
		impl:        routineImpl,
		numRoutines: numSubRoutines,
	}

	if err := candidateRoutine.validate(); err != nil {
		return nil, err
	}

	return candidateRoutine, nil
}

func (routine *RoutineSet) validate() error {
	if routine.name == "" {
		routine.name = unknownRoutineName
	}

	if routine.numRoutines < 1 {
		routine.numRoutines = 1
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

// Spawn a routine, associated error consolidater for each required routine in the set.
// Also spawn a single terminate signal broadcaster and output pipe merger shared across all routines if more than
// one in the set. Stitch all channels together as per the design.
func (routineset *RoutineSet) startAllAndGetOutputPipes(inPipes *inputPipes, pipelineConsolidatedErrOutPipe chan error) (*outputPipes, error) {

	if err := routineset.validate(); err != nil {
		return nil, err
	}
	// Obtain output pipes for all routines in this routine group.
	// The termination broadcaster will broadcast the termination signal to all routines in the routine set.
	// The output pipes will be merged into a single terminate and data output pipe for the set by the outputmerger
	// routine later
	terminatePipesAllRoutines := routineset.startTerminationBroadcaster(inPipes.terminateCallbackIn)
	dataOutputPipesAllRoutines := make([]*outputPipes, routineset.numRoutines)

	for i := 0; i < routineset.numRoutines; i++ {
		subRoutineID := i + 1
		subRoutineInPipes := &inputPipes{
			inPipes.dataIn,
			terminatePipesAllRoutines[i],
		}

		var err error
		dataOutputPipesAllRoutines[i], err = routineset.startRoutineInstanceAndGetOutPipes(subRoutineID, subRoutineInPipes)
		if err != nil {
			return nil, err
		}

		consolidatorID := fmt.Sprintf("%s:%d", routineset.name, subRoutineID)
		errorConsolidator := newRoutineSetErrorConsolidator(consolidatorID, routineset.cntl.log.OutLog)
		err = errorConsolidator.startConsolidatorAndMerge(dataOutputPipesAllRoutines[i].errOut, pipelineConsolidatedErrOutPipe)
		if err != nil {
			return nil, err
		}
	}

	mergedOutPipes := routineset.mergeRoutineOutPipes(dataOutputPipesAllRoutines)
	return mergedOutPipes, nil
}

func (routine *RoutineSet) startRoutineInstanceAndGetOutPipes(subRoutineID int, inputPipes *inputPipes) (*outputPipes, error) {

	outputPipes := &outputPipes{
		dataOut:              make(chan interface{}),
		terminateCallbackOut: make(chan chan struct{}),
		errOut:               make(chan error),
	}

	if err := routine.impl.Initialise(); err != nil {
		return nil, &InitialiseError{NewGeneralError(routine.name, err)}
	}

	routine.logStarted(subRoutineID)
	routine.cntl.startWaitGroup.Done()

	go func(subRoutineID int) {
	routineLoop:
		for {
			select {
			case terminationSignal := <-inputPipes.terminateCallbackIn:
				routine.logTerminateSignalReceived(subRoutineID)
				routine.terminate(outputPipes, terminationSignal, subRoutineID)
				break routineLoop

			case data := <-inputPipes.dataIn:
				err := routine.impl.Process(data, outputPipes.dataOut)
				routine.checkAndHandleError(err, data, outputPipes)
			}
		}
	}(subRoutineID)

	return outputPipes, nil
}

func (routine *RoutineSet) terminate(outPipes *outputPipes, terminateSuccessPipe chan struct{}, subRoutineID int) {
	err := routine.impl.Terminate()
	checkAndForwardError(err, outPipes.errOut)
	outPipes.terminateCallbackOut <- terminateSuccessPipe // Propogate termination callback upstream

	close(outPipes.dataOut)
	close(outPipes.terminateCallbackOut)
	close(outPipes.errOut)

	routine.logTerminated(subRoutineID)
}

const supplementaryRoutines = 2 // broadcaster and merger
func (routine *RoutineSet) numberOfSubroutinesNeedingSynchonisedStart() int {

	if routine.numRoutines == 1 {
		return 1
	}
	return routine.numRoutines + supplementaryRoutines
}

func (routine *RoutineSet) logStarted(subRoutineID int) {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("%s routine started (ID %d/%d)!", routine.name, subRoutineID, routine.numRoutines))
}
func (routine *RoutineSet) logTerminateSignalReceived(subRoutineID int) {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("Terminate signal received for %s Routine ID (%d/%d)", routine.name, subRoutineID, routine.numRoutines))
}
func (routine *RoutineSet) logTerminated(subRoutineID int) {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("%s routine terminated! (ID %d/%d)", routine.name, subRoutineID, routine.numRoutines))
}

func (routine *RoutineSet) checkAndLogError(err error) {
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
func (routine *RoutineSet) checkAndHandleError(err error, data interface{}, outPipes *outputPipes) {
	if err != nil {
		if unhandledError := routine.impl.HandleDataProcessError(err, data, outPipes.dataOut); unhandledError != nil {
			outPipes.errOut <- unhandledError
		}
	}
}

func (routine *RoutineSet) validateRoutineOutputPipes(outPipes *outputPipes) error {
	if outPipes == nil {
		return &InitialiseError{NewGeneralError(routine.name, fmt.Errorf("Wiring error: Routine %s didn't return pipes at all", routine.name))}
	}
	if outPipes.dataOut == nil {
		return &InitialiseError{NewGeneralError(routine.name, fmt.Errorf("Wiring error: Routine %s didn't return output pipe", routine.name))}
	}
	return nil
}
