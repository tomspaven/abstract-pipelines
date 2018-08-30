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

func NewRoutineSet(routineSetName string, routineImpl Routine, numSubRoutines int) (*RoutineSet, error) {

	candidateRoutineSet := &RoutineSet{
		name:        routineSetName,
		impl:        routineImpl,
		numRoutines: numSubRoutines,
	}

	if err := candidateRoutineSet.validate(); err != nil {
		return nil, err
	}

	return candidateRoutineSet, nil
}

const unknownRoutineSetName string = "Unknown"

func (routineset *RoutineSet) validate() error {
	if routineset.name == "" {
		routineset.name = unknownRoutineSetName
	}

	if routineset.numRoutines < 1 {
		routineset.numRoutines = 1
	}

	if routineset.impl == nil {
		return &InitialiseError{NewGeneralError(routineset.name, fmt.Errorf("Routineset %s failed to initialise as it had no Impl assigned", routineset.name))}
	}

	return nil
}

// Spawn a routine, associated error consolidater for each required routine in the set.
// Also spawn a single terminate signal broadcaster and output pipe merger shared across all routines if more than
// one in the set. Stitch all channels together as per the design.
func (routineset *RoutineSet) startAllAndGetOutputPipes(inPipes *inputPipes, pipelineConsolidatedErrOutPipe chan error) (*outputPipes, error) {

	if err := routineset.validate(); err != nil {
		return nil, err
	}
	// Obtain output pipes for all routines in this routine set.
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

func (routineset *RoutineSet) startRoutineInstanceAndGetOutPipes(subRoutineID int, inputPipes *inputPipes) (*outputPipes, error) {

	outputPipes := &outputPipes{
		dataOut:              make(chan interface{}),
		terminateCallbackOut: make(chan chan struct{}),
		errOut:               make(chan error),
	}

	if err := routineset.impl.Initialise(); err != nil {
		return nil, &InitialiseError{NewGeneralError(routineset.name, err)}
	}

	routineset.logStarted(subRoutineID)
	routineset.cntl.startWaitGroup.Done()

	go func(subRoutineID int) {
	routineLoop:
		for {
			select {
			case terminationSignal := <-inputPipes.terminateCallbackIn:
				routineset.logTerminateSignalReceived(subRoutineID)
				routineset.terminate(outputPipes, terminationSignal, subRoutineID)
				break routineLoop

			case data := <-inputPipes.dataIn:
				err := routineset.impl.Process(data, outputPipes.dataOut)
				routineset.checkAndHandleError(err, data, outputPipes)
			}
		}
	}(subRoutineID)

	return outputPipes, nil
}

func (routineset *RoutineSet) terminate(outPipes *outputPipes, terminateSuccessPipe chan struct{}, subRoutineID int) {
	err := routineset.impl.Terminate()
	checkAndForwardError(err, outPipes.errOut)
	outPipes.terminateCallbackOut <- terminateSuccessPipe // Propogate termination callback upstream

	close(outPipes.dataOut)
	close(outPipes.terminateCallbackOut)
	close(outPipes.errOut)

	routineset.logTerminated(subRoutineID)
}

const supplementaryRoutines = 2 // broadcaster and merger
func (routineset *RoutineSet) numberOfSubroutinesNeedingSynchonisedStart() int {

	if routineset.numRoutines == 1 {
		return 1
	}
	return routineset.numRoutines + supplementaryRoutines
}

func (routineset *RoutineSet) logStarted(subRoutineID int) {
	routineset.cntl.log.OutLog.Println(fmt.Sprintf("%s routine started (ID %d/%d)!", routineset.name, subRoutineID, routineset.numRoutines))
}
func (routineset *RoutineSet) logTerminateSignalReceived(subRoutineID int) {
	routineset.cntl.log.OutLog.Println(fmt.Sprintf("Terminate signal received for %s Routine ID (%d/%d)", routineset.name, subRoutineID, routineset.numRoutines))
}
func (routineset *RoutineSet) logTerminated(subRoutineID int) {
	routineset.cntl.log.OutLog.Println(fmt.Sprintf("%s routine terminated! (ID %d/%d)", routineset.name, subRoutineID, routineset.numRoutines))
}

func (routineset *RoutineSet) checkAndLogError(err error) {
	if err != nil {
		routineset.cntl.log.ErrLog.Println(err.Error())
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
func (routineset *RoutineSet) checkAndHandleError(err error, data interface{}, outPipes *outputPipes) {
	if err != nil {
		if unhandledError := routineset.impl.HandleDataProcessError(err, data, outPipes.dataOut); unhandledError != nil {
			outPipes.errOut <- unhandledError
		}
	}
}

func (routineset *RoutineSet) validateRoutineOutputPipes(outPipes *outputPipes) error {
	if outPipes == nil {
		return &InitialiseError{NewGeneralError(routineset.name, fmt.Errorf("Wiring error: Routine %s didn't return pipes at all", routineset.name))}
	}
	if outPipes.dataOut == nil {
		return &InitialiseError{NewGeneralError(routineset.name, fmt.Errorf("Wiring error: Routine %s didn't return output pipe", routineset.name))}
	}
	return nil
}
