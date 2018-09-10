package abstractpipeline

import (
	"fmt"
	"sync"
)

type RoutineSet struct {
	name        string
	impl        RoutineImpl
	numRoutines int
	id          int
	cntl        routineSetController
}

type routineSetController struct {
	startWaitGroup *sync.WaitGroup
	log            Loggers
}

func NewRoutineSet(routineSetName string, routineImpl RoutineImpl, numRoutines int) (*RoutineSet, error) {
	candidateRoutineSet := &RoutineSet{
		name:        routineSetName,
		impl:        routineImpl,
		numRoutines: numRoutines,
	}

	if err := candidateRoutineSet.validate(); err != nil {
		return nil, err
	}

	return candidateRoutineSet, nil
}

const unknownRoutineSetName string = "Unknown"

func (rSet *RoutineSet) validate() error {
	if rSet.name == "" {
		rSet.name = unknownRoutineSetName
	}

	if rSet.numRoutines < 1 {
		rSet.numRoutines = 1
	}

	if rSet.impl == nil {
		return &InitialiseError{NewGeneralError(rSet.name, fmt.Errorf("Routineset %s failed to initialise as it had no Impl assigned", rSet.name))}
	}

	return nil
}

// Spawn a routine, associated error consolidater for each required routine in the set.
// Also spawn a single terminate signal broadcaster and output pipe merger shared across all routines if more than
// one in the set. Stitch all channels together as per the design.
func (rSet *RoutineSet) startAllAndGetOutputPipes(inPipes *inputPipes, pipelineConsolidatedErrOutPipe errorPipe) (*outputPipes, error) {

	if err := rSet.validate(); err != nil {
		return nil, err
	}

	terminatePipesAllRoutines := rSet.startTerminationBroadcaster(inPipes.terminateCallbackIn)
	dataOutputPipesAllRoutines := make([]*outputPipes, rSet.numRoutines)

	for i := 0; i < rSet.numRoutines; i++ {
		routineIdx := i + 1
		currentRoutineInPipes := &inputPipes{
			inPipes.dataIn,
			terminatePipesAllRoutines[i],
		}

		var err error
		dataOutputPipesAllRoutines[i], err = rSet.spawnRoutine(currentRoutineInPipes, routineIdx)
		if err != nil {
			return nil, err
		}

		if err = rSet.spawnErrorConsolidatorAndMerge(routineIdx, dataOutputPipesAllRoutines[i].errOut, pipelineConsolidatedErrOutPipe); err != nil {
			return nil, err
		}
	}

	mergedOutPipes := rSet.mergeRoutineOutPipes(dataOutputPipesAllRoutines)
	return mergedOutPipes, nil
}

func (rSet *RoutineSet) spawnRoutine(inPipes *inputPipes, routineIdx int) (outPipes *outputPipes, err error) {
	routineID := fmt.Sprintf("%s %d/%d", rSet.name, routineIdx, rSet.numRoutines)
	routine := newRoutine(rSet.impl, routineID, rSet.cntl)
	return routine.startAndGetOutputPipes(inPipes)
}

const (
	mergeSourceIdx int = 0
	mergeDestIdx   int = 1
)

func (rSet *RoutineSet) spawnErrorConsolidatorAndMerge(routineIdx int, pipesToMerge ...chan error) error {
	consolidatorID := fmt.Sprintf("%s:%d", rSet.name, routineIdx)
	errorConsolidator := newRoutineSetErrorConsolidator(consolidatorID, rSet.cntl.log.OutLog)
	return errorConsolidator.startConsolidatorAndMerge(pipesToMerge[mergeSourceIdx], pipesToMerge[mergeDestIdx])
}

const (
	numBroadcasters int = 1
	numMergers      int = 1
)

func (rSet *RoutineSet) numSynchStartRoutines() int {
	if rSet.numRoutines == 1 {
		return 1
	}
	return rSet.numRoutines + numBroadcasters + numMergers
}

func (rSet *RoutineSet) validateOutputPipes(outPipes *outputPipes) error {
	if outPipes == nil {
		return &InitialiseError{NewGeneralError(rSet.name, fmt.Errorf("Wiring error: Routineset %s didn't return pipes at all", rSet.name))}
	}
	if outPipes.dataOut == nil {
		return &InitialiseError{NewGeneralError(rSet.name, fmt.Errorf("Wiring error: Routineset %s didn't return output pipe", rSet.name))}
	}
	return nil
}
