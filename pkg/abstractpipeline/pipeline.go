package abstractpipeline

import (
	"fmt"
	"log"
	"sync"
)

const (
	pipelineName           string = "Pipeline"
	errorConsolidatorName         = "Error Consolidator"
	terminationMonitorName        = "Termination Monitor"
)

type Pipeline struct {
	SinkOutPipe           chan interface{}
	ErrorOutPipe          chan error
	terminateCallbackPipe chan chan struct{}
	log                   Loggers
}

type Loggers struct {
	OutLog *log.Logger
	ErrLog *log.Logger
}

func New(sourceInPipe chan interface{}, loggers Loggers, routines ...*RoutineSet) (*Pipeline, error) {

	pipeline := &Pipeline{
		terminateCallbackPipe: make(chan chan struct{}),
		ErrorOutPipe:          make(chan error),
		log:                   loggers,
	}

	pipeline.logHorizontalRule()
	pipeline.logStarting(pipelineName, 1)

	prepareRoutines(routines, loggers)
	if err := pipeline.stitchPipeline(sourceInPipe, routines); err != nil {
		return nil, err
	}

	pipeline.logStarted(pipelineName, 1)
	pipeline.logHorizontalRule()
	return pipeline, nil
}

func prepareRoutines(routines []*RoutineSet, loggers Loggers) {
	wg := &sync.WaitGroup{}
	wg.Add(numberOfTerminationMonitorRoutines)

	for routineID, routine := range routines {
		wg.Add(routine.numSynchStartRoutines())
		routine.id = routineID
		routine.cntl = routineSetController{
			startWaitGroup: wg,
			log:            loggers,
		}
	}
	return
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

const (
	firstRoutineID int = 0
)

func (pipeline *Pipeline) stitchPipeline(sourceInPipe chan interface{}, routines []*RoutineSet) error {
	var nextOutPipes *outputPipes
	var err error

	// Gateway into the pipeline, and these will be the input feeds into the first routine
	sourceOutputPipes := &outputPipes{
		dataOut:              sourceInPipe,
		terminateCallbackOut: pipeline.terminateCallbackPipe,
	}

	prevInPipes := sourceOutputPipes
	for _, routine := range routines {
		if nextOutPipes, err = pipeline.startRoutineAndLinkToPipeline(routine, prevInPipes); err != nil {
			return err
		}
		prevInPipes = nextOutPipes

	}

	lastOutPipes := nextOutPipes
	pipeline.SinkOutPipe = lastOutPipes.dataOut

	wg := routines[firstRoutineID].cntl.startWaitGroup
	pipeline.startAndStitchTerminationMonitor(lastOutPipes.terminateCallbackOut, wg)
	wg.Wait()

	return nil
}

func (pipeline *Pipeline) startRoutineAndLinkToPipeline(routineset *RoutineSet, prevRoutinesOutPipes *outputPipes) (newOutputPipes *outputPipes, err error) {
	inPipes := &inputPipes{
		dataIn:              prevRoutinesOutPipes.dataOut,
		terminateCallbackIn: prevRoutinesOutPipes.terminateCallbackOut,
	}
	if newOutputPipes, err = routineset.startAllAndGetOutputPipes(inPipes, pipeline.ErrorOutPipe); err != nil {
		return nil, err
	}
	if err = routineset.validateRoutineOutputPipes(newOutputPipes); err != nil {
		return nil, err
	}

	return newOutputPipes, nil
}

func (pipeline *Pipeline) Stop() (success <-chan struct{}) {
	pipeline.logHorizontalRule()
	terminateSuccess := make(chan struct{})
	pipeline.terminateCallbackPipe <- terminateSuccess
	return terminateSuccess
}

func (pipeline *Pipeline) logTerminateSignalReceived(receiverName string, ID int) {
	pipeline.log.OutLog.Println(fmt.Sprintf("Terminate signal received for %s %d", receiverName, ID))
}

func (pipeline *Pipeline) logStarting(receiverName string, ID int) {
	pipeline.log.OutLog.Println(fmt.Sprintf("%s starting (ID %d)", receiverName, ID))
}

func (pipeline *Pipeline) logStarted(receiverName string, ID int) {
	pipeline.log.OutLog.Println(fmt.Sprintf("%s started (ID %d)", receiverName, ID))
}

func (pipeline *Pipeline) logTerminated(receiverName string, ID int) {
	pipeline.log.OutLog.Println(fmt.Sprintf("%s terminated (ID %d)", receiverName, ID))
}

func (pipeline *Pipeline) logHorizontalRule() {
	pipeline.log.OutLog.Println(fmt.Sprintf("==================================================="))
}
