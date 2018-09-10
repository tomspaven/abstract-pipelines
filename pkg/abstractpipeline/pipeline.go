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

type terminationRqRsChan chan chan struct{}
type terminationResponseChan chan struct{}
type terminationSignal struct{}

type dataPipe chan interface{}
type errorPipe chan error

type Pipeline struct {
	SinkOutPipe           dataPipe
	ErrorOutPipe          errorPipe
	terminateCallbackPipe terminationRqRsChan
	log                   Loggers
}

type Loggers struct {
	OutLog *log.Logger
	ErrLog *log.Logger
}

func New(sourceInPipe chan interface{}, loggers Loggers, routineSets ...*RoutineSet) (*Pipeline, error) {

	pipeline := &Pipeline{
		terminateCallbackPipe: make(terminationRqRsChan),
		ErrorOutPipe:          make(errorPipe),
		log:                   loggers,
	}

	pipeline.logHorizontalRule()
	pipeline.logStarting(pipelineName, 1)

	prepareRoutineSets(routineSets, loggers)
	if err := pipeline.stitchPipeline(sourceInPipe, routineSets); err != nil {
		return nil, err
	}

	pipeline.logStarted(pipelineName, 1)
	pipeline.logHorizontalRule()
	return pipeline, nil
}

func prepareRoutineSets(routineSets []*RoutineSet, loggers Loggers) {
	wg := &sync.WaitGroup{}
	wg.Add(numberOfTerminationMonitorRoutines)

	for routineSetID, routineSet := range routineSets {
		wg.Add(routineSet.numSynchStartRoutines())
		routineSet.id = routineSetID
		routineSet.cntl = routineSetController{
			startWaitGroup: wg,
			log:            loggers,
		}
	}
	return
}

type outputPipes struct {
	dataOut              dataPipe
	terminateCallbackOut terminationRqRsChan
	errOut               errorPipe
}

type inputPipes struct {
	dataIn              dataPipe
	terminateCallbackIn terminationRqRsChan
}

const (
	firstRoutineID int = 0
)

func (pipeline *Pipeline) stitchPipeline(sourceInPipe dataPipe, routineSets []*RoutineSet) error {
	var nextOutPipes *outputPipes
	var err error

	// Gateway into the pipeline, and these will be the input feeds into the first routine
	sourceOutputPipes := &outputPipes{
		dataOut:              sourceInPipe,
		terminateCallbackOut: pipeline.terminateCallbackPipe,
	}

	prevInPipes := sourceOutputPipes
	for _, routineSet := range routineSets {
		if nextOutPipes, err = pipeline.startRoutineSetAndLinkToPipeline(routineSet, prevInPipes); err != nil {
			return err
		}
		prevInPipes = nextOutPipes

	}

	lastOutPipes := nextOutPipes
	pipeline.SinkOutPipe = lastOutPipes.dataOut

	wg := routineSets[firstRoutineID].cntl.startWaitGroup
	pipeline.startAndStitchTerminationMonitor(lastOutPipes.terminateCallbackOut, wg)
	wg.Wait()

	return nil
}

func (pipeline *Pipeline) startRoutineSetAndLinkToPipeline(routineset *RoutineSet, prevRoutinesOutPipes *outputPipes) (newOutputPipes *outputPipes, err error) {
	inPipes := &inputPipes{
		dataIn:              prevRoutinesOutPipes.dataOut,
		terminateCallbackIn: prevRoutinesOutPipes.terminateCallbackOut,
	}
	if newOutputPipes, err = routineset.startAllAndGetOutputPipes(inPipes, pipeline.ErrorOutPipe); err != nil {
		return nil, err
	}
	if err = routineset.validateOutputPipes(newOutputPipes); err != nil {
		return nil, err
	}

	return newOutputPipes, nil
}

func (pipeline *Pipeline) Stop() (success <-chan struct{}) {
	pipeline.logHorizontalRule()
	terminateSuccess := make(terminationResponseChan)
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
