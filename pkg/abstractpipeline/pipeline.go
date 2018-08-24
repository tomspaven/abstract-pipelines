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

func New(sourceInPipe chan interface{}, loggers Loggers, routines ...*Routine) (*Pipeline, error) {

	pipeline := &Pipeline{
		terminateCallbackPipe: make(chan chan struct{}),
		ErrorOutPipe:          make(chan error),
		log:                   loggers,
	}

	pipeline.logHorizontalRule()
	pipeline.logStarting("Pipeline", 1)

	prepareRoutines(routines, loggers)
	if err := pipeline.stitchPipeline(sourceInPipe, routines); err != nil {
		return nil, err
	}

	pipeline.logStarted("Pipeline", 1)
	pipeline.logHorizontalRule()
	return pipeline, nil
}

func prepareRoutines(routines []*Routine, loggers Loggers) {
	wg := &sync.WaitGroup{}
	wg.Add(len(routines))
	wg.Add(numberOfTerminationMonitorRoutines)

	for routineID, routine := range routines {
		routine.id = routineID
		routine.cntl = routineController{
			startWaitGroup: wg,
			log:            loggers,
		}
	}
	return
}

const (
	firstRoutineID int = 0
)

func (pipeline *Pipeline) stitchPipeline(sourceInPipe chan interface{}, routines []*Routine) error {
	var nextOutPipes *outputPipes
	var err error

	// Gateway into the pipeline, and these will be the input feeds into the first routine
	sourceOutputPipes := &outputPipes{
		dataOut:              sourceInPipe,
		terminateCallbackOut: pipeline.terminateCallbackPipe,
	}

	for routineID, routine := range routines {
		if routineID == firstRoutineID {
			// Stitch first routine to the "gateway"
			if nextOutPipes, err = pipeline.startRoutineAndLinkToPipeline(routines[firstRoutineID], sourceOutputPipes); err != nil {
				return err
			}
			continue
		}
		// Stitch remaining routines to each other
		prevInPipes := nextOutPipes
		if nextOutPipes, err = pipeline.startRoutineAndLinkToPipeline(routine, prevInPipes); err != nil {
			return err
		}
	}

	lastOutPipes := nextOutPipes
	pipeline.SinkOutPipe = lastOutPipes.dataOut

	wg := routines[firstRoutineID].cntl.startWaitGroup
	pipeline.startAndStitchTerminationMonitor(lastOutPipes.terminateCallbackOut, wg)
	wg.Wait()

	return nil
}

func (pipeline *Pipeline) startRoutineAndLinkToPipeline(routine *Routine, prevRoutinesOutPipes *outputPipes) (newOutputPipes *outputPipes, err error) {
	inPipes := &inputPipes{
		dataIn:              prevRoutinesOutPipes.dataOut,
		terminateCallbackIn: prevRoutinesOutPipes.terminateCallbackOut,
	}
	if newOutputPipes, err = routine.startAndGetOutputPipes(inPipes); err != nil {
		return nil, err
	}
	if err = routine.validateRoutineOutputPipes(newOutputPipes); err != nil {
		return nil, err
	}

	pipeline.startErrorConsolidatorAndMerge(newOutputPipes.errOut, routine.id)
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
