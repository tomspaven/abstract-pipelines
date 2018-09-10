package abstractpipeline

import "sync"

type terminationRqRsChan chan chan struct{}
type terminationResponseChan chan struct{}

func (routineset *RoutineSet) isOutputPipeMergerRequired() bool {
	if routineset.numRoutines > 1 {
		return true
	}
	return false
}
func (routineSet *RoutineSet) mergeRoutineOutPipes(allSubRoutineOutPipes []*outputPipes) (mergedOutputPipes *outputPipes) {

	if !routineSet.isOutputPipeMergerRequired() {
		return allSubRoutineOutPipes[firstRoutineID]
	}

	mergedOutputPipes = &outputPipes{
		dataOut:              make(chan interface{}),
		terminateCallbackOut: make(terminationRqRsChan),
	}

	defer routineSet.cntl.startWaitGroup.Done()
	dataMergerTerminators := startDataPipeMergersAndGetTerminationChans(allSubRoutineOutPipes, mergedOutputPipes)
	startTerminationSignalMerger(allSubRoutineOutPipes, mergedOutputPipes, dataMergerTerminators)
	return

}

func startDataPipeMergersAndGetTerminationChans(allSubRoutineOutPipes []*outputPipes, mergedOutputPipes *outputPipes) []terminationRqRsChan {
	var dataMergerTerminators []terminationRqRsChan
	for _, subRoutineDataOutPipe := range allSubRoutineOutPipes {
		currentTerminator := make(terminationRqRsChan)
		dataMergerTerminators = append(dataMergerTerminators, currentTerminator)
		startDataPipeMerger(subRoutineDataOutPipe, mergedOutputPipes, currentTerminator)
	}
	return dataMergerTerminators
}

func startDataPipeMerger(subRoutineDataOut, mergedOutputPipes *outputPipes, terminateChan terminationRqRsChan) {
	go func(subRoutineDataOut *outputPipes, terminateChan terminationRqRsChan) {
		var doneRespChan terminationResponseChan
		defer func() { doneRespChan <- struct{}{} }()

	routineLoop:
		for {
			select {
			case doneRespChan = <-terminateChan:
				break routineLoop
			default:
				for data := range subRoutineDataOut.dataOut {
					mergedOutputPipes.dataOut <- data
				}
			}
		}

	}(subRoutineDataOut, terminateChan)
}

func startTerminationSignalMerger(allSubRoutineOutPipes []*outputPipes, mergedOutputPipes *outputPipes, dataMergerTerminators []terminationRqRsChan) {
	// This could be more performant.  Wait to receive a termination callback from all subroutines
	// before killing all data mergers and propogating the termination callback to to the next routineset
	// and ultimately terminating the merger.
	go func() {
		terminateCallback := waitForAllRoutineTerminationSignals(allSubRoutineOutPipes)
		terminateDataPipeMergersAndWait(dataMergerTerminators)
		mergedOutputPipes.terminateCallbackOut <- terminateCallback
		close(mergedOutputPipes.dataOut)
		close(mergedOutputPipes.terminateCallbackOut)
	}()
}

func waitForAllRoutineTerminationSignals(allSubRoutineOutPipes []*outputPipes) terminationResponseChan {
	var terminateCallback terminationResponseChan
	for _, outPipes := range allSubRoutineOutPipes {
		terminateCallback = <-outPipes.terminateCallbackOut
	}
	return terminateCallback
}

func terminateDataPipeMergersAndWait(dataMergerTerminators []terminationRqRsChan) {
	allDataMergersDeadWg := &sync.WaitGroup{}

	for _, terminator := range dataMergerTerminators {
		allDataMergersDeadWg.Add(1)
		go func(dataMergerTerminator terminationRqRsChan) {
			defer allDataMergersDeadWg.Done()
			respChan := make(terminationResponseChan)
			dataMergerTerminator <- respChan
			<-respChan
		}(terminator)
	}

	allDataMergersDeadWg.Wait()
}
