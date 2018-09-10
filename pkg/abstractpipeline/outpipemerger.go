package abstractpipeline

import "sync"

type terminationRqRsChan chan chan struct{}

func (routineset *RoutineSet) isOutputPipeMergerRequired() bool {
	if routineset.numRoutines > 1 {
		return true
	}
	return false
}
func (routine *RoutineSet) mergeRoutineOutPipes(allSubRoutineOutPipes []*outputPipes) (mergedOutputPipes *outputPipes) {

	if !routine.isOutputPipeMergerRequired() {
		return allSubRoutineOutPipes[firstRoutineID]
	}

	mergedOutputPipes = &outputPipes{
		dataOut:              make(chan interface{}),
		terminateCallbackOut: make(terminationRqRsChan),
	}

	defer routine.cntl.startWaitGroup.Done()
	dataMergerTerminators := startDataPipeMergersAndGetTerminationChans(allSubRoutineOutPipes, mergedOutputPipes)
	// Just pass through data received from all subroutines to the consolidated output pipe

	// This could be more performant.  Wait to receive a termination callback from all subroutines
	// before killing all data mergers and propogating the termination callback to to the next routineset
	// and ultimately terminating the merger.
	go func() {
		var terminateCallback chan struct{}
		for _, outPipes := range allSubRoutineOutPipes {
			terminateCallback = <-outPipes.terminateCallbackOut
		}

		allDataMergersDeadWg := &sync.WaitGroup{}

		for _, terminator := range dataMergerTerminators {
			allDataMergersDeadWg.Add(1)
			go func(dataMergerTerminator terminationRqRsChan) {
				defer allDataMergersDeadWg.Done()
				respChan := make(chan struct{})
				dataMergerTerminator <- respChan
				<-respChan
			}(terminator)
		}

		allDataMergersDeadWg.Wait()

		mergedOutputPipes.terminateCallbackOut <- terminateCallback
		close(mergedOutputPipes.dataOut)
		close(mergedOutputPipes.terminateCallbackOut)

	}()

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
		var doneRespChan chan struct{}
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
