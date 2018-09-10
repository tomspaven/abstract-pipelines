package abstractpipeline

import "sync"

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
		terminateCallbackOut: make(chan chan struct{}),
	}

	defer routine.cntl.startWaitGroup.Done()

	// Just pass through data received from all subroutines to the consolidated output pipe
	var dataMergerTerminators []chan chan struct{}
	for _, subRoutineDataOutPipe := range allSubRoutineOutPipes {
		currentTerminator := make(chan chan struct{})
		dataMergerTerminators = append(dataMergerTerminators, currentTerminator)
		startDataMerger(subRoutineDataOutPipe, mergedOutputPipes, currentTerminator)
	}

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
			go func(dataMergerTerminator chan chan struct{}) {
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

func startDataMerger(subRoutineDataOut, mergedOutputPipes *outputPipes, terminateChan chan chan struct{}) {
	go func(subRoutineDataOut *outputPipes, terminateChan chan chan struct{}) {
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
