package abstractpipeline

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
	for _, subRoutineDataOutPipe := range allSubRoutineOutPipes {
		go func(subRoutineDataOut *outputPipes) {
			for data := range subRoutineDataOut.dataOut {
				mergedOutputPipes.dataOut <- data
			}
		}(subRoutineDataOutPipe)
	}

	// This could be more performant.  Wait to receive a termination callback from all subroutines
	// before propogating to the next routine and terminating the merger.
	go func() {
		var terminateCallback chan struct{}
		for _, outPipes := range allSubRoutineOutPipes {
			terminateCallback = <-outPipes.terminateCallbackOut
		}

		mergedOutputPipes.terminateCallbackOut <- terminateCallback
		close(mergedOutputPipes.dataOut)
		close(mergedOutputPipes.terminateCallbackOut)

	}()

	return

}
