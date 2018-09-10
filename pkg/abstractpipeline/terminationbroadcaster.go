package abstractpipeline

import "fmt"

func (routine *RoutineSet) startTerminationBroadcaster(terminateCallbackIn terminationRqRsChan) (terminateCallbackOutPipes []terminationRqRsChan) {

	terminateCallbackOutPipes = make([]terminationRqRsChan, routine.numRoutines)

	for i := 0; i < routine.numRoutines; i++ {
		terminateCallbackOutPipes[i] = make(terminationRqRsChan)
	}

	if routine.numRoutines == 1 {
		terminateCallbackOutPipes[firstRoutineID] = terminateCallbackIn
		return
	}

	defer routine.cntl.startWaitGroup.Done()
	go func() {
		routine.logTerminateBroadcasterStarted()
		terminateCallback := <-terminateCallbackIn
		// Don't really need to make this asynchonous.  Just loop around the routine channels.
		for _, terminateCallbackOutPipe := range terminateCallbackOutPipes {
			terminateCallbackOutPipe <- terminateCallback
		}
		routine.logTerminateBroadcasterTerminated()
	}()

	return terminateCallbackOutPipes

}

func (routine *RoutineSet) logTerminateBroadcasterStarted() {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("Termination Broadcaster started for routine group %s to fanout termination signals to %d subroutines.", routine.name, routine.numRoutines))
}
func (routine *RoutineSet) logTerminateBroadcasterTerminated() {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("Termination Broadcaster terminated for routine group %s.", routine.name))
}
