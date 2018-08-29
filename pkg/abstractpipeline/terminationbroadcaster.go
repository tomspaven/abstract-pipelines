package abstractpipeline

import "fmt"

func (routine *Routine) startTerminationBroadcaster(terminateCallbackIn chan chan struct{}) (terminateCallbackOutPipes []chan chan struct{}) {

	terminateCallbackOutPipes = make([]chan chan struct{}, routine.numSubRoutines)

	for i := 0; i < routine.numSubRoutines; i++ {
		terminateCallbackOutPipes[i] = make(chan chan struct{})
	}

	if routine.numSubRoutines == 1 {
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

func (routine *Routine) logTerminateBroadcasterStarted() {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("Termination Broadcaster started for routine group %s to fanout termination signals to %d subroutines.", routine.name, routine.numSubRoutines))
}
func (routine *Routine) logTerminateBroadcasterTerminated() {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("Termination Broadcaster terminated for routine group %s.", routine.name))
}
