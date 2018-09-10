package abstractpipeline

import (
	"sync"
)

const numberOfTerminationMonitorRoutines int = 1

/* Start a pipeline destruction monitor and wire it to the terminate output pipe
   from the last routine.  i.e:
   		1) client sends signal on pipeline.TerminateIn to terminate the pipeline.
        2) termination signal is propogated through all routines
        3) final routine propogates the signal out back to the termination monitor
        4) Now it is safe for the termination monitor to close all pipeline
		   output channels before ultimately closing the consolidated output error
		   pipe to the client
*/
func (pipeline *Pipeline) startAndStitchTerminationMonitor(lastTerminateCallbackOut terminationRqRsChan, startWg *sync.WaitGroup) {

	pipeline.logStarted(terminationMonitorName, 1)
	startWg.Done()

	go func() {
		defer func() {
			pipeline.logTerminated(terminationMonitorName, 1)
			pipeline.logTerminated(pipelineName, 1)
		}()

		terminateSuccess := <-lastTerminateCallbackOut
		pipeline.logTerminateSignalReceived(terminationMonitorName, 1)
		close(pipeline.ErrorOutPipe)
		terminateSuccess <- terminationSignal{}
	}()
}
