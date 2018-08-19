package abstractpipeline

type PipelineProcessor interface {
	initialise() error
	terminate() error
	process() error
}

type PipelineRoutine struct {
	name string
	impl PipelineProcessor
	cntl RoutineController
}

func (routine *PipelineRoutine) RunAndGetPipe(inputDataChan <-chan interface{}) (<-chan interface{}, error) {
	outputDataPipe := make(chan interface{})
	if err := routine.impl.initialise(); err != nil {
		return nil, err
	}

	routine.cntl.StartWaitGroup.Done()
	go func() {
	routineLoop:
		for {
			select {

			case <-routine.cntl.TerminateChan:
				close(outputDataPipe)
				if err := routine.impl.terminate(); err != nil {
				}
				break routineLoop

			case <-inputDataChan:
				if err := routine.impl.process(); err != nil {
				}
			}
		}
	}()

	return outputDataPipe, nil

}
