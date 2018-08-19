package abstractpipeline

type PipelineProcessor interface {
	Initialise() error
	Terminate() error
	Process() error
}

type PipelineRoutine struct {
	Name string
	Impl PipelineProcessor
	Cntl RoutineController
}

func (routine *PipelineRoutine) RunAndGetPipe(inputDataChan <-chan interface{}) (<-chan interface{}, error) {

	outputDataPipe := make(chan interface{})
	if err := routine.Impl.Initialise(); err != nil {
		return nil, err
	}

	routine.Cntl.StartWaitGroup.Done()
	go func() {
	routineLoop:
		for {
			select {

			case <-routine.Cntl.TerminateChan:
				close(outputDataPipe)
				if err := routine.Impl.Terminate(); err != nil {
				}
				break routineLoop

			case <-inputDataChan:
				if err := routine.Impl.Process(); err != nil {
				}
			}
		}
	}()

	return outputDataPipe, nil

}
