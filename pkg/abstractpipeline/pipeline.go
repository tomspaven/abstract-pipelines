package abstractpipeline

type Processor interface {
	Initialise() error
	Terminate() error
	Process(data interface{}, outputDataPipe chan<- interface{}) error
}

type Pipeline struct {
	OutputPipe      <-chan interface{}
	orderedRoutines []*Routine //  Needed to terminate routines in the correct order.
}

func New(inputPipe <-chan interface{}, routines ...*Routine) *Pipeline {

	pipeline := &Pipeline{
		OutputPipe:      wirePipeline(inputPipe, routines),
		orderedRoutines: routines,
	}

	return pipeline
}

func wirePipeline(inputPipe <-chan interface{}, routines []*Routine) <-chan interface{} {

	var feedPipe <-chan interface{}
	for idx, routine := range routines {
		// Wire the first routine to the inputPipe
		if idx == 0 {
			feedPipe = routine.runAndGetOutputPipe(inputPipe)
			continue
		}
		feedPipe = routine.runAndGetOutputPipe(feedPipe)
	}
	return feedPipe
}

func (pipeline *Pipeline) Stop() {
	for _, routine := range pipeline.orderedRoutines {
		routine.Cntl.TerminateChan <- struct{}{}
	}
}
