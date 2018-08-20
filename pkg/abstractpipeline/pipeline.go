package abstractpipeline

import "errors"

type Pipeline struct {
	OutputPipe      <-chan interface{}
	orderedRoutines []*Routine //  Needed to terminate routines in the correct order.
}

func New(inputPipe <-chan interface{}, routines ...*Routine) (*Pipeline, error) {

	outputPipe, err := wirePipeline(inputPipe, routines)
	if err != nil {
		return nil, err
	}

	pipeline := &Pipeline{
		OutputPipe:      outputPipe,
		orderedRoutines: routines,
	}
	return pipeline, nil
}

func wirePipeline(inputPipe <-chan interface{}, routines []*Routine) (<-chan interface{}, error) {

	var feedPipe <-chan interface{}
	for idx, routine := range routines {
		// Wire the first routine to the inputPipe
		if idx == 0 {
			feedPipe = routine.runAndGetOutputPipe(inputPipe)
			if feedPipe == nil {
				return nil, &InitialiseError{routine.Name, errors.New("Wiring input error")}
			}
			continue
		}
		// Chain remaining routines together.
		feedPipe = routine.runAndGetOutputPipe(feedPipe)
		if feedPipe == nil {
			return nil, &InitialiseError{routine.Name, errors.New("Wiring error")}
		}
	}
	return feedPipe, nil
}

func (pipeline *Pipeline) Stop() {
	for _, routine := range pipeline.orderedRoutines {
		routine.Cntl.TerminateChan <- struct{}{}
	}
}
