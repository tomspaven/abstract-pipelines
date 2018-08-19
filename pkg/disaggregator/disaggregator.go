package disaggregator

import "io"

type Disaggregator interface {
	Disaggregate(datainput io.Reader) (disaggregatedOutputPipe <-chan interface{})
}

type UnknownDisaggregatorError struct{}

func (e UnknownDisaggregatorError) Error() string {
	return "Unknown disaggregator"
}

type UnknownDisaggregator struct{}

func (b UnknownDisaggregator) Disaggregate(r io.Reader) <-chan interface{} {
	// Rather than sending back a nil channel in the event of an error
	// send back a single element buffered channel with the error queued up on it.
	errorPipe := make(chan interface{}, 1)
	errorPipe <- &UnknownDisaggregatorError{}
	return errorPipe
}
