package abstractpipeline

import (
	"fmt"
	"log"
)

type consolidator interface {
	startConsolidatorAndMerge(singleOutPipe, consolidatedOutpipe interface{}) error
}

type routineSetErrorConsolidator struct {
	id  string
	out *log.Logger
}

func newRoutineSetErrorConsolidator(id string, out *log.Logger) consolidator {
	return &routineSetErrorConsolidator{id, out}
}

func (consolidator *routineSetErrorConsolidator) startConsolidatorAndMerge(out, consolidatedOut interface{}) error {

	chanErrPipes, err := consolidator.typeAssertInPipes(out, consolidatedOut)
	if err != nil {
		return err
	}

	const (
		routineErrorOutputPipeIdx int = 0
		consolidatedOutputPipeIdx     = iota
	)

	go func() {
		consolidator.logErrorConsolidatorStarted()
		defer consolidator.logErrorConsolidatorTerminated()
		for errorFromRoutine := range chanErrPipes[routineErrorOutputPipeIdx] {
			chanErrPipes[consolidatedOutputPipeIdx] <- errorFromRoutine
		}
	}()
	return nil
}

func (consolidator *routineSetErrorConsolidator) typeAssertInPipes(inputPipes ...interface{}) (pipes []errorPipe, err error) {

	for i, rawPipe := range inputPipes {
		if chanErrPipe, ok := rawPipe.(chan error); ok {
			pipes = append(pipes, chanErrPipe)
			continue
		}
		errGenerator := fmt.Sprintf("Error Consolidator %s:%d", consolidator.id, i)
		return nil, NewTypeAssertionError(NewGeneralError(errGenerator, fmt.Errorf("")), "chan error")
	}
	return
}

func (consolidator *routineSetErrorConsolidator) logErrorConsolidatorStarted() {
	consolidator.out.Println(fmt.Sprintf("Error consolidator started for routine/subroutine %s.", consolidator.id))
}

func (consolidator *routineSetErrorConsolidator) logErrorConsolidatorTerminated() {
	consolidator.out.Println(fmt.Sprintf("Error consolidator terminated for routine/subroutine %s.", consolidator.id))
}
