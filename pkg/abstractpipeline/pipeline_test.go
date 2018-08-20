package abstractpipeline_test

import (
	abspipe "abstract-pipelines/pkg/abstractpipeline"
	"log"
	"os"
	"sync"
	"testing"
)

type MockPipeline struct{}

func (mockpipe *MockPipeline) Initialise() error {
	return nil
}
func (mockpipe *MockPipeline) Process(data interface{}, outputPipe chan<- interface{}) error {
	return nil
}
func (mockpipe *MockPipeline) Terminate() error {
	return nil
}

func TestRunAndGetPipe(t *testing.T) {

	uut := &abspipe.PipelineRoutine{
		Name: "MockPipeline",
		Impl: &MockPipeline{},
		Cntl: *setupRoutineController(),
	}

	triggerChan := make(chan interface{})
	_ = uut.RunAndGetOutputPipe(triggerChan)

}

func setupRoutineController() *abspipe.RoutineController {

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)

	terminateChan := make(chan bool)

	logFlags := log.Ldate | log.Ltime | log.Lshortfile
	loggers := abspipe.Loggers{
		OutLog: log.New(os.Stdout, "Info:", logFlags),
		ErrLog: log.New(os.Stderr, "Error:", logFlags),
	}

	return &abspipe.RoutineController{
		StartWaitGroup: waitGroup,
		TerminateChan:  terminateChan,
		Log:            loggers,
	}
}
