package abstractpipeline_test

import (
	abspipe "abstract-pipelines/pkg/abstractpipeline"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

type StringPrinter struct {
	out io.Writer
}

func (mockpipe *StringPrinter) Initialise() error { return nil }
func (mockpipe *StringPrinter) Terminate() error  { return nil }
func (mockpipe *StringPrinter) Process(data interface{}, outputPipe chan<- interface{}) error {
	if stringData, ok := data.(string); ok {
		//fmt.Println(fmt.Sprintf("\t%s processed data %s", reflect.TypeOf(mockpipe).Name(), stringData))
		outputPipe <- stringData
		return nil
	}
	return fmt.Errorf("type assertion failue on data")
}

type StringAppender struct{}

func (mockpipe *StringAppender) Initialise() error { return nil }
func (mockpipe *StringAppender) Terminate() error  { return nil }
func (mockpipe *StringAppender) Process(data interface{}, outputPipe chan<- interface{}) error {
	if stringData, ok := data.(string); ok {
		stringData = stringData + " PIPELINED!"
		outputPipe <- stringData
		return nil
	}
	return fmt.Errorf("type assertion failue on data")
}

// Basic test
// Create a two step pipeline:
// Step 1: Prints input string
// Step 2: Appends "PIPELINED!" to the string
// Send a small set of strings to it without termination
func TestRunAndGetPipe(t *testing.T) {
	inputChan, _ := runPrintAndAppendPipeline()
	for _, datastring := range []string{"potato", "banana", "pineapple", "arancini", "n'duja"} {
		inputChan <- datastring
	}
}

const terminateTestLengthMilliseconds time.Duration = 50

// Setup same basic pipeline
// Terminate the pipeline after 50 milliseconds in the same order it was constructed
// (i.e. printer THEN appender)
func TestRunAndGetPipeWithForwardTerminate(t *testing.T) {

	inputChan, routineControllers := runPrintAndAppendPipeline()
	go func() {
		for datastring := range generateInifiteRandomStrings() {
			inputChan <- datastring
		}
	}()

	terminatePipelineTicker := time.NewTicker(terminateTestLengthMilliseconds * time.Millisecond)
	<-terminatePipelineTicker.C

	// Terminate print routine then append routine
	for _, controller := range routineControllers {
		controller.TerminateChan <- struct{}{}
	}
}

// Setup same basic pipeline
// Terminate the pipeline after 50 milliseconds in the reverse order to how it was constructed
// (i.e. appender THEN printer)
func TestRunAndGetPipeWithBackwardTerminate(t *testing.T) {

	inputChan, routineControllers := runPrintAndAppendPipeline()
	go func() {
		for datastring := range generateInifiteRandomStrings() {
			inputChan <- datastring
		}
	}()

	terminatePipelineTicker := time.NewTicker(terminateTestLengthMilliseconds * time.Millisecond)
	<-terminatePipelineTicker.C

	// Terminate append routine then print routine
	routineControllers[APPEND_ROUTINE_IDX].TerminateChan <- struct{}{}
	routineControllers[PRINT_ROUTINE_IDX].TerminateChan <- struct{}{}
}

const (
	PRINT_ROUTINE_IDX  int = 0
	APPEND_ROUTINE_IDX     = iota
)

func runPrintAndAppendPipeline() (chan<- interface{}, []*abspipe.RoutineController) {
	routines := generatePrintAndAppendTestRoutines()
	inputChan := make(chan interface{})
	go func() {
		pipelineOutputChannel := routines[APPEND_ROUTINE_IDX].RunAndGetOutputPipe(
			routines[PRINT_ROUTINE_IDX].RunAndGetOutputPipe(
				inputChan))

		for raw := range pipelineOutputChannel {
			if _, ok := raw.(string); ok {
				//fmt.Println(fmt.Sprintf("\t\tGot output %s from pipeline", outputString))
			}
		}
	}()
	return inputChan, []*abspipe.RoutineController{
		&routines[PRINT_ROUTINE_IDX].Cntl,
		&routines[APPEND_ROUTINE_IDX].Cntl,
	}
}

func generatePrintAndAppendTestRoutines() []*abspipe.Routine {
	pipelineControllers := setupRoutineControllers(2)

	printRoutine := &abspipe.Routine{
		Name: "MockPipeline 1",
		Impl: &StringPrinter{},
		Cntl: *pipelineControllers[PRINT_ROUTINE_IDX],
	}

	appendRoutine := &abspipe.Routine{
		Name: "MockPipeline 2",
		Impl: &StringAppender{},
		Cntl: *pipelineControllers[APPEND_ROUTINE_IDX],
	}

	return []*abspipe.Routine{printRoutine, appendRoutine}
}

func setupRoutineControllers(numberOfRoutines int) []*abspipe.RoutineController {

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(numberOfRoutines)

	logFlags := log.Ldate | log.Ltime | log.Lshortfile
	loggers := abspipe.Loggers{
		OutLog: log.New(os.Stdout, "Info:", logFlags),
		ErrLog: log.New(os.Stderr, "Error:", logFlags),
	}

	controllers := make([]*abspipe.RoutineController, numberOfRoutines)
	for i := 0; i < len(controllers); i++ {
		controllers[i] = &abspipe.RoutineController{
			StartWaitGroup: waitGroup,
			TerminateChan:  make(chan struct{}),
			Log:            loggers,
		}
	}
	return controllers
}

func generateInifiteRandomStrings() <-chan string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	randomStringChan := make(chan string)
	go func() {
		for {
			b := make([]byte, 8)
			for i := range b {
				b[i] = letterBytes[rand.Intn(len(letterBytes))]
			}
			randomStringChan <- string(b)
		}
	}()
	return randomStringChan
}
