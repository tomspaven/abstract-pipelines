package abstractpipeline_test

import (
	abspipe "abstract-pipelines/pkg/abstractpipeline"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type StringPrinter struct{}

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
		//fmt.Println(fmt.Sprintf("\t\t%s PIPELINED data %s", reflect.TypeOf(mockpipe).Name(), stringData))
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
func TestNewAndStopWhenDone(t *testing.T) {
	inputChan, pipeline := runPrintAndAppendPipeline()

	go func() {
		for _, datastring := range []string{"potato", "banana", "pineapple", "arancini", "n'duja"} {
			inputChan <- datastring
		}
		pipeline.Stop()
	}()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	drinkFromStringPipeAndAssert(pipeline, t, wg)
	wg.Wait()
}

const terminateTestLengthMilliseconds time.Duration = 50

// Setup same basic pipeline but hammer it with an infinite barrage of random strings.
// Terminate the pipeline after 50 milliseconds in the same order it was constructed
// (i.e. printer THEN appender)
func TestNewAndStopAbruptly(t *testing.T) {

	inputChan, pipeline := runPrintAndAppendPipeline()
	go func() {
		for datastring := range generateInfiniteRandomStrings() {
			inputChan <- datastring
		}
	}()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	drinkFromStringPipeAndAssert(pipeline, t, wg)

	terminatePipelineTicker := time.NewTicker(terminateTestLengthMilliseconds * time.Millisecond)
	<-terminatePipelineTicker.C

	pipeline.Stop()
	wg.Wait()
}

const (
	PRINT_ROUTINE_IDX  int = 0
	APPEND_ROUTINE_IDX     = iota
)

func runPrintAndAppendPipeline() (chan<- interface{}, *abspipe.Pipeline) {
	routines := generatePrintAndAppendTestRoutines()
	inputChan := make(chan interface{})
	pipeline := abspipe.New(inputChan, routines[PRINT_ROUTINE_IDX], routines[APPEND_ROUTINE_IDX])
	return inputChan, pipeline
}

func drinkFromStringPipeAndAssert(pipeline *abspipe.Pipeline, t *testing.T, wg *sync.WaitGroup) {

	go func() {
		for raw := range pipeline.OutputPipe {
			obtainedString, ok := raw.(string)
			assert.Equalf(t, true, ok, "type assertion failure on string pipe expected string")

			checkString := "PIPELINED!"
			assert.Containsf(t, obtainedString, checkString, "Pipeline processed string %s didn't contain string %s", obtainedString, checkString)
		}
		wg.Done()
	}()
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

func generateInfiniteRandomStrings() <-chan string {
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
