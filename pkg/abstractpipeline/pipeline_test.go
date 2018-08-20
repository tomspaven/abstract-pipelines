package abstractpipeline_test

import (
	abspipe "abstract-pipelines/pkg/abstractpipeline"
	"bytes"
	"fmt"
	"log"
	"math/rand"
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

type InitErrorer struct{}

func (mockpipe *InitErrorer) Initialise() error {
	return fmt.Errorf("I threwz an error on initialisation din't i?!")
}
func (mockpipe *InitErrorer) Terminate() error { return nil }
func (mockpipe *InitErrorer) Process(data interface{}, outputPipe chan<- interface{}) error {
	return nil
}

const (
	PRINT_ROUTINE  int = 0
	APPEND_ROUTINE     = iota
	INIT_ERR_ROUTINE
)

var routineNameDictionary = map[int]string{
	PRINT_ROUTINE:    "Print",
	APPEND_ROUTINE:   "Append",
	INIT_ERR_ROUTINE: "InitError",
}

type MockLog struct {
	Out *bytes.Buffer
	Err *bytes.Buffer
}

var mockLog *MockLog

// Basic test
// Create a two step pipeline:
// Step 1: Prints input string
// Step 2: Appends "PIPELINED!" to the string
// Send a small set of strings to it without termination
func TestNewAndStopWhenDone(t *testing.T) {
	inputChan, pipeline, err := makePipeline(PRINT_ROUTINE, APPEND_ROUTINE)

	assert.Nilf(t, err, "Error returned when making pipeline")
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

	inputChan, pipeline, err := makePipeline(PRINT_ROUTINE, APPEND_ROUTINE)
	assert.Nilf(t, err, "Unexpected Error returned when making pipeline")

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

func TestNewWithInitError(t *testing.T) {
	_, pipeline, err := makePipeline(PRINT_ROUTINE, INIT_ERR_ROUTINE)
	assert.NotNilf(t, err, "Unexpected Error returned when making pipeline")
	assert.Nilf(t, pipeline, "Unpexetced pipeline returned, should be nil")
}

func makePipeline(routineIDs ...int) (chan<- interface{}, *abspipe.Pipeline, error) {
	routines := generatePipelineRoutines(routineIDs...)
	inputChan := make(chan interface{})
	pipeline, err := abspipe.New(inputChan, routines...)
	return inputChan, pipeline, err
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

func generatePipelineRoutines(testRoutineIDs ...int) []*abspipe.Routine {
	pipelineControllers := setupRoutineControllers(len(testRoutineIDs))
	routines := make([]*abspipe.Routine, len(testRoutineIDs))

	for i, id := range testRoutineIDs {
		routine := createRoutineFactoryMethod(id)
		routine.Name = routineNameDictionary[id]
		routine.Cntl = *pipelineControllers[i]
		routines[i] = routine
	}

	return routines
}

func createRoutineFactoryMethod(id int) *abspipe.Routine {
	routine := &abspipe.Routine{}
	switch id {
	case PRINT_ROUTINE:
		routine.Impl = &StringPrinter{}
	case APPEND_ROUTINE:
		routine.Impl = &StringAppender{}
	case INIT_ERR_ROUTINE:
		routine.Impl = &InitErrorer{}
	default:
		routine.Impl = &InitErrorer{}
	}
	return routine
}

func setupRoutineControllers(numberOfRoutines int) []*abspipe.RoutineController {

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(numberOfRoutines)

	mockLog = &MockLog{
		Out: &bytes.Buffer{},
		Err: &bytes.Buffer{},
	}

	logFlags := log.Ldate | log.Ltime | log.Lshortfile
	loggers := abspipe.Loggers{
		OutLog: log.New(mockLog.Out, "Info:", logFlags),
		ErrLog: log.New(mockLog.Err, "Error:", logFlags),
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
