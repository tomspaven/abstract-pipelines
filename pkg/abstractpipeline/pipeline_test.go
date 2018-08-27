package abstractpipeline_test

import (
	"abstract-pipelines/pkg/abstractpipeline"
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var mockLog *MockLog

func TestNew(t *testing.T) {
	var err error

	creator := func() {
		_, _, err = makePipeline(PRINT_ROUTINE, APPEND_ROUTINE)
	}
	assert.NotPanicsf(t, creator, "Paniced when creating pipeline")
	assert.Nilf(t, err, "Error returned when making pipeline")
	checkEmptyErrorLogAndAssert(t)
}

func TestTerminate(t *testing.T) {
	var err error
	var pipeline *abstractpipeline.Pipeline

	creator := func() {
		_, pipeline, err = makePipeline(PRINT_ROUTINE, APPEND_ROUTINE)
	}
	assert.NotPanicsf(t, creator, "Paniced when creating pipeline")
	assert.Nilf(t, err, "Error returned when making pipeline")

	closer := func() {
		<-pipeline.Stop()
	}

	assert.NotPanicsf(t, closer, "Paniced when terminating pipeline")
	checkEmptyErrorLogAndAssert(t)
}

// Basic test
// Create a two step pipeline:
// Step 1: Prints input string
// Step 2: Appends "PIPELINED!" to the string
// Send a small set of strings to it without termination
func TestInputStingsStopWhenDone(t *testing.T) {
	var err error
	var pipeline *abstractpipeline.Pipeline
	var pipelineIn chan<- interface{}

	creator := func() {
		pipelineIn, pipeline, err = makePipeline(PRINT_ROUTINE, APPEND_ROUTINE, COUNTER_ROUTINE)
	}

	assert.NotPanicsf(t, creator, "Paniced when creating pipeline")
	assert.Nilf(t, err, "Error returned when making pipeline")

	go func() {
		for _, datastring := range []string{"potato", "banana", "pineapple", "arancini", "n'duja"} {
			pipelineIn <- datastring
		}
		<-pipeline.Stop()
	}()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	drinkFromStringPipeAndAssert(pipeline, t, wg)
	drinkFromErrorPipeAndAssert(pipeline, t, wg)
	wg.Wait()

	checkEmptyErrorLogAndAssert(t)

}

const terminateTestLengthMilliseconds time.Duration = 50

// Setup same basic pipeline but hammer it with an infinite barrage of random strings.
// Terminate the pipeline after 50 milliseconds in the same order it was constructed
// (i.e. printer THEN appender)
func TestNewAndStopAbruptly(t *testing.T) {

	var err error
	var pipeline *abstractpipeline.Pipeline
	var pipelineIn chan<- interface{}

	creator := func() {
		pipelineIn, pipeline, err = makePipeline(PRINT_ROUTINE, APPEND_ROUTINE, COUNTER_ROUTINE)
	}

	assert.NotPanicsf(t, creator, "Paniced when creating pipeline")
	assert.Nilf(t, err, "Error returned when making pipeline")

	go func() {
		for datastring := range generateInfiniteRandomStrings() {
			pipelineIn <- datastring
		}
	}()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	drinkFromStringPipeAndAssert(pipeline, t, wg)
	drinkFromErrorPipeAndAssert(pipeline, t, wg)

	terminatePipelineTicker := time.NewTicker(terminateTestLengthMilliseconds * time.Millisecond)
	<-terminatePipelineTicker.C

	var stopSuccess struct{}
	stopFunc := func() {
		stopSuccess = <-pipeline.Stop()
	}

	assert.NotPanicsf(t, stopFunc, "Paniced when stopping pipeline")
	assert.NotNilf(t, stopSuccess, "Nil returned from pipeline StopFunc channel")

	checkEmptyErrorLogAndAssert(t)
	wg.Wait()
}

func TestNewWithInitError(t *testing.T) {

	var err error
	var pipeline *abstractpipeline.Pipeline
	var rawErr error
	creator := func() {
		_, pipeline, rawErr = makePipeline(PRINT_ROUTINE, APPEND_ROUTINE, COUNTER_ROUTINE, INIT_ERR_ROUTINE)
	}

	assert.NotPanicsf(t, creator, "Paniced when creating pipeline")
	assert.NotNilf(t, rawErr, "Unexpected Error not returned when making pipeline with a routine with a faulty initialise")
	assert.Nilf(t, pipeline, "Unexpected pipeline returned, should be nil")
	assert.IsTypef(t, &abstractpipeline.InitialiseError{}, rawErr, "Unexpected error type returned by faulty initialiser routine, expecting %s, got %T", "InitialiseError", err)
	initialiseErr, ok := rawErr.(*abstractpipeline.InitialiseError)
	assert.Equalf(t, ok, true, "Type assertion error on error returned from pipeline")
	assert.NotNilf(t, initialiseErr.GenErr.PreviousError, "Previous err was nil - not expected")
	previousErrText := initialiseErr.GenErr.PreviousError.Error()
	assert.Containsf(t, previousErrText, "I threwz an error on initialisation din't i?", "Expected previous error to contain \"I threwz an error on initialisation din't i?\".  Previous Error text was %s", previousErrText)
}

func TestRoutineWithNoImpl(t *testing.T) {

	var err error
	var pipeline *abstractpipeline.Pipeline
	var rawErr error
	creator := func() {
		_, pipeline, rawErr = makePipeline(PRINT_ROUTINE, APPEND_ROUTINE, NO_IMPL_ROUTINE, COUNTER_ROUTINE)
	}
	assert.NotPanicsf(t, creator, "Paniced when creating pipeline")
	assert.NotNilf(t, rawErr, "Unexpected Error not returned when making pipeline with a routine with a faulty initialise")
	assert.Nilf(t, pipeline, "Unexpected pipeline returned, should be nil")
	assert.IsTypef(t, &abstractpipeline.InitialiseError{}, rawErr, "Unexpected error type returned by faulty initialiser routine, expecting %s, got %T", "InitialiseError", err)
}

func makePipeline(routineIDs ...int) (chan<- interface{}, *abstractpipeline.Pipeline, error) {
	routines := generatePipelineRoutines(routineIDs...)
	inputChan := make(chan interface{})
	pipeline, err := abstractpipeline.New(inputChan, createLoggers(), routines...)
	return inputChan, pipeline, err
}

func checkEmptyErrorLogAndAssert(t *testing.T) {
	bytes := mockLog.Err.Bytes()
	assert.Equalf(t, 0, len(bytes), "Expeted empty error log, but %s was there", string(bytes))
}

func checkExpectedErrorLogContentAndAssert(t *testing.T, expectedlogContent string) {
	bytes := mockLog.Err.Bytes()
	logString := string(bytes)
	assert.Containsf(t, logString, expectedlogContent, "Expeted %s in error log, but %s was there", expectedlogContent, logString)
}

func drinkFromStringPipeAndAssert(pipeline *abstractpipeline.Pipeline, t *testing.T, wg *sync.WaitGroup) {

	go func() {
		for raw := range pipeline.SinkOutPipe {
			obtainedString, ok := raw.(string)
			fmt.Println(fmt.Sprintf("\t\t\tand so, %s came out the other end", obtainedString))
			assert.Equalf(t, true, ok, "type assertion failure on string pipe expected string, got %T", obtainedString)

			checkString := "PIPELINED!"
			assert.Containsf(t, obtainedString, checkString, "Pipeline processed string %s didn't contain string %s", obtainedString, checkString)
		}
		wg.Done()
	}()

}

func drinkFromErrorPipeAndAssert(pipeline *abstractpipeline.Pipeline, t *testing.T, wg *sync.WaitGroup) {
	go func() {
		for raw := range pipeline.ErrorOutPipe {
			err, ok := raw.(error)
			assert.Equalf(t, true, ok, "Read SOMETHING unexpected from the error pipe but wasn't an error, type assert problem")
			if err == nil {
				err = errors.New("NIL ERROR???")
			}
			assert.Equalf(t, true, true, "Got unexpected error %s from pipe", err.Error())
		}
		wg.Done()
	}()
}

func generatePipelineRoutines(testRoutineIDs ...int) []*abstractpipeline.Routine {
	routines := make([]*abstractpipeline.Routine, len(testRoutineIDs))

	for i, id := range testRoutineIDs {
		routine := createRoutineFactoryMethod(id)
		routine.Name = routineNameDictionary[id]
		routines[i] = routine
	}

	return routines
}

func createLoggers() abstractpipeline.Loggers {
	mockLog = &MockLog{
		Out: &bytes.Buffer{},
		Err: &bytes.Buffer{},
	}

	logFlags := log.Ldate | log.Ltime | log.Lshortfile
	loggers := abstractpipeline.Loggers{
		OutLog: log.New(mockLog.Out, "Info:", logFlags),
		ErrLog: log.New(mockLog.Err, "Error:", logFlags),
		//OutLog: log.New(os.Stdout, "Info:", logFlags),
		//ErrLog: log.New(os.Stderr, "Error:", logFlags),
	}
	return loggers
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
