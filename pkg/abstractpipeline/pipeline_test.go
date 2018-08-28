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

	wgs := waitGroups{
		start: &sync.WaitGroup{},
		end:   &sync.WaitGroup{},
	}

	wgs.start.Add(2)
	wgs.end.Add(2)
	drinkFromStringPipeAndAssert(pipeline, t, wgs)
	drinkFromErrorPipeAndAssertUnexpected(pipeline, t, wgs)
	wgs.start.Wait()
	checkEmptyErrorLogAndAssert(t)
	wgs.end.Wait()
}

const terminateTestLengthMilliseconds time.Duration = 1000

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

	wgs := waitGroups{
		start: &sync.WaitGroup{},
		end:   &sync.WaitGroup{},
	}

	wgs.start.Add(2)
	wgs.end.Add(2)
	drinkFromStringPipeAndAssert(pipeline, t, wgs)
	drinkFromErrorPipeAndAssertUnexpected(pipeline, t, wgs)
	wgs.start.Wait()

	terminatePipelineTicker := time.NewTicker(terminateTestLengthMilliseconds * time.Millisecond)
	<-terminatePipelineTicker.C

	var stopSuccess struct{}
	stopFunc := func() {
		stopSuccess = <-pipeline.Stop()
	}

	assert.NotPanicsf(t, stopFunc, "Paniced when stopping pipeline")
	assert.NotNilf(t, stopSuccess, "Nil returned from pipeline StopFunc channel")

	checkEmptyErrorLogAndAssert(t)
	wgs.end.Wait()
}

func TestNewWithInitError(t *testing.T) {
	var pipeline *abstractpipeline.Pipeline
	var rawErr error
	creator := func() {
		_, pipeline, rawErr = makePipeline(PRINT_ROUTINE, APPEND_ROUTINE, COUNTER_ROUTINE, INIT_ERR_ROUTINE)
	}
	assert.NotPanicsf(t, creator, "Paniced when creating pipeline")
	initialiseErr := assertOnInitialiseError(t, rawErr, pipeline)
	previousErrText := initialiseErr.GenErr.PreviousError.Error()
	assert.Containsf(t, previousErrText, "I threwz an error on initialisation din't i?", "Expected previous error to contain \"I threwz an error on initialisation din't i?\".  Previous Error text was %s", previousErrText)
	checkEmptyErrorLogAndAssert(t)
}

func TestRoutineWithNoImpl(t *testing.T) {
	var pipeline *abstractpipeline.Pipeline
	var rawErr error
	creator := func() {
		_, pipeline, rawErr = makePipeline(PRINT_ROUTINE, APPEND_ROUTINE, NO_IMPL_ROUTINE, COUNTER_ROUTINE)
	}
	assert.NotPanicsf(t, creator, "Paniced when creating pipeline")
	initialiseErr := assertOnInitialiseError(t, rawErr, pipeline)
	previousErrText := initialiseErr.GenErr.PreviousError.Error()
	assert.Containsf(t, previousErrText, "failed to initialise as it had no Impl assigned", "Expected previous error to contain \"failed to initialise as it had no Impl assigned\".  Previous Error text was %s", previousErrText)
	checkEmptyErrorLogAndAssert(t)
}

func TestWithRoutineThatJustDropsErrors(t *testing.T) {
	var err error
	var pipeline *abstractpipeline.Pipeline
	var pipelineIn chan<- interface{}
	var rawErr error
	creator := func() {
		pipelineIn, pipeline, rawErr = makePipeline(PRINT_ROUTINE, APPEND_ROUTINE, TEFLON_PROCESS_ERR_ROUTINE, COUNTER_ROUTINE)
	}

	assert.NotPanicsf(t, creator, "Paniced when creating pipeline")
	assert.Nilf(t, err, "Error returned when making pipeline")
	assert.NotNilf(t, pipeline, "Unexpected nil pipeline returned")

	recordsSentChan := make(chan int)
	go func() {
		recordsSent := 0
		for _, datastring := range []string{"potato", "banana", "pineapple", "arancini", "n'duja", "Goji Berry", "Chia Seed"} {
			pipelineIn <- datastring
			recordsSent++
		}
		_ = <-pipeline.Stop()
		recordsSentChan <- recordsSent

	}()

	wgs := waitGroups{
		start: &sync.WaitGroup{},
		end:   &sync.WaitGroup{},
	}

	wgs.start.Add(2)
	wgs.end.Add(2)

	drinkFromStringPipeAndAssertUnexpected(pipeline, t, wgs) // We don't want any data out - teflon just drops it and feeds to error pipe.
	errorsReceivedChan := drinkFromErrorPipeAndAssert(pipeline, t, wgs, func(rawOutErr error) {
		assert.IsTypef(t, &abstractpipeline.GeneralError{}, rawOutErr, "Unexpected error type returned by teflon processor routine, expecting %s, got %T", "GeneralError", rawOutErr)
		generalErr, ok := rawOutErr.(*abstractpipeline.GeneralError)
		assert.Equalf(t, ok, true, "Type assertion error on error returned from pipeline error out")
		assert.NotNilf(t, generalErr, "generalErr was nil - not expected")
		assert.NotNilf(t, generalErr.PreviousError, "Previous err was nil - not expected")
		assert.Contains(t, generalErr.PreviousError.Error(), "Nuffin to do wi' me mate...I just create errors...")
	})
	wgs.start.Wait()

	checkEmptyErrorLogAndAssert(t)
	wgs.end.Wait()

	recordsSent := <-recordsSentChan
	errorsReceived := <-errorsReceivedChan

	fmt.Println(fmt.Sprintf("Send %d records and got %d errors", recordsSent, errorsReceived))
	assert.Equalf(t, recordsSent, errorsReceived, "Didn't receive the same number of errors out as messages pushed into the pipeline. Pushed %d, got %d errors", recordsSent, errorsReceived)

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

type waitGroups struct {
	start *sync.WaitGroup
	end   *sync.WaitGroup
}

func drinkFromErrorPipeAndAssert(pipeline *abstractpipeline.Pipeline, t *testing.T, wgs waitGroups, errorAsserter func(rawOutErr error)) (numErrorsChan chan int) {
	numErrorsChan = make(chan int)
	numRead := 0
	wgs.start.Done()
	go func() {
		for rawErr := range pipeline.ErrorOutPipe {
			numRead++
			errorAsserter(rawErr)
		}

		assert.NotEqualf(t, numRead, 0, "Didn't get at least one error on the error pipe when errors were expected")
		wgs.end.Done()
		numErrorsChan <- numRead
	}()
	return numErrorsChan
}
func drinkFromErrorPipeAndAssertUnexpected(pipeline *abstractpipeline.Pipeline, t *testing.T, wgs waitGroups) {
	wgs.start.Done()
	go func() {
		for raw := range pipeline.ErrorOutPipe {
			err, ok := raw.(error)
			if err == nil {
				err = errors.New("NIL ERROR???")
			}
			assert.Equalf(t, true, ok, "Read SOMETHING unexpected from the error pipe but wasn't an error, type assert problem")
			assert.Equalf(t, true, true, "Got unexpected error %s from pipe", err.Error())
		}
		wgs.end.Done()
	}()
}

func drinkFromStringPipeAndAssertUnexpected(pipeline *abstractpipeline.Pipeline, t *testing.T, wgs waitGroups) {
	wgs.start.Done()
	go func() {
		for raw := range pipeline.SinkOutPipe {
			obtainedString, ok := raw.(string)
			assert.Equalf(t, true, ok, "type assertion failure on string pipe expected string, got %T", obtainedString)
			assert.FailNowf(t, "Data received on the output pipe when unexpected, %s", obtainedString)
		}
		wgs.end.Done()
	}()

}

func drinkFromStringPipeAndAssert(pipeline *abstractpipeline.Pipeline, t *testing.T, wgs waitGroups) {
	wgs.start.Done()
	go func() {
		for raw := range pipeline.SinkOutPipe {
			obtainedString, ok := raw.(string)
			//fmt.Println(fmt.Sprintf("\t\t\tand so, %s came out the other end", obtainedString))
			assert.Equalf(t, true, ok, "type assertion failure on string pipe expected string, got %T", obtainedString)

			checkString := "PIPELINED!"
			assert.Containsf(t, obtainedString, checkString, "Pipeline processed string %s didn't contain string %s", obtainedString, checkString)
		}
		wgs.end.Done()
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

func assertOnInitialiseError(t *testing.T, rawErr error, pipeline *abstractpipeline.Pipeline) *abstractpipeline.InitialiseError {
	assert.NotNilf(t, rawErr, "Unexpected Error not returned when making pipeline with a routine with a faulty initialise")
	assert.Nilf(t, pipeline, "Unexpected pipeline returned, should be nil")
	assert.IsTypef(t, &abstractpipeline.InitialiseError{}, rawErr, "Unexpected error type returned by faulty initialiser routine, expecting %s, got %T", "InitialiseError", rawErr)
	initialiseErr, ok := rawErr.(*abstractpipeline.InitialiseError)
	assert.Equalf(t, ok, true, "Type assertion error on error returned from pipeline")
	assert.NotNilf(t, initialiseErr.GenErr.PreviousError, "Previous err was nil - not expected")
	return initialiseErr
}
