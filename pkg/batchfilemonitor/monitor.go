package batchfilemonitor

import (
	pipe "abstract-pipelines/pkg/abstractpipeline"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
)

type Loggers struct {
	OutLog *log.Logger
	ErrLog *log.Logger
}

type Params struct {
	checkForFileIntervalSeconds time.Duration
	inputFileDirectory          string
	log                         Loggers
}

type Monitor struct {
	pipe.RoutineController
	Params
	Disaggregator
}

func New(params Params, controller pipe.RoutineController, disaggregator Disaggregator) *Monitor {
	return &Monitor{
		controller,
		params,
		disaggregator,
	}
}

func (monitor *Monitor) WaitForFilesAndDisaggregate() (recordsFromFilePipe chan<- interface{}) {

	fileCheckTriggerPipe := monitor.setupFileCheckTrigger()
	pipelineRoutine := pipe.PipelineRoutine{
		Name: "Input File Monitor and Disaggregator",
		Impl: monitor,
		Cntl: monitor.RoutineController,
	}

	return pipelineRoutine.RunAndGetOutputPipe(fileCheckTriggerPipe)

}

func (monitor *Monitor) setupFileCheckTrigger() <-chan interface{} {
	// Need to setup like this as ticker.C is a channel of a concrete type.
	// This essentially listens for and converts the ticker signal (sending on an abstract channel)
	// so it can be used with the pipeline framework
	fileCheckTriggerChan := make(chan interface{})
	checkForFileTicker := time.NewTicker(monitor.checkForFileIntervalSeconds * time.Second)
	go func() {
		for {
			time := <-checkForFileTicker.C
			fileCheckTriggerChan <- time
		}
	}()
	return fileCheckTriggerChan
}

func createErrorPipe(errorData error) (pipe chan<- interface{}) {
	errorPipe := make(chan interface{}, 1)
	errorPipe <- &WaitForFilesError{errorData}
	return errorPipe
}

type WaitForFilesError struct {
	previousError error
}

func (e *WaitForFilesError) Error() string {
	return fmt.Sprintf("Error waiting for files: %s", e.previousError.Error())
}

// Implement the PipelineProcessor interface so the monitor can be used as a pipeline processor
func (monitor *Monitor) Initialise() error { /* Nothing to Initialise */ return nil }
func (monitor *Monitor) Terminate() error  { /* Nothing to Clean up   */ return nil }
func (monitor *Monitor) Process(data interface{}, outputDataPipe chan<- interface{}) error {

	files, err := ioutil.ReadDir(monitor.Params.inputFileDirectory)
	if err != nil {
		return err
	}

	monitor.disaggregateAllFilesAndTransmit(files, outputDataPipe)
	return nil
}

func (monitor *Monitor) disaggregateAllFilesAndTransmit(files []os.FileInfo, outputDataPipe chan<- interface{}) {
	for _, file := range files {
		if !file.IsDir() {
			fullFilePath := fmt.Sprintf("%s%s%s", monitor.Params.inputFileDirectory, "/", file.Name())
			go monitor.disaggregateOneFileAndTransmit(fullFilePath, outputDataPipe)
		}
	}
	return
}

func (monitor *Monitor) disaggregateOneFileAndTransmit(fullFilePath string, outputDataPipe chan<- interface{}) {
	file, err := os.Open(fullFilePath)
	if err != nil {
		monitor.log.ErrLog.Println(fmt.Sprintf("Couldn't open file %s:%s"), fullFilePath, err.Error())
		return
	}
	defer file.Close()

	for record := range monitor.Disaggregator.Disaggregate(file) {
		outputDataPipe <- record
	}
}
