package abstractpipeline_test

import (
	"abstract-pipelines/pkg/abstractpipeline"
	"bytes"
	"fmt"
	"reflect"
)

type MockLog struct {
	Out *bytes.Buffer
	Err *bytes.Buffer
}

const (
	PRINT_ROUTINE  int = 0
	APPEND_ROUTINE     = iota
	INIT_ERR_ROUTINE
	COUNTER_ROUTINE
	NO_IMPL_ROUTINE
)

var routineNameDictionary = map[int]string{
	PRINT_ROUTINE:    "Print",
	APPEND_ROUTINE:   "Append",
	INIT_ERR_ROUTINE: "InitError",
	COUNTER_ROUTINE:  "Counter",
	NO_IMPL_ROUTINE:  "NoImpl",
}

func createRoutineFactoryMethod(id int) *abstractpipeline.Routine {
	routine := &abstractpipeline.Routine{}
	switch id {
	case PRINT_ROUTINE:
		routine.Impl = &StringPrinter{}
	case APPEND_ROUTINE:
		routine.Impl = &StringAppender{}
	case INIT_ERR_ROUTINE:
		routine.Impl = &InitErrorer{}
	case COUNTER_ROUTINE:
		routine.Impl = &RecordCounter{}
	case NO_IMPL_ROUTINE:
		routine.Impl = nil
	default:
		routine.Impl = &InitErrorer{}
	}
	return routine
}

type StringPrinter struct{}

func (printer *StringPrinter) Initialise() error { return nil }
func (printer *StringPrinter) Terminate() error  { return nil }
func (printer *StringPrinter) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) {
	return
}
func (printer *StringPrinter) Process(data interface{}, outputPipe chan<- interface{}) error {
	stringData, ok := data.(string)
	if ok {
		fmt.Println(fmt.Sprintf("\t%s processed data %s", reflect.TypeOf(printer).Name(), stringData))
		outputPipe <- stringData
		return nil
	}

	generalError := abstractpipeline.NewGeneralError("StringPrinter", nil)
	err := abstractpipeline.NewTypeAssertionError(generalError, "string")

	return err
}

type StringAppender struct{}

func (appender *StringAppender) Initialise() error { return nil }
func (appender *StringAppender) Terminate() error  { return nil }
func (appender *StringAppender) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) {
	return
}
func (appender *StringAppender) Process(data interface{}, outputPipe chan<- interface{}) error {
	stringData, ok := data.(string)
	if ok {
		fmt.Println(fmt.Sprintf("\t\t%s PIPELINED data %s", reflect.TypeOf(appender).Name(), stringData))
		stringData = stringData + " PIPELINED!"
		outputPipe <- stringData
		return nil
	}

	generalError := abstractpipeline.NewGeneralError("StringAppender", nil)
	err := abstractpipeline.NewTypeAssertionError(generalError, "string")

	return err
}

type InitErrorer struct{}

func (errorer *InitErrorer) Initialise() error {
	generalError := abstractpipeline.NewGeneralError("InitErrorer", fmt.Errorf("I threwz an error on initialisation din't i?!"))
	return &abstractpipeline.InitialiseError{generalError}
}
func (errorer *InitErrorer) Terminate() error { return nil }
func (errorer *InitErrorer) Process(data interface{}, outputPipe chan<- interface{}) error {
	return nil
}
func (errorer *InitErrorer) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) {
	return
}

type RecordCounter struct {
	recordsProcessed int64
}

func (counter *RecordCounter) Initialise() error { return nil }
func (counter *RecordCounter) Terminate() error {
	fmt.Println(fmt.Sprintf("******************RECORDS PROCESSED: %d**********************", counter.recordsProcessed))
	return nil
}
func (counter *RecordCounter) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) {
	return
}
func (counter *RecordCounter) Process(data interface{}, outputPipe chan<- interface{}) error {
	counter.recordsProcessed++
	outputPipe <- data
	return nil
}
