package abstractpipeline_test

import (
	"abstract-pipelines/pkg/abstractpipeline"
	"bytes"
	"fmt"
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
	TEFLON_PROCESS_ERR_ROUTINE
	HANDLED_PROCESS_ERR_ROUTINE
)

var routineNameDictionary = map[int]string{
	PRINT_ROUTINE:               "Print",
	APPEND_ROUTINE:              "Append",
	INIT_ERR_ROUTINE:            "InitError",
	COUNTER_ROUTINE:             "Counter",
	NO_IMPL_ROUTINE:             "NoImpl",
	TEFLON_PROCESS_ERR_ROUTINE:  "Teflon",
	HANDLED_PROCESS_ERR_ROUTINE: "Handled",
}

func createRoutineFactoryMethod(id int, errorAsserter func(err error)) *abstractpipeline.RoutineSet {
	var err error
	routine := &abstractpipeline.RoutineSet{}
	switch id {
	case PRINT_ROUTINE:
		routine, err = abstractpipeline.NewRoutineSet(routineNameDictionary[id], &StringPrinter{}, 1)
	case APPEND_ROUTINE:
		routine, err = abstractpipeline.NewRoutineSet(routineNameDictionary[id], &StringAppender{}, 1)
	case INIT_ERR_ROUTINE:
		routine, err = abstractpipeline.NewRoutineSet(routineNameDictionary[id], &InitErrorer{}, 1)
	case COUNTER_ROUTINE:
		routine, err = abstractpipeline.NewRoutineSet(routineNameDictionary[id], &RecordCounter{}, 1)
	case NO_IMPL_ROUTINE:
		routine, err = abstractpipeline.NewRoutineSet(routineNameDictionary[id], nil, 1)
	case TEFLON_PROCESS_ERR_ROUTINE:
		routine, err = abstractpipeline.NewRoutineSet(routineNameDictionary[id], &TeflonProcessErrorer{}, 1)
	case HANDLED_PROCESS_ERR_ROUTINE:
		routine, err = abstractpipeline.NewRoutineSet(routineNameDictionary[id], &HandledProcessErrorer{}, 1)
	default:
		routine, err = abstractpipeline.NewRoutineSet(routineNameDictionary[id], &InitErrorer{}, 1)
	}
	errorAsserter(err)
	return routine
}

type StringPrinter struct{}

func (printer *StringPrinter) Initialise() error { return nil }
func (printer *StringPrinter) Terminate() error  { return nil }
func (printer *StringPrinter) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) error {
	return err
}
func (printer *StringPrinter) Process(data interface{}, outputPipe chan<- interface{}) error {
	stringData, ok := data.(string)
	if ok {
		//fmt.Println(fmt.Sprintf("\t%s processed data %s", reflect.TypeOf(printer).Name(), stringData))
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
func (appender *StringAppender) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) error {
	return err
}
func (appender *StringAppender) Process(data interface{}, outputPipe chan<- interface{}) error {
	stringData, ok := data.(string)
	if ok {
		//fmt.Println(fmt.Sprintf("\t\t%s PIPELINED data %s", reflect.TypeOf(appender).Name(), stringData))
		stringData = stringData + " PIPELINED!"
		outputPipe <- stringData
		return nil
	}

	generalError := abstractpipeline.NewGeneralError("StringAppender", nil)
	err := abstractpipeline.NewTypeAssertionError(generalError, "string")

	return err
}

type RecordCounter struct {
	recordsProcessed int64
}

func (counter *RecordCounter) Initialise() error { return nil }
func (counter *RecordCounter) Terminate() error {
	fmt.Println(fmt.Sprintf("******************RECORDS PROCESSED: %d**********************", counter.recordsProcessed))
	return nil
}
func (counter *RecordCounter) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) error {
	return err
}
func (counter *RecordCounter) Process(data interface{}, outputPipe chan<- interface{}) error {
	counter.recordsProcessed++
	outputPipe <- data
	return nil
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
func (errorer *InitErrorer) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) error {
	return err
}

type TeflonProcessErrorer struct{}

func (errorer *TeflonProcessErrorer) Initialise() error { return nil }
func (errorer *TeflonProcessErrorer) Terminate() error  { return nil }
func (errorer *TeflonProcessErrorer) Process(data interface{}, outputPipe chan<- interface{}) error {
	return abstractpipeline.NewGeneralError("TeflonErrorer", fmt.Errorf("Nuffin to do wi' me mate...I just create errors..."))
}
func (errorer *TeflonProcessErrorer) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) error {
	return err
}

type HandledProcessErrorer struct{}

func (errorer *HandledProcessErrorer) Initialise() error { return nil }
func (errorer *HandledProcessErrorer) Terminate() error  { return nil }
func (errorer *HandledProcessErrorer) Process(data interface{}, outputPipe chan<- interface{}) error {
	return abstractpipeline.NewGeneralError("HandledProcessErrorer", fmt.Errorf("I create an error when processing, but I know how to bloody well deal with it - pass it down the pipeline and don't bother the client!"))
}
func (errorer *HandledProcessErrorer) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) error {
	stringData, ok := data.(string)
	if !ok {
		return abstractpipeline.NewTypeAssertionError(abstractpipeline.NewGeneralError("HandledProcessErrorer", err), "string")
	}
	outDataPipe <- stringData + " HANDLED"
	return nil
}
