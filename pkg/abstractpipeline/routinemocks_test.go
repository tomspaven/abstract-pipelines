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

type StringPrinter struct{}

func (mockpipe *StringPrinter) Initialise() error { return nil }
func (mockpipe *StringPrinter) Terminate() error  { return nil }
func (mockpipe *StringPrinter) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) {
	return
}
func (mockpipe *StringPrinter) Process(data interface{}, outputPipe chan<- interface{}) error {
	stringData, ok := data.(string)
	if ok {
		//fmt.Println(fmt.Sprintf("\t%s processed data %s", reflect.TypeOf(mockpipe).Name(), stringData))
		outputPipe <- stringData
		return nil
	}

	generalError := abstractpipeline.NewGeneralError("StringPrinter", nil)
	err := abstractpipeline.NewTypeAssertionError(generalError, "string")

	return err
}

type StringAppender struct{}

func (mockpipe *StringAppender) Initialise() error { return nil }
func (mockpipe *StringAppender) Terminate() error  { return nil }
func (mockpipe *StringAppender) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) {
	return
}
func (mockpipe *StringAppender) Process(data interface{}, outputPipe chan<- interface{}) error {
	stringData, ok := data.(string)
	if ok {
		//fmt.Println(fmt.Sprintf("\t\t%s PIPELINED data %s", reflect.TypeOf(mockpipe).Name(), stringData))
		stringData = stringData + " PIPELINED!"
		outputPipe <- stringData
		return nil
	}

	generalError := abstractpipeline.NewGeneralError("StringAppender", nil)
	err := abstractpipeline.NewTypeAssertionError(generalError, "string")

	return err
}

type InitErrorer struct{}

func (mockpipe *InitErrorer) Initialise() error {
	generalError := abstractpipeline.NewGeneralError("InitErrorer", fmt.Errorf("I threwz an error on initialisation din't i?!"))
	return &abstractpipeline.InitialiseError{generalError}
}
func (mockpipe *InitErrorer) Terminate() error { return nil }
func (mockpipe *InitErrorer) Process(data interface{}, outputPipe chan<- interface{}) error {
	return nil
}
func (mockpipe *InitErrorer) HandleDataProcessError(err error, data interface{}, outDataPipe chan<- interface{}) {
	return
}
