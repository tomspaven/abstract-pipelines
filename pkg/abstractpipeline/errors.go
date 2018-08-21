package abstractpipeline

import (
	"errors"
	"fmt"
)

type GeneralError struct {
	RoutineName   string
	PreviousError error
}

func (e *GeneralError) Error() string {
	return fmt.Sprintf("General problem with pipeline %s: %s", e.RoutineName, e.PreviousError.Error())
}
func NewGeneralError() *GeneralError {
	return &GeneralError{
		PreviousError: errors.New(""),
	}
}

type TypeAssertionError struct {
	GenErr       *GeneralError
	ExpectedType string
}

func NewTypeAssertionError(expectedType string) *TypeAssertionError {
	return &TypeAssertionError{
		GenErr:       NewGeneralError(),
		ExpectedType: expectedType,
	}
}
func (e *TypeAssertionError) Error() string {
	return fmt.Sprintf("Type assertion on data processed by pipline routine %s - expected type %s: %s", e.GenErr.RoutineName, e.ExpectedType, e.GenErr.PreviousError.Error())
}

type InitialiseError GeneralError

func (e *InitialiseError) Error() string {
	return fmt.Sprintf("Couldn't initialise pipeline routine %s: %s", e.RoutineName, e.PreviousError.Error())
}

type ProcessError GeneralError

func (e *ProcessError) Error() string {
	return fmt.Sprintf("Processing error for pipeline %s: %s", e.RoutineName, e.PreviousError.Error())
}

type TerminateError GeneralError

func NewTerminateError() *TerminateError {
	return &TerminateError{
		PreviousError: errors.New(""),
	}
}

func (e *TerminateError) Error() string {
	return fmt.Sprintf("Problem when terminating pipline %s: %s", e.RoutineName, e.PreviousError.Error())
}
