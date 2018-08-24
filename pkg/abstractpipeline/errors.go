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
func NewGeneralError(routineName string, previousError error) *GeneralError {

	if previousError == nil {
		previousError = errors.New("")
	}
	return &GeneralError{
		RoutineName:   routineName,
		PreviousError: previousError,
	}
}

type TypeAssertionError struct {
	GenErr       *GeneralError
	ExpectedType string
}

func NewTypeAssertionError(genError *GeneralError, expectedType string) *TypeAssertionError {
	return &TypeAssertionError{
		GenErr:       genError,
		ExpectedType: expectedType,
	}
}
func (e *TypeAssertionError) Error() string {
	return fmt.Sprintf("Type assertion on data processed by pipline routine %s - expected type %s: %s", e.GenErr.RoutineName, e.ExpectedType, e.GenErr.PreviousError.Error())
}

type InitialiseError struct {
	GenErr *GeneralError
}

func (e *InitialiseError) Error() string {
	return fmt.Sprintf("Couldn't initialise pipeline routine %s: %s", e.GenErr.RoutineName, e.GenErr.PreviousError.Error())
}

type ProcessError GeneralError

func (e *ProcessError) Error() string {
	return fmt.Sprintf("Processing error for pipeline %s: %s", e.RoutineName, e.PreviousError.Error())
}

type TerminateError struct {
	GenErr *GeneralError
}

func (e *TerminateError) Error() string {
	return fmt.Sprintf("Problem when terminating pipline %s: %s", e.GenErr.RoutineName, e.GenErr.PreviousError.Error())
}
