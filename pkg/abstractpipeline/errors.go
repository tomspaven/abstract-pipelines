package abstractpipeline

import "fmt"

type generalError struct {
	pipelineName  string
	previousError error
}
type InitialiseError generalError
type ProcessError generalError
type TerminateError generalError

func (e generalError) Error() string {
	return fmt.Sprintf("General problem with pipeline %s: %s", e.pipelineName, e.previousError.Error())
}
func (e *InitialiseError) Error() string {
	return fmt.Sprintf("Couldn't initialise pipeline routine %s: %s", e.pipelineName, e.previousError.Error())
}
func (e *ProcessError) Error() string {
	return fmt.Sprintf("Processing error for pipeline %s: %s", e.pipelineName, e.previousError.Error())
}
func (e *TerminateError) Error() string {
	return fmt.Sprintf("Problem when terminating pipline %s: %s", e.pipelineName, e.previousError.Error())
}

func pipelineErrorFactory(genError generalError, errorType string) error {
	switch errorType {
	case "initialise":
		return &InitialiseError{genError.pipelineName, genError.previousError}
	case "process":
		return &ProcessError{genError.pipelineName, genError.previousError}
	case "terminate":
		return &TerminateError{genError.pipelineName, genError.previousError}
	default:
		return genError
	}
}
