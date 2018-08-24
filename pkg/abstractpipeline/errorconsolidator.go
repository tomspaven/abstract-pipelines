package abstractpipeline

func (pipeline *Pipeline) startErrorConsolidatorAndMerge(routineErrorOutPipe chan error, errConsID int) {

	pipeline.logStarted(errorConsolidatorName, errConsID)

	go func() {
		defer pipeline.logTerminated(errorConsolidatorName, errConsID)
		for errorFromRoutine := range routineErrorOutPipe {
			pipeline.ErrorOutPipe <- errorFromRoutine
		}
	}()
	return
}
