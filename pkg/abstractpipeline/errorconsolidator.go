package abstractpipeline

import "fmt"

func (routine *RoutineSet) startErrorConsolidatorAndMerge(routineErrorOutPipe, pipelineConsolidatedOutPipe chan error, subRoutineID int) {

	routine.logErrorConsolidatorStarted(subRoutineID)

	go func() {
		defer routine.logErrorConsolidatorTerminated(subRoutineID)
		for errorFromRoutine := range routineErrorOutPipe {
			pipelineConsolidatedOutPipe <- errorFromRoutine
		}
	}()
	return
}

func (routine *RoutineSet) logErrorConsolidatorStarted(subRoutineID int) {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("Error consolidator started for routine/subroutine %s/%d.", routine.name, routine.numRoutines))
}

func (routine *RoutineSet) logErrorConsolidatorTerminated(subRoutineID int) {
	routine.cntl.log.OutLog.Println(fmt.Sprintf("Error consolidator terminated for routine/subroutine %s/%d.", routine.name, routine.numRoutines))
}
