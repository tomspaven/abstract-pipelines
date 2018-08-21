package abstractpipeline

import "time"

type TriggerGenerator interface {
	GenerateTrigger() <-chan interface{}
}

type TimedTriggerMonitor struct {
	checkIntervalMilliseconds time.Duration
}

func (trigger *TimedTriggerMonitor) GenerateTrigger() <-chan interface{} {
	// Need to setup like this as ticker.C is a channel of a concrete type.
	// This essentially listens for and converts the ticker signal (sending on an abstract channel)
	// so it can be used with the pipeline framework
	triggerChan := make(chan interface{})
	checkForFileTicker := time.NewTicker(trigger.checkIntervalMilliseconds * time.Millisecond)
	go func() {
		for {
			<-checkForFileTicker.C
			triggerChan <- struct{}{}
		}
	}()
	return triggerChan
}
