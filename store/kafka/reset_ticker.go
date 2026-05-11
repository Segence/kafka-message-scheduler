package kafka

// resetTicker will reset the scheduler to resynch al schedules for the current day
import (
	"fmt"
	"time"

	"github.com/etf1/kafka-message-scheduler/config"
	log "github.com/sirupsen/logrus"
)

type resetTicker struct {
	stopChan           chan bool
	resetChan          chan bool
	schedulingInterval int
}

func newResetTicker(schedulingInterval int) resetTicker {
	return resetTicker{
		stopChan:           make(chan bool),
		resetChan:          make(chan bool),
		schedulingInterval: schedulingInterval,
	}
}

func (r resetTicker) start() {
	schedulingInterval := func() time.Time {

		var nextPointInTime time.Time

		switch r.schedulingInterval {
		case config.ScheduleEveryDayAtMidnight:
			nextPointInTime = time.Now().AddDate(0, 0, 1)
		case config.ScheduleEveryHour:
			nextPointInTime = time.Now().Add(time.Hour)
		case config.ScheduleEvery15Minutes:
			nextPointInTime = time.Now().Add(15 * time.Minute)
		case config.ScheduleEvery5Minutes:
			nextPointInTime = time.Now().Add(5 * time.Minute)
		case config.ScheduleEveryMinute:
			nextPointInTime = time.Now().Add(time.Minute)
		}

		return time.Date(nextPointInTime.Year(), nextPointInTime.Month(),
			nextPointInTime.Day(), nextPointInTime.Hour(), nextPointInTime.Minute(),
			0, 0, nextPointInTime.Location())
	}

	ticker := time.NewTimer(time.Until(schedulingInterval()))
	defer ticker.Stop()

	go func() {
		defer log.Println("closing reset ticker ...")
		for {
			ticker.Reset(time.Until(schedulingInterval()))
			select {
			case <-r.stopChan:
				return
			case t := <-ticker.C:
				fmt.Println("Tick at", t)
				r.resetChan <- true
			}
		}
	}()
}

func (r resetTicker) close() {
	r.stopChan <- true
}

func (r resetTicker) ticks() chan bool {
	return r.resetChan
}
