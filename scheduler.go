package scheduler

import (
	"container/heap"
	"log"
	"time"
)

type QueueItem struct {
	*Event
	timestamp int64
	index     int
}

type EventQueue []*QueueItem

func (pq EventQueue) Len() int { return len(pq) }

func (pq EventQueue) Less(i, j int) bool {
	return pq[i].timestamp < pq[j].timestamp
}

func (pq EventQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *EventQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*QueueItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *EventQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

type Event struct {
	When     string
	Interval string
	What     string
	On       string
	ExceptOn string
	Days     map[string]string
	time     time.Time
	on       time.Time
}

func (e *Event) duration() time.Duration {
	if d, err := time.ParseDuration(e.Interval); err != nil {
		log.Println("could not parse interval of '" + e.Interval + "' for " + e.What)
		return 60 * 60 * 24 * time.Second
	} else {
		return d
	}
}

func (e *Event) next() time.Time {
	duration := e.duration()
	wait := time.Duration((e.on.UnixNano() - time.Now().UnixNano()) % int64(duration))
	if wait < 0 {
		wait += duration
	}
	t := time.Now().Add(wait)
	log.Println("next '" + e.What + "' on: " + t.String())
	return t
}

var WEEKDAYS = []string{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"}

var HOLIDAYS = map[string]string{"Christmas Day": "2012-12-25", "New Year's Day": "2013-01-01", "Birthday of Martin Luther King, Jr.": "2013-01-21", "Washington's Birthday": "2013-02-18", "Memorial Day": "2013-05-27", "Independence Day": "2013-07-04", "Labor Day": "2013-09-02", "Columbus Day": "2013-10-14", "Veterans Day": "2013-11-11", "Thanksgiving Day": "2013-11-28", "Christmas Day 2013": "2013-12-25"}

func (e *Event) shouldRun(t time.Time) bool {
	t = t.In(time.Local)
	run := false
	if e.On == "" {
		run = true
	} else if e.On == "weekdays" {
		d := t.Weekday().String()
		for _, wd := range WEEKDAYS {
			if d == wd {
				run = true
				break
			}
		}
	} else if e.On == "weekends" {
		d := t.Weekday().String()
		for _, wd := range []string{"Saturday", "Sunday"} {
			if d == wd {
				run = true
				break
			}
		}
	}
	if e.ExceptOn == "" {

	} else if e.ExceptOn == "holidays" {
		s := t.Format("2006-01-02")
		for _, v := range HOLIDAYS {
			if s == v {
				log.Println("not running due to holiday:", e)
				run = false
				break
			}
		}
	}
	return run
}

type Schedule []*Event

func (s Schedule) Run() (chan Event, error) {
	eventsCh := make(chan Event, 1)

	pq := &EventQueue{}
	heap.Init(pq)

	now := time.Now()
	zone, _ := now.Zone()

	for _, e := range s {
		if on, err := time.Parse("2006-01-02 "+time.Kitchen+" MST", now.Format("2006-01-02 ")+e.When+" "+zone); err != nil {
			log.Println("could not parse when of '" + e.When + "' for " + e.What)
			continue
		} else {
			e.on = on
		}

		t := e.next()
		heap.Push(pq, &QueueItem{Event: e, timestamp: t.UnixNano()})
	}

	go func(pq *EventQueue) {
		for pq.Len() > 0 {
			item := heap.Pop(pq).(*QueueItem)
			now := time.Now()
			d := time.Duration(item.timestamp - now.UnixNano())
			if d < 0 {
				if item.shouldRun(now) {
					log.Println(item.What + " at " + now.String())
					item.time = now
					eventsCh <- *item.Event
				}
				item.timestamp = item.next().UnixNano()
				heap.Push(pq, item)
			} else {
				heap.Push(pq, item)
				if d < time.Second {
					time.Sleep(d)
				} else {
					time.Sleep(time.Second)
				}
			}
		}
	}(pq)

	return eventsCh, nil
}
