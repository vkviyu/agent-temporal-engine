package engine

import "time"

// collect never holds the core while waiting for more proposals. Admissions,
// lookups of existing flights and caller cancellation remain independent of IO.
func (e *Engine) collect() []*flight {
	a := e.admission
	var batch []*flight
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		a.mu.Lock()
		for len(a.queue) > 0 && len(batch) < e.options.Batch.MaxCount {
			f := a.queue[0]
			a.queue[0] = nil
			a.queue = a.queue[1:]
			f.phase = "selected"
			batch = append(batch, f)
		}
		stopping := !a.accepting
		a.mu.Unlock()
		if len(batch) == e.options.Batch.MaxCount || (stopping && len(batch) > 0) {
			return batch
		}
		if len(batch) == 0 && stopping {
			return nil
		}
		if len(batch) > 0 && timer == nil {
			timer = time.NewTimer(e.options.Batch.MaxDelay)
		}
		var deadline <-chan time.Time
		if timer != nil {
			deadline = timer.C
		}
		select {
		case <-a.wake:
		case <-deadline:
			return batch
		}
	}
}

func (e *Engine) requeueFront(batch []*flight) {
	if len(batch) == 0 {
		return
	}
	a := e.admission
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, f := range batch {
		f.phase = "queued"
	}
	queue := make([]*flight, 0, len(batch)+len(a.queue))
	queue = append(queue, batch...)
	queue = append(queue, a.queue...)
	a.queue = queue
	signal(a)
}
