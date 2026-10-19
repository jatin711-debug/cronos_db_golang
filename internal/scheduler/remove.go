package scheduler

// Remove forgets a physical WAL reference from every scheduler tier. Already
// drained worker events are still checked by the partition's delivery gate.
func (s *Scheduler) Remove(offset, scheduleTS int64) error {
	s.tickMu.Lock()
	defer s.tickMu.Unlock()
	s.budgetMu.Lock()
	defer s.budgetMu.Unlock()
	if s.coldStore != nil {
		if err := s.coldStore.DeleteIfPresent(offset, scheduleTS); err != nil {
			return err
		}
	}
	_ = s.timingWheel.RemoveTimer(offset) // absence is normal for a cold/ready event
	s.readyMu.Lock()
	n := 0
	for _, e := range s.readyQueue {
		if e.Offset != offset {
			s.readyQueue[n] = e
			n++
		}
	}
	clear(s.readyQueue[n:])
	s.readyQueue = s.readyQueue[:n]
	s.readyMu.Unlock()
	if s.budget != nil {
		s.releaseEvent(offset)
	}
	return nil
}
