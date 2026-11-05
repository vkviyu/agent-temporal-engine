package engine

import (
	"context"
	"sync"
)

// contextMutex preserves a single core owner while allowing a caller waiting
// for that owner to cancel. It never polls and its zero value is usable.
type contextMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *contextMutex) init() {
	m.once.Do(func() { m.token = make(chan struct{}, 1); m.token <- struct{}{} })
}
func (m *contextMutex) Lock()   { m.init(); <-m.token }
func (m *contextMutex) Unlock() { m.token <- struct{}{} }
func (m *contextMutex) LockContext(ctx context.Context) error {
	m.init()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.token:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	}
}
