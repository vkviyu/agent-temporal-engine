package storage

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

// CleanupError preserves ownership of resources whose Close has not yet been
// confirmed. In addition to inspecting its cause, callers may use errors.As and
// retry Close. Resources are retired in order; a failed file close retains the
// owner lock behind it. Constructor errors must not lose their cleanup handles.
// Close can already be complete when an outer error also retains this diagnostic.
type CleanupError struct {
	mu      sync.Mutex
	cause   error
	pending []io.Closer
}

func (e *CleanupError) Error() string { return fmt.Sprintf("resource cleanup: %v", e.cause) }
func (e *CleanupError) Unwrap() error { return e.cause }

func (e *CleanupError) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for len(e.pending) != 0 {
		if err := e.pending[0].Close(); err != nil {
			return err
		}
		e.pending[0] = nil
		e.pending = e.pending[1:]
	}
	return nil
}

func pendingCleanups(err error) []io.Closer {
	var out []io.Closer
	seen := make(map[*CleanupError]bool)
	var walk func(error)
	walk = func(err error) {
		if err == nil {
			return
		}
		if pending, ok := err.(*CleanupError); ok {
			if !seen[pending] {
				seen[pending] = true
				out = append(out, pending)
			}
			return // this handle already owns its nested resources in order
		}
		switch e := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range e.Unwrap() {
				walk(child)
			}
		case interface{ Unwrap() error }:
			walk(e.Unwrap())
		}
	}
	walk(err)
	return out
}

func cleanupOnError(cause error, resources ...io.Closer) error {
	pending := pendingCleanups(cause)
	for _, resource := range resources {
		if resource != nil {
			pending = append(pending, resource)
		}
	}
	group := &CleanupError{cause: cause, pending: pending}
	if err := group.Close(); err != nil {
		group.cause = errors.Join(cause, err)
		return group
	}
	return cause
}

// CloseOnError retires pending CleanupErrors before the supplied parent
// resources. A remaining CleanupError retains their order for caller retries.
// Composition layers use this when an unsuccessful open still owns a directory.
func CloseOnError(cause error, resources ...io.Closer) error {
	return cleanupOnError(cause, resources...)
}

func closeInto(err *error, resource io.Closer) {
	if failure := resource.Close(); failure != nil {
		*err = errors.Join(*err, &CleanupError{cause: failure, pending: []io.Closer{resource}})
	}
}
