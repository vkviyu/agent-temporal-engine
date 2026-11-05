package api

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/model"
)

const MaxCommandBody = 8 << 20
const MaxIdentityBytes = model.MaxIdentityBytes

var ErrInvalidRequest = errors.New("invalid request")

// RequestKey is shared with storage without importing API or transport code.
type RequestKey = model.RequestKey

func (r CommitRequest) Key() RequestKey {
	return RequestKey{Space: r.Space, Principal: r.Principal, RequestID: r.RequestID}
}
func (r LookupRequestRequest) Key() RequestKey {
	return RequestKey{Space: r.Space, Principal: r.Principal, RequestID: r.RequestID}
}

func (r LookupRequestRequest) Validate() error {
	if err := r.Key().Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	return nil
}

func (r CommitRequest) Validate() error {
	if err := r.Key().Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	if r.Method == "" || len(r.Method) > MaxIdentityBytes || !utf8.ValidString(r.Method) {
		return fmt.Errorf("%w: invalid method", ErrInvalidRequest)
	}
	if len(r.Body) > MaxCommandBody {
		return fmt.Errorf("%w: body exceeds %d bytes", ErrInvalidRequest, MaxCommandBody)
	}
	if r.ExpectedFence != nil {
		if err := r.ExpectedFence.Validate(); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
		}
		if len(r.ExpectedFence.Store) > MaxIdentityBytes || !utf8.ValidString(string(r.ExpectedFence.Store)) {
			return fmt.Errorf("%w: invalid fence store", ErrInvalidRequest)
		}
	}
	return nil
}
