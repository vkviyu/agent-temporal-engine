package model

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

const MaxIdentityBytes = 256

var ErrInvalidRequestKey = errors.New("invalid request key")

// RequestKey scopes idempotency to an authenticated stable principal and space.
// It is independent of temporary connections, deadlines, or correlation IDs.
type RequestKey struct {
	Space     SpaceID
	Principal PrincipalID
	RequestID RequestID
}

func (k RequestKey) Validate() error {
	for _, f := range []struct{ name, value string }{{"space", string(k.Space)}, {"principal", string(k.Principal)}, {"request ID", string(k.RequestID)}} {
		if f.value == "" || len(f.value) > MaxIdentityBytes || !utf8.ValidString(f.value) {
			return fmt.Errorf("%w: %s must be 1..%d UTF-8 bytes", ErrInvalidRequestKey, f.name, MaxIdentityBytes)
		}
	}
	return nil
}
