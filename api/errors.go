package api

import "errors"

var (
	ErrReadEmissionDenied       = errors.New("read emission denied")
	ErrReadAuthorizationChanged = errors.New("read authorization changed before grant issuance")
)

// ErrorClass is a stable machine-readable failure category shared by library
// adapters and the local protocol. ResultCode still describes commit outcomes.
type ErrorClass string

const (
	ErrorInvalidRequest  ErrorClass = "invalid_request"
	ErrorUnauthorized    ErrorClass = "unauthorized"
	ErrorUnsupported     ErrorClass = "unsupported"
	ErrorResourceLimit   ErrorClass = "resource_limit"
	ErrorConflict        ErrorClass = "conflict"
	ErrorWaitInterrupted ErrorClass = "wait_interrupted"
	ErrorCommitUnknown   ErrorClass = "commit_unknown"
	ErrorStoreFenced     ErrorClass = "store_fenced"
	ErrorCorruption      ErrorClass = "corruption_detected"
	ErrorClosing         ErrorClass = "closing"
	ErrorClosed          ErrorClass = "closed"
	ErrorInvalidView     ErrorClass = "invalid_view"
	ErrorViewExpired     ErrorClass = "view_expired"
	ErrorAnchorMismatch  ErrorClass = "anchor_mismatch"
	ErrorCanceled        ErrorClass = "canceled"
	ErrorDeadline        ErrorClass = "deadline_exceeded"
	ErrorInternal        ErrorClass = "internal"
	ErrorNotFound        ErrorClass = "not_found"
)
