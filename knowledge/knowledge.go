package knowledge

import "errors"

const Schema uint64 = 1
const MaxItems = 4096
const MaxRecordBytes = 4 << 20
const MaxEstimatedTokens = 16 << 20

var (
	ErrInvalid           = errors.New("invalid knowledge record")
	ErrFutureSource      = errors.New("knowledge source is after consumer commit")
	ErrKnowledgeMismatch = errors.New("knowledge key does not match source")
	ErrDuplicateItem     = errors.New("duplicate context item")
)
