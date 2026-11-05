package engine

import (
	"errors"
	"github.com/vkviyu/agent-temporal-engine/transfer"
)

func transferCommandRejection(err error) bool { return errors.Is(err, transfer.ErrInvalid) }
