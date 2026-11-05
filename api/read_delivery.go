package api

import (
	"context"

	"github.com/vkviyu/agent-temporal-engine/model"
)

// ReadDeliveryBinding identifies one authenticated transport response and the
// exact request that produced it. RequestDigest is SHA-256 of the request body.
type ReadDeliveryBinding struct {
	Destination   ReadDestination
	Method        string
	RequestDigest model.ObjectID
}

// ReadDeliveryGrant releases the authorized JSON body exactly once.
type ReadDeliveryGrant interface {
	Consume(ReadDeliveryBinding) ([]byte, error)
}

// ReadDeliveryPreparer authorizes the result of one supported read called
// with the scoped callback context. The callback only reports its error. A
// scoped read delivers its result through the returned grant.
type ReadDeliveryPreparer interface {
	PrepareReadDelivery(context.Context, ReadDeliveryBinding, func(context.Context) error) (ReadDeliveryGrant, error)
}
