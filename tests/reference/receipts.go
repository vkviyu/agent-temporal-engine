package reference

import "slices"

type RequestIdentity struct{ Space, Principal, ID string }
type Receipt struct {
	Command     string
	Publication uint64
}
type VisibleOccurrence struct {
	Space, Principal, Method string
	Publication, Sequence    uint64
}
type ReceiptBook struct {
	Publication uint64
	Receipts    map[RequestIdentity]Receipt
	Events      []VisibleOccurrence
}

func NewReceiptBook() *ReceiptBook { return &ReceiptBook{Receipts: map[RequestIdentity]Receipt{}} }

// Submit models receipt-first idempotency and global CAS without hashing or
// sharing a receipt/root codec with the implementation under test.
func (b *ReceiptBook) Submit(key RequestIdentity, command, method string, expected *uint64) (string, uint64) {
	if old, ok := b.Receipts[key]; ok {
		if old.Command != command {
			return "idempotency_conflict", 0
		}
		return "ack", old.Publication
	}
	if expected != nil && *expected != b.Publication {
		return "precondition_failed", 0
	}
	b.Publication++
	b.Receipts[key] = Receipt{command, b.Publication}
	var seq uint64
	for _, e := range b.Events {
		if e.Space == key.Space && e.Principal == key.Principal {
			seq++
		}
	}
	b.Events = append(b.Events, VisibleOccurrence{key.Space, key.Principal, method, b.Publication, seq + 1})
	return "ack", b.Publication
}
func (b *ReceiptBook) Visible(space, principal string, fence uint64) []VisibleOccurrence {
	out := []VisibleOccurrence{}
	for _, e := range b.Events {
		if e.Space == space && e.Principal == principal && e.Publication <= fence {
			out = append(out, e)
		}
	}
	return slices.Clone(out)
}
