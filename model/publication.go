package model

import (
	"encoding/hex"
	"strings"
	"unicode/utf8"
)

// SourcePublicationRef names the exact publication produced by one transaction
// without embedding that transaction's CommitDigest in its own command bytes.
// It is a durable reference, not a read capability. Resolution authenticates
// the producer, its scope, and selectors against the store's committed log.
type SourcePublicationRef struct {
	Store      StoreID
	Space      SpaceID
	ProducerTx TxID
	Cursor     *Cursor
	Knowledge  *KnowledgeKey
}

func (r SourcePublicationRef) Validate() error {
	for _, s := range []string{string(r.Store), string(r.Space)} {
		if s == "" || len(s) > MaxIdentityBytes || !utf8.ValidString(s) {
			return ErrAnchorMismatch
		}
	}
	if len(r.ProducerTx) != 32 || string(r.ProducerTx) != strings.ToLower(string(r.ProducerTx)) || string(r.ProducerTx) == strings.Repeat("0", 32) {
		return ErrAnchorMismatch
	}
	if _, err := hex.DecodeString(string(r.ProducerTx)); err != nil {
		return ErrAnchorMismatch
	}
	if c := r.Cursor; c != nil && (c.Store != r.Store || c.Space != r.Space || !ValidDigest(string(c.Revision))) {
		return ErrAnchorMismatch
	}
	if k := r.Knowledge; k != nil {
		for _, s := range []string{string(k.Actor), string(k.Execution)} {
			if s == "" || len(s) > MaxIdentityBytes || !utf8.ValidString(s) {
				return ErrAnchorMismatch
			}
		}
	}
	return nil
}

func (r SourcePublicationRef) Clone() SourcePublicationRef {
	if r.Cursor != nil {
		v := *r.Cursor
		r.Cursor = &v
	}
	if r.Knowledge != nil {
		v := *r.Knowledge
		r.Knowledge = &v
	}
	return r
}
