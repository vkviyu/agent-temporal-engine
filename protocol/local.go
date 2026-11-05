// Package protocol defines ATE's bounded experimental local request/response
// transport. JSON is never used for content identity or command fingerprints.
package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

const Version uint32 = 1
const MaxFrameBytes = 16 << 20
const MaxDepth = 32

var ErrProtocol = errors.New("invalid local protocol")
var ErrFrameLimit = errors.New("local frame exceeds limit")

type Request struct {
	Version       uint32          `json:"version"`
	CorrelationID string          `json:"correlation_id"`
	Method        string          `json:"method"`
	Body          json.RawMessage `json:"body"`
}
type Error struct {
	Code    api.ErrorClass `json:"code"`
	Message string         `json:"message"`
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

type Response struct {
	Version       uint32          `json:"version"`
	CorrelationID string          `json:"correlation_id"`
	Body          json.RawMessage `json:"body"`
	Error         *Error          `json:"error,omitempty"`
}
type HelloRequest struct {
	Token string `json:"token"`
}
type HelloResult struct {
	Principal    model.PrincipalID `json:"principal"`
	Spaces       []model.SpaceID   `json:"spaces"`
	Capabilities []string          `json:"capabilities"`
}
type CommitRequest struct {
	Space         model.SpaceID      `json:"space"`
	RequestID     model.RequestID    `json:"request_id"`
	Method        string             `json:"method"`
	Body          []byte             `json:"body"`
	ExpectedFence *model.CommitFence `json:"expected_fence,omitempty"`
}
type MutateRequest struct {
	Space         model.SpaceID      `json:"space"`
	RequestID     model.RequestID    `json:"request_id"`
	Batch         api.StateBatch     `json:"batch"`
	ExpectedFence *model.CommitFence `json:"expected_fence,omitempty"`
}
type LookupRequest struct {
	Space     model.SpaceID   `json:"space"`
	RequestID model.RequestID `json:"request_id"`
}
type ReadStateRequest struct {
	Space model.SpaceID `json:"space"`
	Key   []byte        `json:"key"`
}
type OpenViewRequest struct {
	Space     model.SpaceID         `json:"space"`
	Anchor    *model.SnapshotAnchor `json:"anchor,omitempty"`
	Knowledge *model.KnowledgeKey   `json:"knowledge,omitempty"`
	Mode      string                `json:"mode"`
}
type CloseViewRequest struct {
	Ref model.ViewRef `json:"ref"`
}
type ReadViewRequest struct {
	View model.ViewRef `json:"view"`
	Key  []byte        `json:"key"`
}
type ReadViewEntriesRequest struct {
	View   model.ViewRef          `json:"view"`
	Limits api.StateEntriesLimits `json:"limits"`
}
type ResolvePublicationRequest struct {
	Source model.SourcePublicationRef `json:"source"`
}
type Empty struct{}

func ReadFrame(r io.Reader) ([]byte, error) { return ReadFrameLimit(r, MaxFrameBytes) }

// ReadFrameLimit checks a receiver's lower frame ceiling before allocation.
// It uses exactly the same wire format and never raises the protocol ceiling.
func ReadFrameLimit(r io.Reader, maximum uint32) ([]byte, error) {
	if maximum == 0 || maximum > MaxFrameBytes {
		return nil, ErrFrameLimit
	}
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(h[:])
	if n == 0 {
		return nil, ErrProtocol
	}
	if n > maximum {
		return nil, ErrFrameLimit
	}
	p := make([]byte, int(n))
	_, err := io.ReadFull(r, p)
	return p, err
}
func WriteFrame(w io.Writer, p []byte) error {
	if len(p) == 0 {
		return ErrProtocol
	}
	if len(p) > MaxFrameBytes {
		return ErrFrameLimit
	}
	var h [4]byte
	binary.BigEndian.PutUint32(h[:], uint32(len(p)))
	if err := writeAll(w, h[:]); err != nil {
		return err
	}
	return writeAll(w, p)
}
func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n < 0 || n > len(p) {
			return io.ErrShortWrite
		}
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

// DecodeStrict rejects duplicate object keys at every depth before decoding
// typed integer/byte fields. Numbers never pass through a float64 conversion.
func DecodeStrict(p []byte, out any) error {
	if len(p) > MaxFrameBytes {
		return ErrFrameLimit
	}
	if !utf8.Valid(p) {
		return ErrProtocol
	}
	d := json.NewDecoder(bytes.NewReader(p))
	d.UseNumber()
	if err := checkValue(d, 0); err != nil {
		return fmt.Errorf("%w: %w", ErrProtocol, err)
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("%w: trailing JSON", ErrProtocol)
	}
	d = json.NewDecoder(bytes.NewReader(p))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return fmt.Errorf("%w: %w", ErrProtocol, err)
	}
	return nil
}
func checkValue(d *json.Decoder, depth int) error {
	if depth > MaxDepth {
		return ErrProtocol
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]bool)
		for d.More() {
			k, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := k.(string)
			if !ok || seen[key] {
				return ErrProtocol
			}
			seen[key] = true
			if err = checkValue(d, depth+1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return ErrProtocol
		}
	case '[':
		for d.More() {
			if err := checkValue(d, depth+1); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return ErrProtocol
		}
	default:
		return ErrProtocol
	}
	return nil
}
func DecodeRequest(p []byte) (Request, error) {
	var r Request
	err := DecodeStrict(p, &r)
	if err != nil {
		return r, err
	}
	if r.Version != Version || r.CorrelationID == "" || len(r.CorrelationID) > 128 || r.Method == "" || len(r.Method) > 128 || len(r.Body) == 0 || bytes.Equal(r.Body, []byte("null")) {
		return Request{}, ErrProtocol
	}
	return r, nil
}
func DecodeResponse(p []byte) (Response, error) {
	var r Response
	err := DecodeStrict(p, &r)
	if err != nil {
		return r, err
	}
	if r.Version != Version || r.CorrelationID == "" || len(r.CorrelationID) > 128 || len(r.Body) == 0 {
		return Response{}, ErrProtocol
	}
	if r.Error != nil && !ValidErrorClass(r.Error.Code) {
		return Response{}, ErrProtocol
	}
	return r, nil
}
func ValidErrorClass(c api.ErrorClass) bool {
	switch c {
	case api.ErrorInvalidRequest, api.ErrorUnauthorized, api.ErrorUnsupported, api.ErrorResourceLimit, api.ErrorConflict, api.ErrorWaitInterrupted, api.ErrorCommitUnknown, api.ErrorStoreFenced, api.ErrorCorruption, api.ErrorClosing, api.ErrorClosed, api.ErrorInvalidView, api.ErrorViewExpired, api.ErrorAnchorMismatch, api.ErrorCanceled, api.ErrorDeadline, api.ErrorInternal, api.ErrorNotFound:
		return true
	}
	return false
}
