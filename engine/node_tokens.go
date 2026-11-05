package engine

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/model"
)

type nodePageToken struct{ Scope, After string }

func nodePageScope(entry *viewEntry, kind string, filter any) string {
	b, _ := json.Marshal(struct {
		View   model.ViewRef
		Digest model.ObjectID
		Kind   string
		Filter any
	}{entry.descriptor.Ref, entry.descriptor.ViewDigest, kind, filter})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func readNodeToken(raw, scope string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if len(raw) > 16384 {
		return "", api.ErrInvalidRequest
	}
	b, e := base64.RawURLEncoding.DecodeString(raw)
	if e != nil {
		return "", api.ErrInvalidRequest
	}
	var t nodePageToken
	if json.Unmarshal(b, &t) != nil || t.Scope != scope || t.After == "" {
		return "", api.ErrInvalidRequest
	}
	expected, _ := json.Marshal(t)
	if string(expected) != string(b) {
		return "", api.ErrInvalidRequest
	}
	return t.After, nil
}
func writeNodeToken(scope, after string) string {
	b, _ := json.Marshal(nodePageToken{scope, after})
	return base64.RawURLEncoding.EncodeToString(b)
}
func nodeSortKey(ref model.NodeRef) string { b, _ := json.Marshal(ref); return string(b) }
func jsonBytes(v any) int                  { b, _ := json.Marshal(v); return len(b) }
