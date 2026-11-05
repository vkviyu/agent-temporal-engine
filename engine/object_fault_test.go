package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

func reviewCorruptValueRefCount(t *testing.T, path string, d storage.DurableIO, space model.SpaceID, data []byte) {
	t.Helper()
	value, err := makeValue(space, api.Value{Kind: api.ValueBytes, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.EncodeObject(value)
	if err != nil {
		t.Fatal(err)
	}
	// Locate the reference count in a canonical value envelope. Its two-byte
	// replacement also overwrites the following length without growing the file.
	decoder := codec.NewDecoder(encoded[len("ATEOBJ01"):])
	if _, err = decoder.Uint(); err != nil {
		t.Fatal(err)
	}
	if _, err = decoder.Uint(); err != nil {
		t.Fatal(err)
	}
	if _, err = decoder.String(128); err != nil {
		t.Fatal(err)
	}
	if _, err = decoder.String(model.MaxIdentityBytes); err != nil {
		t.Fatal(err)
	}
	if _, err = decoder.Uint(); err != nil {
		t.Fatal(err)
	}
	countAt := len(encoded) - decoder.Remaining()
	if encoded[countAt] != 0 {
		t.Fatal("value object unexpectedly has references")
	}
	name, segment := recoverySegment(t, path, d)
	objectAt := bytes.Index(segment, encoded)
	if objectAt < 0 {
		t.Fatal("stored value object missing")
	}
	file, err := os.OpenFile(filepath.Join(path, filepath.FromSlash(name)), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := file.WriteAt([]byte{0x81, 0x20}, int64(objectAt+countAt)); err != nil || n != 2 {
		t.Fatalf("corrupt reference count: %d %v", n, err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEngineStoredObjectDecodeLimitFences(t *testing.T) {
	path, d := recoveryDirectory(t)
	e, err := Create(d, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	request := recoveryCommitRequest(t, "decode-limit-corruption", "value", api.ValueCondition{})
	recoveryAck(t, e, request)
	reviewCorruptValueRefCount(t, path, d, request.Space, []byte("value"))
	got, err := e.GetState(context.Background(), api.ReadStateRequest{Space: request.Space, Key: []byte("key")})
	if !errors.Is(err, codec.ErrObjectLimit) || !errors.Is(err, ErrStoredObject) || !errors.Is(err, storage.ErrCorruption) || !reflect.DeepEqual(got, api.StateValueResult{}) {
		t.Fatalf("fixture did not reach a stored decode-limit failure: %+v %v", got, err)
	}
	if got, err := e.Snapshot(context.Background()); !errors.Is(err, storage.ErrStoreFenced) || got != (api.StateSnapshot{}) {
		t.Fatalf("stored object decode-limit corruption left active reads enabled: %+v %v", got, err)
	}
	recoveryAssertFencedReads(t, e, request)
}

type reviewFailingReaderAt struct {
	reader io.ReaderAt
	err    error
	short  bool
}

func (r reviewFailingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if !r.short {
		return 0, r.err
	}
	n, _ := r.reader.ReadAt(p[:len(p)-1], off)
	return n, r.err
}

func TestEngineStoredReadIOAndShortEOFClassification(t *testing.T) {
	for _, tc := range []struct {
		name       string
		readErr    error
		short      bool
		corruption bool
	}{
		{name: "EIO", readErr: syscall.EIO},
		{name: "short-EOF", readErr: io.EOF, short: true, corruption: true},
		{name: "short-without-error", short: true, corruption: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, d := recoveryDirectory(t)
			e, err := Create(d, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			request := recoveryCommitRequest(t, "stored-read-failure", "value", api.ValueCondition{})
			ack := recoveryAck(t, e, request)
			before := e.journal.Status().LastPublication
			e.index.source = reviewFailingReaderAt{reader: e.index.source, err: tc.readErr, short: tc.short}
			got, err := e.GetState(context.Background(), api.ReadStateRequest{Space: request.Space, Key: []byte("key")})
			if !errors.Is(err, storage.ErrStoreFenced) || !reflect.DeepEqual(got, api.StateValueResult{}) || errors.Is(err, storage.ErrCorruption) != tc.corruption {
				t.Fatalf("stored read failure lost fail-stop or classification: %+v %v", got, err)
			}
			if tc.corruption {
				if !errors.Is(err, ErrStoredObject) || !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrObjectIO) {
					t.Fatalf("short published object was not classified as missing bytes: %v", err)
				}
			} else if !errors.Is(err, ErrObjectIO) || !errors.Is(err, syscall.EIO) || errors.Is(err, ErrSemanticCorruption) || errors.Is(err, ErrStoredObject) {
				t.Fatalf("ordinary IO was guessed to be content corruption: %v", err)
			}
			if status := e.journal.Status(); status.State != storage.JournalFenced || status.LastPublication != before || status.LastPublication.Fence() != ack.Fence {
				t.Fatalf("read failure changed the last diagnostic publication: %+v", status)
			}
			recoveryAssertFencedReads(t, e, request)
		})
	}
}

func TestEnginePrepareStoredDecodeLimitFencesInsteadOfResourceRejection(t *testing.T) {
	path, d := recoveryDirectory(t)
	e, err := Create(d, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	request := recoveryCommitRequest(t, "seed-before-prepare", "value", api.ValueCondition{})
	recoveryAck(t, e, request)
	reviewCorruptValueRefCount(t, path, d, request.Space, []byte("value"))
	before := e.journal.Status().LastPublication
	objects := len(e.index.entries)
	tree := recoveryTree(t, path)
	fresh := recoveryCommitRequest(t, "prepare-must-fence", "replacement", api.ValueCondition{})
	result, err := e.Commit(context.Background(), fresh)
	if !errors.Is(err, storage.ErrStoreFenced) || !errors.Is(err, ErrStoredObject) || !errors.Is(err, storage.ErrCorruption) || !errors.Is(err, codec.ErrObjectLimit) || result.Code == api.CodeResourceLimit || result.Code == api.CodeDurableAck {
		t.Fatalf("stored corruption in prepare became ordinary admission rejection: %+v %v", result, err)
	}
	if status := e.journal.Status(); status.State != storage.JournalFenced || status.LastPublication != before {
		t.Fatalf("prepare read corruption changed publication or failed to fence: %+v", status)
	}
	if len(e.index.entries) != objects || !reflect.DeepEqual(tree, recoveryTree(t, path)) {
		t.Fatal("failed prepare exposed staged objects or wrote journal bytes")
	}
	recoveryAssertFencedReads(t, e, request)
}
