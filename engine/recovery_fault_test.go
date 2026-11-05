package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
	"github.com/vkviyu/agent-temporal-engine/storage"
)

var errRecoveryCurrentReplace = errors.New("test: CURRENT replacement failed")

type recoveryCurrentFaultIO struct {
	storage.DurableIO
	armed    bool
	after    bool
	injected int
}

func (d *recoveryCurrentFaultIO) ReplaceAtomic(from, to string) error {
	if !d.armed || to != "CURRENT" {
		return d.DurableIO.ReplaceAtomic(from, to)
	}
	d.armed = false
	d.injected++
	if !d.after {
		return errRecoveryCurrentReplace
	}
	if err := d.DurableIO.ReplaceAtomic(from, to); err != nil {
		return err
	}
	return errRecoveryCurrentReplace
}

type recoveryObservedIO struct {
	storage.DurableIO
	mutations []string
}

func (d *recoveryObservedIO) CreateExclusive(name string) (storage.DurableFile, error) {
	d.mutations = append(d.mutations, "CreateExclusive "+name)
	return d.DurableIO.CreateExclusive(name)
}

func (d *recoveryObservedIO) Mkdir(name string) error {
	d.mutations = append(d.mutations, "Mkdir "+name)
	return d.DurableIO.Mkdir(name)
}

func (d *recoveryObservedIO) SyncDir(name string) error {
	d.mutations = append(d.mutations, "SyncDir "+name)
	return d.DurableIO.SyncDir(name)
}

func recoveryDirectory(t *testing.T) (string, *storage.DirectoryIO) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("native durability profile requires Darwin or Linux")
	}
	path := t.TempDir()
	d, err := storage.OpenDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return path, d
}

func recoveryCommitRequest(t *testing.T, id string, value string, condition api.ValueCondition) api.CommitRequest {
	t.Helper()
	body, err := EncodeStateBatch(api.StateBatch{Operations: []api.StateMutation{{
		Key: []byte("key"), Kind: api.MutationPut,
		Value: api.Value{Kind: api.ValueBytes, Data: []byte(value)}, Condition: condition,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return api.CommitRequest{Space: "recovery-space", Principal: "host", RequestID: model.RequestID(id), Method: api.MethodMutateState, Body: body}
}

func recoveryAck(t *testing.T, e *Engine, request api.CommitRequest) api.CommitResult {
	t.Helper()
	result, err := e.Commit(context.Background(), request)
	if err != nil || result.Code != api.CodeDurableAck {
		t.Fatalf("expected durable ACK: %+v %v", result, err)
	}
	return result
}

func recoveryLookupRequest(request api.CommitRequest) api.LookupRequestRequest {
	return api.LookupRequestRequest{Space: request.Space, Principal: request.Principal, RequestID: request.RequestID}
}

func recoveryAssertFencedReads(t *testing.T, e *Engine, request api.CommitRequest) {
	t.Helper()
	for _, key := range [][]byte{[]byte("key"), []byte("never-written")} {
		got, err := e.GetState(context.Background(), api.ReadStateRequest{Space: request.Space, Key: key})
		if !errors.Is(err, storage.ErrStoreFenced) || !reflect.DeepEqual(got, api.StateValueResult{}) {
			t.Fatalf("fenced state read leaked a value or false absence: %+v %v", got, err)
		}
	}
	for _, id := range []model.RequestID{request.RequestID, "never-submitted"} {
		lookup := recoveryLookupRequest(request)
		lookup.RequestID = id
		got, err := e.LookupRequest(context.Background(), lookup)
		if !errors.Is(err, storage.ErrStoreFenced) || (got.Code != api.CodeStoreFenced && got.Code != api.CodeIndeterminate) || got.Result != (api.CommitResult{}) || got.ObservedFence != (model.CommitFence{}) {
			t.Fatalf("fenced lookup exposed old ACK or false NotFound: %+v %v", got, err)
		}
	}
	if got, err := e.Snapshot(context.Background()); !errors.Is(err, storage.ErrStoreFenced) || got != (api.StateSnapshot{}) {
		t.Fatalf("fenced snapshot exposed roots or an active fence: %+v %v", got, err)
	}
}

func recoverySegment(t *testing.T, path string, d storage.DurableIO) (string, []byte) {
	t.Helper()
	names, err := d.List("segments")
	if err != nil {
		t.Fatal(err)
	}
	var name string
	for _, candidate := range names {
		if strings.HasPrefix(candidate, "SEGMENT.") {
			if name != "" {
				t.Fatal("test requires one active segment")
			}
			name = "segments/" + candidate
		}
	}
	if name == "" {
		t.Fatal("active segment missing")
	}
	contents, err := os.ReadFile(filepath.Join(path, filepath.FromSlash(name)))
	if err != nil {
		t.Fatal(err)
	}
	return name, contents
}

func recoveryTree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			files[relative+"/"] = nil
			return nil
		}
		contents, err := os.ReadFile(path)
		if err == nil {
			files[relative] = contents
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestEngineCurrentReplacementFailureFencesUntilRecovery(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("after=%t", after), func(t *testing.T) {
			path, directory := recoveryDirectory(t)
			d := &recoveryCurrentFaultIO{DurableIO: directory, after: after}
			e, err := Create(d, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if e != nil {
					if err := e.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			firstRequest := recoveryCommitRequest(t, "known-ack", "old", api.ValueCondition{Kind: api.ConditionAbsent})
			first := recoveryAck(t, e, firstRequest)
			secondRequest := recoveryCommitRequest(t, "uncertain", "new", api.ValueCondition{Kind: api.ConditionValue, Value: api.Value{Kind: api.ValueBytes, Data: []byte("old")}})
			d.armed = true
			unknown, err := e.Commit(context.Background(), secondRequest)
			if d.injected != 1 || unknown.Code != api.CodeCommitUnknown || !errors.Is(err, storage.ErrCommitUnknown) || !errors.Is(err, errRecoveryCurrentReplace) {
				t.Fatalf("CURRENT error did not produce CommitUnknown: injected=%d result=%+v err=%v", d.injected, unknown, err)
			}
			recoveryAssertFencedReads(t, e, firstRequest)
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			e = nil
			currentBytes, err := os.ReadFile(filepath.Join(path, "CURRENT"))
			if err != nil {
				t.Fatal(err)
			}
			current, err := storage.DecodeCurrent(currentBytes)
			if err != nil {
				t.Fatal(err)
			}
			manifestBytes, err := os.ReadFile(filepath.Join(path, "manifests", "MANIFEST."+current.ManifestID))
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := storage.DecodeManifest(manifestBytes)
			if err != nil {
				t.Fatal(err)
			}
			committed := manifest.Position.LSN == first.Fence.LSN+1
			if committed != after || (!committed && manifest.Position.LSN != first.Fence.LSN) {
				t.Fatalf("fixture CURRENT does not identify its actual old/new publication: %+v", manifest.Position)
			}
			var report storage.RecoveryReport
			e, report, err = OpenExisting(d, Options{})
			if !committed {
				if e != nil || !errors.Is(err, storage.ErrRecoveryRequired) || !report.Verified || report.TailBytes == 0 {
					t.Fatalf("old CURRENT silently promoted or discarded unreferenced transaction: %+v %v", report, err)
				}
				e, report, err = OpenExisting(d, Options{Journal: storage.JournalOptions{TailPolicy: storage.TailQuarantine}})
				if err != nil || report.TailDisposition != "quarantined" {
					t.Fatalf("explicit tail recovery failed: %+v %v", report, err)
				}
			}
			if err != nil || e == nil || !report.Verified || !report.PublicationSynced || report.Publication.Position != manifest.Position {
				t.Fatalf("recovery ignored CURRENT publication: %+v %v", report, err)
			}
			if retry := recoveryAck(t, e, firstRequest); retry != first {
				t.Fatalf("recovery changed prior historical ACK: %+v != %+v", retry, first)
			}
			state, err := e.GetState(context.Background(), api.ReadStateRequest{Space: firstRequest.Space, Key: []byte("key")})
			want := "old"
			if committed {
				want = "new"
			}
			if err != nil || !state.Found || state.Value.Kind != api.ValueBytes || string(state.Value.Data) != want || state.Fence.LSN != manifest.Position.LSN {
				t.Fatalf("recovered state does not match actual CURRENT: %+v %v", state, err)
			}
			lookup, err := e.LookupRequest(context.Background(), recoveryLookupRequest(secondRequest))
			if err != nil {
				t.Fatal(err)
			}
			if committed {
				if lookup.Code != api.CodeDurableAck || lookup.Result.Fence.LSN != manifest.Position.LSN || lookup.Result.StateRoot != state.StateRoot || lookup.Result.RootSet != state.RootSet {
					t.Fatalf("published unknown transaction lost its receipt: %+v", lookup)
				}
				if retry := recoveryAck(t, e, secondRequest); retry != lookup.Result {
					t.Fatalf("committed unknown retry did not return original ACK: %+v != %+v", retry, lookup.Result)
				}
			} else {
				if lookup.Code != api.CodeNotFoundAtFence || lookup.Result != (api.CommitResult{}) || lookup.ObservedFence != first.Fence {
					t.Fatalf("recovery did not establish absence at verified CURRENT: %+v", lookup)
				}
				second := recoveryAck(t, e, secondRequest)
				if second.Fence.LSN != first.Fence.LSN+1 {
					t.Fatalf("verified-absent original request could not retry: %+v", second)
				}
				if retry := recoveryAck(t, e, secondRequest); retry != second {
					t.Fatal("retry changed newly committed receipt")
				}
			}
		})
	}
}

func TestEngineSemanticReplayRejectsBeforeTailRepair(t *testing.T) {
	for _, kind := range []string{"malformed-rootset", "missing-command-operations-receipt"} {
		t.Run(kind, func(t *testing.T) {
			path, d := recoveryDirectory(t)
			e, err := Create(d, Options{})
			if err != nil {
				t.Fatal(err)
			}
			recoveryAck(t, e, recoveryCommitRequest(t, "valid-first", "value", api.ValueCondition{}))
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			journal, _, err := storage.OpenJournal(d, storage.JournalOptions{})
			if err != nil {
				t.Fatal(err)
			}
			previous, err := journal.Published()
			if err != nil {
				t.Fatal(err)
			}
			tx := storage.Transaction{
				TxID:          "ed000000000000000000000000000002",
				Request:       model.RequestKey{Space: "recovery-space", Principal: "host", RequestID: "invalid-semantic-second"},
				RequestDigest: storage.Digest(sha256.Sum256([]byte("unknown command"))),
				LSN:           previous.Position.LSN + 1, PreviousDigest: previous.Position.Digest,
				Receipt:     storage.Digest(sha256.Sum256([]byte("unknown receipt"))),
				PostRootSet: storage.Digest(sha256.Sum256([]byte("unknown rootset"))),
			}
			if kind == "malformed-rootset" {
				encoded, err := codec.EncodeObject(codec.Object{Type: rootSetType, Space: systemSpace, Version: 1, Payload: corePrefix()})
				if err != nil {
					t.Fatal(err)
				}
				tx.Records = []storage.Record{{Kind: storage.KindObject, Payload: encoded}}
			}
			if _, err = journal.Publish(context.Background(), tx); err != nil {
				t.Fatalf("fixture must be physically valid: %v", err)
			}
			if err = journal.Close(); err != nil {
				t.Fatal(err)
			}
			name, prefix := recoverySegment(t, path, d)
			tail := []byte("extra unreferenced bytes must remain after semantic rejection")
			file, err := d.OpenAppend(name)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := file.Write(tail); err != nil || n != len(tail) {
				t.Fatalf("append fixture tail: %d %v", n, err)
			}
			if err = file.Sync(); err != nil {
				t.Fatal(err)
			}
			if err = file.Close(); err != nil {
				t.Fatal(err)
			}
			before := recoveryTree(t, path)
			observed := &recoveryObservedIO{DurableIO: d}
			opened, report, err := OpenExisting(observed, Options{Journal: storage.JournalOptions{TailPolicy: storage.TailQuarantine}})
			if opened != nil || err == nil || !errors.Is(err, ErrSemanticCorruption) || !errors.Is(err, storage.ErrCorruption) || errors.Is(err, storage.ErrRecoveryIncomplete) || report.PublicationSynced {
				t.Fatalf("semantically invalid physical prefix became usable or was guessed repairable: %+v %v", report, err)
			}
			if len(observed.mutations) != 0 || !reflect.DeepEqual(before, recoveryTree(t, path)) {
				t.Fatalf("semantic rejection performed recovery mutations or witness sync: %+v", observed.mutations)
			}
			_, after := recoverySegment(t, path, d)
			if !bytes.Equal(after, append(bytes.Clone(prefix), tail...)) {
				t.Fatal("semantic rejection changed published prefix or unpublished tail")
			}
			owner, err := d.AcquireOwnerLock()
			if err != nil {
				t.Fatalf("semantic rejection leaked journal owner lock: %v", err)
			}
			if err = owner.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEngineRuntimeObjectHashFailureFencesSubsequentReads(t *testing.T) {
	path, d := recoveryDirectory(t)
	e, err := Create(d, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	request := recoveryCommitRequest(t, "runtime-corruption", "authenticated-value", api.ValueCondition{})
	recoveryAck(t, e, request)
	value, err := makeValue(request.Space, api.Value{Kind: api.ValueBytes, Data: []byte("authenticated-value")})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.EncodeObject(value)
	if err != nil {
		t.Fatal(err)
	}
	name, segment := recoverySegment(t, path, d)
	objectAt := bytes.Index(segment, encoded)
	if objectAt < 0 || bytes.Index(segment[objectAt+len(encoded):], encoded) >= 0 {
		t.Fatal("test requires exactly one copy of the encoded value object")
	}
	// Deliberately violate the immutable-file contract after ACK, preserving the
	// object's envelope shape while invalidating its externally stored ObjectID.
	file, err := os.OpenFile(filepath.Join(path, filepath.FromSlash(name)), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := file.WriteAt([]byte{encoded[len(encoded)-1] ^ 1}, int64(objectAt+len(encoded)-1)); err != nil || n != 1 {
		t.Fatalf("corrupt published object: %d %v", n, err)
	}
	if err = file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := e.GetState(context.Background(), api.ReadStateRequest{Space: request.Space, Key: []byte("key")})
	if !errors.Is(err, ErrSemanticCorruption) || !errors.Is(err, storage.ErrCorruption) || !reflect.DeepEqual(got, api.StateValueResult{}) {
		t.Fatalf("runtime object mutation returned cached/unauthenticated value: %+v %v", got, err)
	}
	recoveryAssertFencedReads(t, e, request)
}
