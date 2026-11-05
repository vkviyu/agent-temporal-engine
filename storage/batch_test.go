package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func batchChain(t testing.TB, old Publication, first, count int) ([]Transaction, []*PreparedTransaction) {
	t.Helper()
	txs := make([]Transaction, count)
	prepared := make([]*PreparedTransaction, count)
	for i := range txs {
		txs[i] = faultNextTransaction(old, first+i)
		var err error
		prepared[i], err = PrepareTransaction(txs[i], Limits{})
		if err != nil {
			t.Fatal(err)
		}
		old.Position = prepared[i].Position()
	}
	return txs, prepared
}

func batchTip(old Publication, batch []*PreparedTransaction) Publication {
	old.Generation++
	for _, p := range batch {
		old.Position, old.PostRootSet = p.position, p.postRootSet
		old.PublishedEnd += p.Size()
	}
	return old
}

func TestPreparedTransactionOwnsEncodingAndMetadata(t *testing.T) {
	f, j, previous, _ := faultSeedAcknowledged(t)
	defer j.Close()
	tx := faultNextTransaction(previous, 2)
	tx.Records = []Record{{KindObject, []byte("original object")}, {KindEvent, []byte("original event")}}
	tx.RefUpdates, tx.ControlUpdates = []byte("refs"), []byte("control")
	encoded, digest, err := EncodeTransaction(tx, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := PrepareTransaction(tx, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	wantRoot, wantRequest, wantReceipt := tx.PostRootSet, tx.Request, tx.Receipt
	for _, record := range tx.Records {
		for i := range record.Payload {
			record.Payload[i] = 'x'
		}
	}
	tx.Records[0] = Record{KindRevision, []byte("replacement")}
	tx.RefUpdates[0], tx.ControlUpdates[0] = 'x', 'x'
	tx.TxID, tx.LSN, tx.PreviousDigest = model.TxID(strings.Repeat("f", 32)), 99, testDigest("changed")
	tx.Request.RequestID, tx.RequestDigest, tx.Receipt, tx.PostRootSet = "changed", testDigest("changed"), testDigest("changed"), testDigest("changed")
	if !bytes.Equal(p.encoded, encoded) || p.Position() != (Position{LSN: previous.Position.LSN + 1, Digest: digest}) || p.Size() != uint64(len(encoded)) || p.request != wantRequest || p.receipt != wantReceipt {
		t.Fatal("preparation retained caller storage or changed metadata")
	}
	result, err := j.PublishPreparedBatch(context.Background(), []*PreparedTransaction{p})
	if err != nil || result.Publication.PostRootSet != wantRoot {
		t.Fatalf("publication=%+v err=%v", result, err)
	}
	_, data := faultSegmentBytes(t, f)
	if !bytes.Equal(data[previous.PublishedEnd:result.Publication.PublishedEnd], encoded) {
		t.Fatal("published bytes changed after preparation")
	}
	if (*PreparedTransaction)(nil).Size() != 0 || (*PreparedTransaction)(nil).Position() != (Position{}) {
		t.Fatal("nil getters are not values")
	}
}

func TestPreparedBatchPublishesOneP1WithOwnProofs(t *testing.T) {
	f, j, old, first := faultSeedAcknowledged(t)
	defer j.Close()
	txs, batch := batchChain(t, old, 2, 3)
	result, err := j.PublishPreparedBatch(context.Background(), batch)
	if err != nil || result.Publication != batchTip(old, batch) || result.Publication != faultPublished(t, j) || len(result.Transactions) != len(batch) {
		t.Fatalf("batch result=%+v err=%v", result, err)
	}
	segmentSyncs, currentInstalls, rootSyncs, manifestInstalls := 0, 0, 0, 0
	for _, call := range f.calls {
		if call.Op == "Sync" && strings.HasPrefix(call.Name, "segments/") {
			segmentSyncs++
		}
		if call.Op == "ReplaceAtomic" && call.To == "CURRENT" {
			currentInstalls++
		}
		if call.Op == "InstallNew" && strings.HasPrefix(call.To, "manifests/") {
			manifestInstalls++
		}
		if call.Op == "SyncDir" && call.Name == "." {
			rootSyncs++
		}
	}
	if segmentSyncs != 1 || currentInstalls != 1 || manifestInstalls != 1 || rootSyncs != 1 {
		t.Fatalf("P1 repeated: segmentSyncs=%d currentInstalls=%d manifests=%d rootSyncs=%d trace=%+v", segmentSyncs, currentInstalls, manifestInstalls, rootSyncs, f.calls)
	}
	end := old.PublishedEnd
	_, data := faultSegmentBytes(t, f)
	scanner, err := NewScanner(bytes.NewReader(data[old.PublishedEnd:]), old.Position, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for i, own := range result.Transactions {
		start := end
		end += batch[i].Size()
		if own.Publication.Generation != old.Generation+1 || own.Publication.Position != batch[i].Position() || own.Publication.PostRootSet != txs[i].PostRootSet || own.Publication.PublishedEnd != end || own.Publication.Fence().LSN != txs[i].LSN {
			t.Fatalf("transaction %d got final tip instead of own proof: %+v", i, own)
		}
		scanned, err := scanner.Next()
		if err != nil || !reflect.DeepEqual(scanned.Transaction, txs[i]) {
			t.Fatalf("transaction %d scan=%+v err=%v", i, scanned, err)
		}
		for n := range scanned.RecordLocations {
			scanned.RecordLocations[n].PayloadOffset += int64(old.PublishedEnd)
		}
		if !reflect.DeepEqual(scanned.RecordLocations, own.RecordLocations) {
			t.Fatalf("transaction %d location disagreement: %+v %+v", i, own.RecordLocations, scanned.RecordLocations)
		}
		for n, location := range own.RecordLocations {
			if location.PayloadOffset < int64(start) || location.PayloadOffset+int64(location.PayloadLength) > int64(end) {
				t.Fatalf("transaction %d location outside own interval", i)
			}
			body := make([]byte, location.PayloadLength)
			if _, err := j.ReadAt(body, location.PayloadOffset); err != nil || !bytes.Equal(body, txs[i].Records[n].Payload) {
				t.Fatalf("transaction %d body mismatch: %v", i, err)
			}
		}
	}
	if _, err := scanner.Next(); err != io.EOF {
		t.Fatal(err)
	}
	faultAssertPublishedPrefix(t, f, result.Publication, append([]Transaction{first}, txs...))
	reboot := f.crash()
	recovered, report, err := OpenJournal(reboot, JournalOptions{})
	if err != nil || report.Publication != result.Publication || report.ScannedTransactions != 4 {
		t.Fatalf("batch recovery=%+v err=%v", report, err)
	}
	defer recovered.Close()
}

func TestPreparedBatchEveryMutationReturnsUnknownWithoutPartialACK(t *testing.T) {
	f, j, old, _ := faultSeedAcknowledged(t)
	_, batch := batchChain(t, old, 2, 3)
	if _, err := j.PublishPreparedBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	trace := append([]faultCall(nil), f.calls...)
	_ = j.Close()
	for _, call := range trace {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%02d-%s/after=%t", call.Step, call.Op, after), func(t *testing.T) {
				f, j, old, first := faultSeedAcknowledged(t)
				txs, batch := batchChain(t, old, 2, 3)
				want := batchTip(old, batch)
				f.failAt(call.Step, after)
				result, err := j.PublishPreparedBatch(context.Background(), batch)
				if !reflect.DeepEqual(result, PublishedBatch{}) || !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, errFaultIO) {
					t.Fatalf("partial ACK or lost uncertainty: %+v %v", result, err)
				}
				var publicationErr *PublicationError
				if !errors.As(err, &publicationErr) || publicationErr.TxID != txs[0].TxID || !reflect.DeepEqual(publicationErr.TxIDs, []model.TxID{txs[0].TxID, txs[1].TxID, txs[2].TxID}) {
					t.Fatalf("missing transaction diagnostics: %v", err)
				}
				if status := j.Status(); status.State != JournalFenced || status.LastPublication != old {
					t.Fatalf("lost old ACK: %+v", status)
				}
				calls := len(f.calls)
				if _, err := j.PublishPreparedBatch(context.Background(), batch); !errors.Is(err, ErrStoreFenced) || len(f.calls) != calls {
					t.Fatal("fenced batch performed IO")
				}
				reboot := f.crash()
				recovered, report, err := OpenJournal(reboot, JournalOptions{})
				if errors.Is(err, ErrRecoveryRequired) {
					if report.Publication != old {
						t.Fatalf("unpublished complete tail promoted: %+v", report)
					}
					recovered, report, err = OpenJournal(reboot, JournalOptions{TailPolicy: TailQuarantine})
				}
				if err != nil || (report.Publication != old && report.Publication != want) {
					t.Fatalf("partial crash outcome: %+v %v", report, err)
				}
				if call.Step == trace[len(trace)-1].Step && after && report.Publication != want {
					t.Fatal("final sync did not persist full batch")
				}
				acknowledged := []Transaction{first}
				if report.Publication == want {
					acknowledged = append(acknowledged, txs...)
				}
				faultAssertPublishedPrefix(t, reboot, report.Publication, acknowledged)
				_ = recovered.Close()
			})
		}
	}
}

func TestPreparedBatchPreflightDoesNotWrite(t *testing.T) {
	for _, name := range []string{"empty", "nil", "zero", "count", "chain", "duplicate-batch", "duplicate-committed", "bytes-quota", "count-quota", "generation-overflow", "lsn-overflow", "prepared-bytes-limit", "prepared-payload-limit", "prepared-record-limit", "canceled"} {
		t.Run(name, func(t *testing.T) {
			f, j, old, first := faultSeedAcknowledged(t)
			defer j.Close()
			txs, batch := batchChain(t, old, 2, 3)
			ctx := context.Background()
			want := ErrLimit
			switch name {
			case "empty":
				batch, want = nil, ErrMalformed
			case "nil":
				batch[1], want = nil, ErrMalformed
			case "zero":
				batch[1], want = &PreparedTransaction{}, ErrMalformed
			case "count":
				batch = make([]*PreparedTransaction, MaxPreparedBatchTransactions+1)
			case "chain":
				txs[2].PreviousDigest = old.Position.Digest
				batch[2], _ = PrepareTransaction(txs[2], Limits{})
				want = ErrPrecondition
			case "duplicate-batch":
				txs[2].TxID = txs[1].TxID
				batch[2], _ = PrepareTransaction(txs[2], Limits{})
				want = ErrDuplicateTransaction
			case "duplicate-committed":
				txs[0].TxID = first.TxID
				batch[0], _ = PrepareTransaction(txs[0], Limits{})
				batch = batch[:1]
				want = ErrDuplicateTransaction
			case "bytes-quota":
				j.options.MaxPublishedBytes = old.PublishedEnd + batch[0].Size()
			case "count-quota":
				j.options.MaxPublishedTransactions = old.Position.LSN + 2
			case "generation-overflow":
				j.publication.Generation = math.MaxUint64
			case "lsn-overflow":
				j.publication.Position.LSN = math.MaxUint64 - 1
			case "prepared-bytes-limit":
				j.options.Limits.MaxTransactionBytes = batch[0].Size() - 1
			case "prepared-payload-limit":
				j.options.Limits.MaxPayload = 1
			case "prepared-record-limit":
				j.options.Limits.MaxRecords = 1
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			}
			before := j.publication
			result, err := j.PublishPreparedBatch(ctx, batch)
			if !errors.Is(err, want) || !reflect.DeepEqual(result, PublishedBatch{}) || len(f.calls) != 0 || j.Status().State != JournalReady || j.Status().LastPublication != before {
				t.Fatalf("preflight wrote/fenced/ACKed: result=%+v err=%v calls=%+v", result, err, f.calls)
			}
		})
	}
}

func TestPreparedBatchCancellationAfterFirstWriteDoesNotStopP1(t *testing.T) {
	f, j, old, _ := faultSeedAcknowledged(t)
	defer j.Close()
	_, batch := batchChain(t, old, 2, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "Write" && strings.HasPrefix(b.Name, "segments/") && b.After {
			cancel()
		}
		return nil
	}
	result, err := j.PublishPreparedBatch(ctx, batch)
	if err != nil || ctx.Err() != context.Canceled || result.Publication != batchTip(old, batch) {
		t.Fatalf("cancellation stopped P1: %+v %v", result, err)
	}
}

func TestPreparedBatchHardByteCapRejectsBeforeAnyWrite(t *testing.T) {
	f, j, old, _ := faultSeedAcknowledged(t)
	defer j.Close()
	// Each individual encoding fits comfortably within its 64 MiB ceiling;
	// their combined canonical lengths must still fit the batch ceiling.
	payload := bytes.Repeat([]byte("x"), 1<<20)
	var batch []*PreparedTransaction
	var total uint64
	for n := 2; total <= MaxPreparedBatchBytes; n++ {
		tx := faultNextTransaction(old, n)
		tx.Records = []Record{{KindObject, payload}}
		p, err := PrepareTransaction(tx, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		batch = append(batch, p)
		total += p.Size()
		old.Position = p.Position()
	}
	result, err := j.PublishPreparedBatch(context.Background(), batch)
	if !errors.Is(err, ErrLimit) || !reflect.DeepEqual(result, PublishedBatch{}) || len(f.calls) != 0 || j.Status().State != JournalReady {
		t.Fatalf("oversize batch performed IO or ACKed: %+v %v", result, err)
	}
}

type batchSizeFile struct {
	DurableFile
	size int64
	err  error
}

func (f batchSizeFile) Size() (int64, error) { return f.size, f.err }

func TestPreparedBatchSizePreflightFencesWithoutCommitUnknown(t *testing.T) {
	for _, name := range []string{"negative", "changed", "io-error", "cancel-on-size"} {
		t.Run(name, func(t *testing.T) {
			f, j, old, _ := faultSeedAcknowledged(t)
			defer j.Close()
			_, batch := batchChain(t, old, 2, 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want, state := ErrStoreFenced, JournalFenced
			switch name {
			case "negative":
				j.segment = batchSizeFile{DurableFile: j.segment, size: -1}
			case "changed":
				j.segment = batchSizeFile{DurableFile: j.segment, size: int64(old.PublishedEnd) + 1}
			case "io-error":
				j.segment = batchSizeFile{DurableFile: j.segment, err: errFaultIO}
			case "cancel-on-size":
				j.segment = cancelOnSizeFile{j.segment, cancel}
				want, state = context.Canceled, JournalReady
			}
			result, err := j.PublishPreparedBatch(ctx, batch)
			if !errors.Is(err, want) || errors.Is(err, ErrCommitUnknown) || !reflect.DeepEqual(result, PublishedBatch{}) || len(f.calls) != 0 || j.Status().State != state || j.Status().LastPublication != old {
				t.Fatalf("size preflight result=%+v err=%v status=%+v calls=%+v", result, err, j.Status(), f.calls)
			}
		})
	}
}

func TestPreparedBatchAcceptsCountCeiling(t *testing.T) {
	f, j, old, _ := faultSeedAcknowledged(t)
	defer j.Close()
	_, batch := batchChain(t, old, 2, MaxPreparedBatchTransactions)
	result, err := j.PublishPreparedBatch(context.Background(), batch)
	if err != nil || result.Publication != batchTip(old, batch) || len(result.Transactions) != MaxPreparedBatchTransactions {
		t.Fatalf("ceiling batch=%+v err=%v", result.Publication, err)
	}
	segmentSyncs := 0
	for _, call := range f.calls {
		if call.Op == "Sync" && strings.HasPrefix(call.Name, "segments/") {
			segmentSyncs++
		}
	}
	if segmentSyncs != 1 {
		t.Fatalf("count ceiling batch synced segment %d times", segmentSyncs)
	}
}

func TestPreparedBatchCompleteUnpublishedTailQuarantinesWithSmallTransactionLimit(t *testing.T) {
	f := newFaultIO()
	options := JournalOptions{Limits: Limits{MaxTransactionBytes: 2048}}
	j, err := CreateJournal(f, options)
	if err != nil {
		t.Fatal(err)
	}
	first := faultNextTransaction(faultPublished(t, j), 1)
	old, err := j.Publish(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	f.resetFaults()
	_, batch := batchChain(t, old, 2, 4)
	var tail []byte
	for _, p := range batch {
		if p.Size() > options.Limits.MaxTransactionBytes {
			t.Fatal("test transaction exceeds its own limit")
		}
		tail = append(tail, p.encoded...)
	}
	if uint64(len(tail)) <= options.Limits.MaxTransactionBytes {
		t.Fatal("test batch must exceed single-transaction limit")
	}
	f.onBoundary = func(b faultBoundary) error {
		if b.Op == "Sync" && strings.HasPrefix(b.Name, "segments/") && b.After {
			return errFaultIO
		}
		return nil
	}
	result, err := j.PublishPreparedBatch(context.Background(), batch)
	if !errors.Is(err, ErrCommitUnknown) || !reflect.DeepEqual(result, PublishedBatch{}) {
		t.Fatalf("expected whole unpublished tail: %+v %v", result, err)
	}
	reboot := f.crash()
	before := faultTreeSnapshot(t, reboot)
	recovered, report, err := OpenJournal(reboot, options)
	if recovered != nil || !errors.Is(err, ErrRecoveryRequired) || report.Publication != old || report.TailBytes != uint64(len(tail)) || !reflect.DeepEqual(before, faultTreeSnapshot(t, reboot)) {
		t.Fatalf("complete unpublished batch was promoted or altered: %+v %v", report, err)
	}
	options.TailPolicy = TailQuarantine
	recovered, report, err = OpenJournal(reboot, options)
	if err != nil || report.Publication != old || report.QuarantineFile == "" || !bytes.Equal([]byte(faultReadAll(t, reboot, report.QuarantineFile)), tail) {
		t.Fatalf("whole batch tail could not be quarantined under small transaction limit: %+v %v", report, err)
	}
	faultAssertPublishedPrefix(t, reboot, faultPublished(t, recovered), []Transaction{first})
	_ = recovered.Close()
	reboot = reboot.crash()
	recovered, report, err = OpenJournal(reboot, options)
	if err != nil || report.Publication != old || report.TailBytes != 0 {
		t.Fatalf("quarantine was not durable: %+v %v", report, err)
	}
	_ = recovered.Close()
}

func TestNormalizeJournalOptionsIsPureValidatedDefaulting(t *testing.T) {
	input := JournalOptions{}
	got, err := NormalizeJournalOptions(input)
	if err != nil || input != (JournalOptions{}) || got.Limits != DefaultLimits() || got.MaxPublishedBytes != DefaultMaxPublishedBytes || got.MaxPublishedTransactions != DefaultMaxPublishedTransactions || got.TailPolicy != TailReject {
		t.Fatalf("normalization=%+v err=%v", got, err)
	}
	if _, err := NormalizeJournalOptions(JournalOptions{MaxPublishedBytes: MaxPublishedBytes + 1}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := NormalizeJournalOptions(JournalOptions{TailPolicy: "invalid"}); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
}
