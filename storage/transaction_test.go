package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/model"
)

func testDigest(s string) Digest { return Digest(sha256.Sum256([]byte(s))) }

func exampleTransaction() Transaction {
	return Transaction{TxID: "000102030405060708090a0b0c0d0e0f", Request: model.RequestKey{Space: "s", Principal: "p", RequestID: "r"},
		RequestDigest: testDigest("command"), LSN: 1, PreviousDigest: testDigest("genesis"),
		Records:    []Record{{KindEvent, []byte("E")}, {KindObject, []byte("O")}, {KindRevision, []byte("R")}, {KindStateOps, []byte("S")}},
		RefUpdates: []byte("ref"), ControlUpdates: []byte("ctl"), Receipt: testDigest("receipt"), PostRootSet: testDigest("roots")}
}

func mustEncode(t testing.TB, tx Transaction) ([]byte, Digest) {
	t.Helper()
	b, d, err := EncodeTransaction(tx, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b, d
}

func scannerFor(t testing.TB, b []byte, l Limits) *Scanner {
	t.Helper()
	s, err := NewScanner(bytes.NewReader(b), Position{Digest: testDigest("genesis")}, l)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestTransactionIndependentGolden(t *testing.T) {
	data, err := os.ReadFile("../testdata/journal/transaction-v0.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Encoded  string `json:"encoded_hex"`
		Digest   string `json:"commit_digest"`
		Previous string `json:"previous_digest"`
	}
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	b, d := mustEncode(t, exampleTransaction())
	if hex.EncodeToString(b) != fixture.Encoded || d.String() != fixture.Digest {
		t.Fatalf("golden mismatch: %x / %s", b, d)
	}
	if testDigest("genesis").String() != fixture.Previous {
		t.Fatal("wrong fixture starting position")
	}
	s := scannerFor(t, b, Limits{})
	got, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Transaction, exampleTransaction()) || got.CommitDigest != d || got.StartOffset != 0 || got.EndOffset != int64(len(b)) {
		t.Fatalf("bad transaction: %#v", got)
	}
	if s.Position() != (Position{1, d}) || s.LastCompleteOffset() != int64(len(b)) {
		t.Fatal("complete position not advanced atomically")
	}
	if _, err = s.Next(); err != io.EOF {
		t.Fatalf("end: %v", err)
	}
}

func TestEveryTransactionTruncationIsAtomic(t *testing.T) {
	b, _ := mustEncode(t, exampleTransaction())
	for n := 0; n < len(b); n++ {
		s := scannerFor(t, b[:n], Limits{})
		got, err := s.Next()
		if n == 0 {
			if err != io.EOF {
				t.Fatalf("empty: %v", err)
			}
			continue
		}
		if !errors.Is(err, ErrIncomplete) || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("at %d: %v", n, err)
		}
		if !reflect.DeepEqual(got, ScannedTransaction{}) || s.Position().LSN != 0 || s.LastCompleteOffset() != 0 {
			t.Fatalf("at %d published partial transaction", n)
		}
		if _, again := s.Next(); again != err {
			t.Fatalf("at %d scanner did not latch its error", n)
		}
	}
}

func unpackFrames(t testing.TB, b []byte) []Frame {
	t.Helper()
	var out []Frame
	r := bytes.NewReader(b)
	for {
		f, _, err := ReadFrame(r, Limits{})
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, f)
	}
}

func repackFrames(t testing.TB, frames []Frame) []byte {
	t.Helper()
	var b []byte
	for _, f := range frames {
		part, err := EncodeFrame(f, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		b = append(b, part...)
	}
	return b
}

func TestTransactionInvalidCorpus(t *testing.T) {
	valid, _ := mustEncode(t, exampleTransaction())
	cases := []struct {
		name   string
		mutate func([]Frame) []Frame
		want   error
	}{
		{"not-begin", func(f []Frame) []Frame { f[0].Kind = KindObject; return f }, ErrMalformed},
		{"nested-begin", func(f []Frame) []Frame { f[1].Kind = KindBegin; return f }, ErrMalformed},
		{"ordinal-gap", func(f []Frame) []Frame { f[1].Ordinal++; return f }, ErrMalformed},
		{"ordinal-repeat", func(f []Frame) []Frame { f[1].Ordinal = 0; return f }, ErrMalformed},
		{"foreign-tx", func(f []Frame) []Frame { f[2].TxID = "100102030405060708090a0b0c0d0e0f"; return f }, ErrMalformed},
		{"body-after-refs", func(f []Frame) []Frame { f[6].Kind = KindObject; return f }, ErrMalformed},
		{"missing-refs", func(f []Frame) []Frame { f[5].Kind = KindControlUpdates; return f }, ErrMalformed},
		{"duplicate-refs", func(f []Frame) []Frame { f[6].Kind = KindRefUpdates; return f }, ErrMalformed},
		{"early-commit", func(f []Frame) []Frame { f[6].Kind = KindCommit; return f }, ErrMalformed},
		{"body-changed-valid-crc", func(f []Frame) []Frame { f[1].Payload = []byte("changed"); return f }, ErrIntegrity},
		{"refs-changed-valid-crc", func(f []Frame) []Frame { f[5].Payload = []byte("changed"); return f }, ErrIntegrity},
		{"control-changed-valid-crc", func(f []Frame) []Frame { f[6].Payload = []byte("changed"); return f }, ErrIntegrity},
		{"begin-trailing-data", func(f []Frame) []Frame { f[0].Payload = append(f[0].Payload, 0); return f }, ErrMalformed},
		{"receipt-trailing-data", func(f []Frame) []Frame { f[7].Payload = append(f[7].Payload, 0); return f }, ErrMalformed},
		{"unknown-begin-schema", func(f []Frame) []Frame { f[0].Payload[0] = 2; return f }, ErrUnsupported},
		{"unknown-receipt-schema", func(f []Frame) []Frame { f[7].Payload[0] = 2; return f }, ErrUnsupported},
		{"unknown-commit-schema", func(f []Frame) []Frame { f[8].Payload[0] = 2; return f }, ErrUnsupported},
		{"commit-trailing-data", func(f []Frame) []Frame { f[8].Payload = append(f[8].Payload, 0); return f }, ErrMalformed},
	}
	for _, field := range []string{"lsn", "previous", "count", "frames", "receipt", "request", "empty-root"} {
		cases = append(cases, struct {
			name   string
			mutate func([]Frame) []Frame
			want   error
		}{"commit-" + field, func(f []Frame) []Frame {
			c, err := decodeCommit(f[8].Payload)
			if err != nil {
				t.Fatal(err)
			}
			switch field {
			case "lsn":
				c.lsn++
			case "previous":
				c.previous[0] ^= 1
			case "count":
				c.count++
			case "frames":
				c.frames[0] ^= 1
			case "receipt":
				c.receipt[0] ^= 1
			case "request":
				c.request[0] ^= 1
			case "empty-root":
				c.root = Digest{}
			}
			f[8].Payload = encodeCommit(c)
			return f
		}, ErrIntegrity})
		if field == "empty-root" {
			cases[len(cases)-1].want = ErrMalformed
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := repackFrames(t, tc.mutate(unpackFrames(t, valid)))
			s := scannerFor(t, b, Limits{})
			got, err := s.Next()
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
			if tc.want == ErrUnsupported && errors.Is(err, ErrMalformed) {
				t.Fatalf("unsupported schema misclassified as malformed: %v", err)
			}
			if !reflect.DeepEqual(got, ScannedTransaction{}) || s.Position().LSN != 0 || s.LastCompleteOffset() != 0 {
				t.Fatal("published invalid transaction")
			}
			var detail *ScanError
			if !errors.As(err, &detail) || detail.Offset < 0 || detail.Offset >= int64(len(b)) || detail.TransactionOffset != 0 {
				t.Fatalf("missing failure location: %v", err)
			}
		})
	}
}

func TestTransactionChainAndFailedSuccessor(t *testing.T) {
	a, ad := mustEncode(t, exampleTransaction())
	btx := exampleTransaction()
	btx.TxID = "100102030405060708090a0b0c0d0e0f"
	btx.Request.RequestID = "r2"
	btx.LSN = 2
	btx.PreviousDigest = ad
	b, bd := mustEncode(t, btx)
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprintf("corrupt=%t", corrupt), func(t *testing.T) {
			input := append(bytes.Clone(a), b...)
			if corrupt {
				input[len(a)+HeaderSize] ^= 1
			}
			s := scannerFor(t, input, Limits{})
			if _, err := s.Next(); err != nil {
				t.Fatal(err)
			}
			got, err := s.Next()
			if corrupt {
				if !errors.Is(err, ErrIntegrity) || got.LSN != 0 || s.Position() != (Position{1, ad}) || s.LastCompleteOffset() != int64(len(a)) {
					t.Fatalf("corrupt successor changed position: %v", err)
				}
			} else if err != nil || got.CommitDigest != bd || got.StartOffset != int64(len(a)) || s.Position() != (Position{2, bd}) {
				t.Fatalf("chain failed: %v", err)
			}
		})
	}
	// A repeated complete transaction is not another commit at the next LSN.
	s := scannerFor(t, append(bytes.Clone(a), a...), Limits{})
	if _, err := s.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Next(); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("duplicate transaction: %v", err)
	}
}

func TestChangedFinalRootChangesCommitDigest(t *testing.T) {
	b, original := mustEncode(t, exampleTransaction())
	f := unpackFrames(t, b)
	c, err := decodeCommit(f[8].Payload)
	if err != nil {
		t.Fatal(err)
	}
	c.root[0] ^= 1
	f[8].Payload = encodeCommit(c)
	s := scannerFor(t, repackFrames(t, f), Limits{})
	got, err := s.Next()
	if err != nil {
		t.Fatal(err)
	}
	if got.CommitDigest == original {
		t.Fatal("root is not bound by the final digest")
	}
	// Accepting this structurally valid final frame does NOT authenticate it:
	// recovery must compare its digest with CURRENT/manifest's published tip.
}

func TestTransactionLimitsAndEncodeValidation(t *testing.T) {
	b, _ := mustEncode(t, exampleTransaction())
	for _, l := range []Limits{{MaxRecords: 8}, {MaxTransactionBytes: uint64(len(b) - 1)}, {MaxPayload: 32}} {
		if out, _, err := EncodeTransaction(exampleTransaction(), l); !errors.Is(err, ErrLimit) || out != nil {
			t.Fatalf("encode limit: %v", err)
		}
		if _, err := scannerFor(t, b, l).Next(); !errors.Is(err, ErrLimit) {
			t.Fatalf("scan limit: %v", err)
		}
	}
	for _, l := range []Limits{{MaxRecords: MaxRecords + 1}, {MaxPayload: MaxPayload + 1}, {MaxTransactionBytes: MaxTransactionBytes + 1}} {
		if _, err := NewScanner(bytes.NewReader(b), Position{Digest: testDigest("genesis")}, l); !errors.Is(err, ErrLimit) {
			t.Fatalf("bad ceiling: %v", err)
		}
	}
	for _, mutate := range []func(*Transaction){func(tx *Transaction) { tx.TxID = "bad" }, func(tx *Transaction) { tx.Request.Space = "" }, func(tx *Transaction) { tx.LSN = 0 }, func(tx *Transaction) { tx.PreviousDigest = Digest{} }, func(tx *Transaction) { tx.RequestDigest = Digest{} }, func(tx *Transaction) { tx.Receipt = Digest{} }, func(tx *Transaction) { tx.PostRootSet = Digest{} }, func(tx *Transaction) { tx.Records[0].Kind = KindCommit }} {
		tx := exampleTransaction()
		mutate(&tx)
		if out, _, err := EncodeTransaction(tx, Limits{}); !errors.Is(err, ErrMalformed) || out != nil {
			t.Fatalf("invalid transaction encoded: %v", err)
		}
	}
	if _, err := NewScanner(nil, Position{Digest: testDigest("genesis")}, Limits{}); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
	if _, err := NewScanner(bytes.NewReader(b), Position{}, Limits{}); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
	s, err := NewScanner(bytes.NewReader(b), Position{LSN: math.MaxUint64, Digest: testDigest("genesis")}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Next(); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("LSN overflow: %v", err)
	}
}

func TestScannerDoesNotReadBeyondRemainingHeaderBudget(t *testing.T) {
	b, _ := mustEncode(t, exampleTransaction())
	first, err := EncodeFrame(unpackFrames(t, b)[0], Limits{})
	if err != nil {
		t.Fatal(err)
	}
	r := bytes.NewReader(b)
	s, err := NewScanner(r, Position{Digest: testDigest("genesis")}, Limits{MaxTransactionBytes: uint64(len(first) + 1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Next(); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if len(b)-r.Len() != len(first) || s.Offset() != int64(len(first)) {
		t.Fatal("read the next header past the remaining transaction budget")
	}
}

type failedReader struct{ err error }

func (r failedReader) Read([]byte) (int, error) { return 0, r.err }

func TestScannerPreservesIOError(t *testing.T) {
	s, err := NewScanner(failedReader{io.ErrClosedPipe}, Position{Digest: testDigest("genesis")}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Next(); !errors.Is(err, io.ErrClosedPipe) || errors.Is(err, ErrIncomplete) {
		t.Fatalf("IO error misclassified: %v", err)
	}
}

func TestMinimalTransactionAtExactLimits(t *testing.T) {
	tx := exampleTransaction()
	tx.Records = nil
	tx.RefUpdates = nil
	tx.ControlUpdates = nil
	b, digest := mustEncode(t, tx)
	limits := Limits{MaxRecords: 5, MaxTransactionBytes: uint64(len(b))}
	encoded, gotDigest, err := EncodeTransaction(tx, limits)
	if err != nil || !bytes.Equal(encoded, b) || gotDigest != digest {
		t.Fatalf("exact encode limits rejected: %v", err)
	}
	s := scannerFor(t, b, limits)
	got, err := s.Next()
	if err != nil || got.CommitDigest != digest || len(got.Records) != 0 {
		t.Fatalf("minimum transaction: %v", err)
	}
	if _, err = s.Next(); err != io.EOF {
		t.Fatalf("EOF after exact budget: %v", err)
	}
}

func TestRemainingBudgetBoundaries(t *testing.T) {
	b, _ := mustEncode(t, exampleTransaction())
	first, err := EncodeFrame(unpackFrames(t, b)[0], Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, remaining := range []int{47, 48, 55, 56, 63, 64} {
		r := bytes.NewReader(b)
		budget := len(first) + remaining
		s, err := NewScanner(r, Position{Digest: testDigest("genesis")}, Limits{MaxTransactionBytes: uint64(budget)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Next(); !errors.Is(err, ErrLimit) {
			t.Fatalf("budget %d: %v", remaining, err)
		}
		if len(b)-r.Len() > budget || s.Offset() != int64(len(b)-r.Len()) {
			t.Fatalf("read past budget %d", remaining)
		}
	}
}

func TestLastLegalLSN(t *testing.T) {
	tx := exampleTransaction()
	tx.LSN = math.MaxUint64
	b, digest := mustEncode(t, tx)
	s, err := NewScanner(bytes.NewReader(b), Position{LSN: math.MaxUint64 - 1, Digest: tx.PreviousDigest}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Next()
	if err != nil || got.LSN != math.MaxUint64 || s.Position() != (Position{math.MaxUint64, digest}) {
		t.Fatalf("last LSN rejected: %v", err)
	}
	if _, err = s.Next(); err != io.EOF {
		t.Fatal(err)
	}
}

type errorAfterReader struct {
	data      []byte
	remaining int
}

func (r *errorAfterReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.ErrClosedPipe
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	r.remaining -= n
	if r.remaining == 0 {
		return n, io.ErrClosedPipe
	}
	return n, nil
}

func TestPartialReadWithErrorPreservesOffsetAndCause(t *testing.T) {
	b, _ := mustEncode(t, exampleTransaction())
	for _, at := range []int{HeaderSize - 1, HeaderSize + 5} {
		r := &errorAfterReader{data: b, remaining: at}
		s, err := NewScanner(r, Position{Digest: testDigest("genesis")}, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Next(); !errors.Is(err, io.ErrClosedPipe) || errors.Is(err, ErrIncomplete) {
			t.Fatalf("at %d: %v", at, err)
		}
		if s.Offset() != int64(at) || s.Position().LSN != 0 || s.LastCompleteOffset() != 0 {
			t.Fatal("incorrect error offset or partial publication")
		}
	}
}

func FuzzTransactionScanner(f *testing.F) {
	b, _ := mustEncode(f, exampleTransaction())
	f.Add(b)
	f.Add(b[:len(b)/2])
	f.Add([]byte{})
	f.Add([]byte("ATEFRM01"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			return
		}
		s := scannerFor(t, data, Limits{MaxPayload: 4096, MaxTransactionBytes: 16384, MaxRecords: 64})
		previous := s.Position()
		for {
			got, err := s.Next()
			if s.Offset() < 0 || s.Offset() > int64(len(data)) {
				t.Fatal("invalid byte offset")
			}
			if err != nil {
				if s.Position() != previous || !reflect.DeepEqual(got, ScannedTransaction{}) {
					t.Fatal("partial publication")
				}
				return
			}
			if got.LSN != previous.LSN+1 {
				t.Fatal("non-contiguous LSN")
			}
			encoded, d := mustEncode(t, got.Transaction)
			if !bytes.Equal(encoded, data[got.StartOffset:got.EndOffset]) || d != got.CommitDigest {
				t.Fatal("successful parse is not canonical")
			}
			previous = s.Position()
		}
	})
}
