package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandUsage(t *testing.T) {
	for _, tc := range []struct {
		args   []string
		code   int
		want   string
		stderr bool
	}{
		{nil, 0, "usage:", false}, {[]string{"help"}, 0, "usage:", false}, {[]string{"version"}, 0, version, false},
		{[]string{"nonsense"}, 2, "unknown command", true}, {[]string{"version", "extra"}, 2, "no arguments", true},
		{[]string{"check-journal"}, 2, "previous-digest", true}, {[]string{"check-journal", "-h"}, 0, "usage:", true},
		{[]string{"check-journal", "--previous-digest", "bad", "file"}, 2, "usage:", true},
	} {
		var out, errOut bytes.Buffer
		code := run(tc.args, &out, &errOut)
		text := out.String()
		if tc.stderr {
			text = errOut.String()
		}
		if code != tc.code || !strings.Contains(text, tc.want) {
			t.Fatalf("%v: %d %q %q", tc.args, code, out.String(), errOut.String())
		}
	}
}

func TestCheckJournalDoesNotClaimPublication(t *testing.T) {
	fixture, err := os.ReadFile("../../testdata/journal/transaction-v0.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Encoded  string `json:"encoded_hex"`
		Digest   string `json:"commit_digest"`
		Previous string `json:"previous_digest"`
	}
	if err = json.Unmarshal(fixture, &golden); err != nil {
		t.Fatal(err)
	}
	b, err := hex.DecodeString(golden.Encoded)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "raw.journal")
	if err = os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"check-journal", "--previous-digest", golden.Previous, p}
	var out, errOut bytes.Buffer
	if code := run(args, &out, &errOut); code != 0 {
		t.Fatalf("%d: %s", code, errOut.String())
	}
	var result struct {
		Transactions int    `json:"transactions"`
		Digest       string `json:"commit_digest"`
		Published    bool   `json:"publication_verified"`
	}
	if err = json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Transactions != 1 || result.Digest != golden.Digest || result.Published {
		t.Fatalf("wrong evidence: %s", out.String())
	}
	// A corrupt stream must fail and must not print a successful summary.
	b[len(b)/2] ^= 1
	if err = os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	errOut.Reset()
	if code := run(args, &out, &errOut); code != 1 || out.Len() != 0 || errOut.Len() == 0 {
		t.Fatalf("corruption accepted: %d %s %s", code, out.String(), errOut.String())
	}
}
