package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const fingerprintPolicy = "ate-source-v2"

// Explicit roots keep local stores, results and credentials out of the scan.
// New production roots must be added here before relying on this evidence.
const sourceRoots = "agentprofile agentstore annotation api client cmd codec engine eventlog formats graph graphstore investigation journalstore knowledge memory model observation protocol reference service state storage transfer tests scripts testdata"

type sourceFile struct {
	Path   string `json:"path"`
	Mode   uint32 `json:"mode"`
	SHA256 string `json:"sha256"`
}

type fingerprint struct {
	Policy string       `json:"policy"`
	HEAD   string       `json:"head"`
	Dirty  bool         `json:"dirty"`
	SHA256 string       `json:"sha256"`
	Files  []sourceFile `json:"files"`
}

func git(root string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s failed (output withheld)", args[0])
	}
	return out, nil
}

func allowed(path string) bool {
	parts := strings.Split(filepath.ToSlash(path), "/")
	fixture := false
	for _, p := range parts {
		lower := strings.ToLower(p)
		if strings.HasPrefix(p, ".") || lower == "credentials" || lower == "secrets" ||
			lower == "runtime" || lower == "results" || lower == "generated" ||
			lower == "node_modules" || lower == "vendor" ||
			strings.HasSuffix(lower, ".pem") || strings.HasSuffix(lower, ".key") ||
			strings.HasPrefix(lower, "id_rsa") || strings.HasPrefix(lower, "id_ed25519") {
			return false
		}
		if p == "testdata" {
			fixture = true
		}
	}
	if len(parts) > 1 && !strings.Contains(" "+sourceRoots+" ", " "+parts[0]+" ") {
		return false
	}
	if fixture {
		return true
	}
	// Runtime go:embed input outside testdata; do not allow arbitrary JSON files.
	if filepath.ToSlash(path) == "formats/registry.json" {
		return true
	}
	switch filepath.Ext(path) {
	case ".go", ".s", ".S", ".c", ".h", ".cc", ".cpp", ".syso", ".sh", ".py":
		return true
	}
	return path == "go.mod" || path == "go.sum" || path == "go.work" ||
		path == "go.work.sum" || path == "Makefile"
}

func snapshot(root string) (fingerprint, error) {
	f := fingerprint{Policy: fingerprintPolicy, Files: []sourceFile{}}
	head, err := git(root, "rev-parse", "--verify", "HEAD")
	if err != nil {
		return f, err
	}
	f.HEAD = strings.TrimSpace(string(head))
	status, err := git(root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return f, err
	}
	f.Dirty = len(status) != 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			// A hypothetical Go child applies the same exclusion policy to dirs.
			if !allowed(rel + "/probe.go") {
				return filepath.SkipDir
			}
			return nil
		}
		if !allowed(rel) {
			// A symlink might conceal an entire allowed source directory.
			if d.Type()&os.ModeSymlink != 0 && allowed(rel+"/probe.go") {
				return fmt.Errorf("source directory symlink rejected: %s", rel)
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("non-regular source rejected: %s", rel)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, input)
		closeErr := input.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		f.Files = append(f.Files, sourceFile{rel, uint32(info.Mode().Perm()), hex.EncodeToString(h.Sum(nil))})
		return nil
	})
	if err != nil {
		return f, err
	}
	sort.Slice(f.Files, func(i, j int) bool { return f.Files[i].Path < f.Files[j].Path })
	// JSON framing avoids ambiguity in paths containing whitespace or newlines.
	payload, err := json.Marshal(struct {
		Policy string
		HEAD   string
		Files  []sourceFile
	}{f.Policy, f.HEAD, f.Files})
	if err != nil {
		return f, err
	}
	sum := sha256.Sum256(payload)
	f.SHA256 = hex.EncodeToString(sum[:])
	return f, nil
}
