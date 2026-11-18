// Command verify provides the deliberately small R09 local/CI entry point.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type skip struct {
	Classification string `json:"classification"`
	PolicyID       string `json:"policy_id"`
	Lane           string `json:"lane,omitempty"`
	Package        string `json:"package,omitempty"`
	Test           string `json:"test,omitempty"`
	Reason         string `json:"reason"`
	ReasonHash     string `json:"reason_sha256,omitempty"`
	Log            string `json:"log,omitempty"`
}

type result struct {
	Name     string    `json:"name"`
	Command  []string  `json:"command"`
	Status   string    `json:"status"`
	ExitCode int       `json:"exit_code"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Log      string    `json:"log,omitempty"`
	Skips    []skip    `json:"skips,omitempty"`
}

type manifest struct {
	Schema   int          `json:"schema"`
	Task     string       `json:"task"`
	Mode     string       `json:"mode"`
	Status   string       `json:"status"`
	Go       string       `json:"go"`
	OS       string       `json:"os"`
	Arch     string       `json:"arch"`
	Report   string       `json:"report"`
	Started  time.Time    `json:"started"`
	Finished time.Time    `json:"finished"`
	Start    *fingerprint `json:"start,omitempty"`
	End      *fingerprint `json:"end,omitempty"`
	Stable   bool         `json:"source_stable"`
	Results  []result     `json:"results"`
	Skipped  []skip       `json:"skipped_lanes"`
	Error    string       `json:"error,omitempty"`
}

type step struct {
	name    string
	command []string
	test    bool
}

func plan(mode string) []step {
	if mode == "fingerprint" {
		return nil
	}
	steps := []step{
		{"format", []string{"internal:format-changed-go"}, false},
		{"diff", []string{"internal:diff-check"}, false},
		{"vet", []string{"go", "vet", "./..."}, false},
		{"build", []string{"go", "build", "./..."}, false},
	}
	if mode == "full" {
		steps = append(steps, step{"test", []string{"go", "test", "-count=1", "-json", "./..."}, true})
	}
	if mode == "race" {
		steps = append(steps, step{"race", []string{"go", "test", "-race", "-count=1", "-json", "./..."}, true})
	}
	if mode == "benchmark" {
		steps = append(steps, step{"benchmark", []string{
			"go", "test", "-json", "./tests/benchmarks", "-run", "^$",
			"-bench", "^(BenchmarkNativeStore|BenchmarkKnowledge|BenchmarkContextSelection|BenchmarkNativeScale)$",
			"-benchtime=1x", "-count=1", "-cpu=1", "-benchmem",
		}, true})
	}
	if mode == "native" {
		steps = append(steps, step{"native", []string{
			"go", "test", "-count=1", "-json", "./storage",
			"-run", "^(TestJournalNative|TestNative)",
		}, true})
	}
	return steps
}

func main() {
	os.Exit(cli(os.Args[1:]))
}

func cli(args []string) int {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	report := flags.String("report", "", "new report directory outside repository (default: unique OS temp directory)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 || !validMode(flags.Arg(0)) {
		fmt.Fprintln(os.Stderr, "usage: go run ./scripts/verify [-report /absolute/new-directory] quick|full|race|benchmark|native|fingerprint")
		return 2
	}
	rootBytes, err := git("", "rev-parse", "--show-toplevel")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	root, err := filepath.EvalSymlinks(strings.TrimSpace(string(rootBytes)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	dir, err := reportDir(root, *report)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Println("report:", dir)
	code := verify(root, dir, flags.Arg(0), plan(flags.Arg(0)))
	fmt.Println("exit:", code, "manifest:", filepath.Join(dir, "manifest.json"))
	return code
}

func validMode(mode string) bool {
	return mode == "quick" || mode == "full" || mode == "race" ||
		mode == "benchmark" || mode == "native" || mode == "fingerprint"
}

func inside(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func reportDir(root, requested string) (string, error) {
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	parent, name := os.TempDir(), ""
	if requested != "" {
		abs, err := filepath.Abs(requested)
		if err != nil {
			return "", err
		}
		parent, name = filepath.Dir(abs), filepath.Base(abs)
	}
	parent, err = filepath.EvalSymlinks(parent)
	if err != nil {
		return "", err
	}
	target := filepath.Join(parent, name)
	if inside(root, target) {
		return "", errors.New("report must be outside repository, including symlink aliases")
	}
	if requested == "" {
		return os.MkdirTemp(parent, "ate-verify-")
	}
	if err := os.Mkdir(target, 0700); err != nil {
		return "", err // Existing directories are never reused.
	}
	return target, nil
}

func writeManifest(dir, name string, m *manifest) error {
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return errors.Join(enc.Encode(m), f.Close())
}

func verify(root, dir, mode string, steps []step) int {
	m := manifest{
		Schema: 1, Task: "R09", Mode: mode, Status: "running",
		Go: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		Report: dir, Started: time.Now().UTC(), Results: []result{},
	}
	if mode != "benchmark" {
		m.Skipped = append(m.Skipped, skip{Lane: "benchmark", Reason: "not selected"})
	}
	if mode != "native" {
		m.Skipped = append(m.Skipped, skip{Lane: "native", Reason: "not selected"})
	}
	m.Skipped = append(m.Skipped, skip{Lane: "power-loss", Reason: "device power-loss requires an external harness"})
	for _, lane := range []string{"full", "race"} {
		if lane != mode {
			m.Skipped = append(m.Skipped, skip{Lane: lane, Reason: "not selected"})
		}
	}
	if mode == "fingerprint" {
		m.Skipped = append(m.Skipped, skip{Lane: "quick", Reason: "format/diff/vet/build: fingerprint only"})
	}
	for i := range m.Skipped {
		m.Skipped[i].Classification, m.Skipped[i].PolicyID = "not_selected", "lane-not-selected"
		if m.Skipped[i].Lane == "power-loss" {
			m.Skipped[i].Classification, m.Skipped[i].PolicyID = "unverified_environment", "device-power-loss-external-harness"
		}
	}
	start, err := snapshot(root)
	if err == nil {
		m.Start = &start
	}
	// Leave an explicit running record if the process is killed before finalization.
	if err := writeManifest(dir, "started.json", &m); err != nil {
		fmt.Fprintln(os.Stderr, "cannot write starting manifest:", err)
		return 1
	}
	if err == nil {
		for _, s := range steps {
			m.Results = append(m.Results, runStep(root, dir, mode, s))
		}
		end, endErr := snapshot(root)
		err = endErr
		if err == nil {
			m.End = &end
			m.Stable = start.SHA256 == end.SHA256
		}
	}
	code := 0
	m.Status = "selected_passed"
	if mode == "fingerprint" {
		m.Status = "fingerprint_only"
	}
	for _, r := range m.Results {
		if hasTestSkips(r.Skips) {
			m.Status = "selected_passed_with_skips"
		}
		if r.ExitCode != 0 {
			code = 1
		}
	}
	if err != nil {
		m.Error = "fingerprint failed: " + err.Error()
		code = 1
	}
	if !m.Stable {
		code = 1
		if err == nil {
			m.Error = "source changed between start and end; results are not a stable-source verification"
		}
	}
	if code != 0 {
		m.Status = "failed"
	}
	m.Finished = time.Now().UTC()
	if err := writeManifest(dir, "manifest.json", &m); err != nil {
		fmt.Fprintln(os.Stderr, "cannot write final manifest:", err)
		return 1
	}
	return code
}

func runStep(root, dir, mode string, s step) result {
	r := result{Name: s.name, Command: s.command, Status: "passed", Started: time.Now().UTC(), Log: s.name + ".log"}
	log, err := os.OpenFile(filepath.Join(dir, r.Log), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		switch s.name {
		case "format":
			err = checkFormat(root, log)
		case "diff":
			err = checkDiff(root, log)
		default:
			cmd := exec.Command(s.command[0], s.command[1:]...)
			cmd.Dir, cmd.Stdout, cmd.Stderr = root, log, log
			err = cmd.Run()
		}
		if err != nil && !s.test {
			// Do not print environment values or command output to the console.
			fmt.Fprintln(log, "verify: command failed")
		}
		err = errors.Join(err, log.Close())
		if s.test {
			skips, parseErr := testSkips(filepath.Join(dir, r.Log), r.Log, skipContext{
				mode: mode, goos: runtime.GOOS, getenv: os.Getenv,
			})
			r.Skips = skips
			err = errors.Join(err, parseErr)
			for _, skipped := range skips {
				if skipped.Classification == "unexpected" {
					err = errors.Join(err, errors.New("unexpected go test skip"))
				}
			}
		}
	}
	if hasTestSkips(r.Skips) {
		r.Status = "passed_with_skips"
	}
	if err != nil {
		r.Status, r.ExitCode = "failed", 1
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			r.ExitCode = exit.ExitCode()
			if r.ExitCode < 0 {
				r.ExitCode = 1
			}
		}
	}
	r.Finished = time.Now().UTC()
	return r
}

func changed(root string) ([]string, error) {
	var names []string
	for _, args := range [][]string{
		{"diff", "--name-only", "--no-renames", "-z", "--"},
		{"diff", "--cached", "--name-only", "--no-renames", "-z", "--"},
		{"ls-files", "--others", "--exclude-standard", "-z"},
	} {
		out, err := git(root, args...)
		if err != nil {
			return nil, err
		}
		for _, name := range strings.Split(string(out), "\x00") {
			if name != "" {
				names = append(names, name)
			}
		}
	}
	return names, nil
}

func checkFormat(root string, log io.Writer) error {
	names, err := changed(root)
	if err != nil {
		return err
	}
	bad := false
	for _, name := range names {
		if !allowed(name) || filepath.Ext(name) != ".go" {
			continue
		}
		path := filepath.Join(root, name)
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue // Deleted files have no working-tree formatting to check.
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("non-regular changed Go file")
		}
		input, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		formatted, err := format.Source(input)
		if err != nil || !bytes.Equal(input, formatted) {
			fmt.Fprintf(log, "format check failed: %q\n", name)
			bad = true
		}
	}
	if bad {
		return errors.New("changed Go files need formatting")
	}
	return nil
}

func checkDiff(root string, log io.Writer) error {
	// Check both index and worktree, even if their changes cancel against HEAD.
	commands := [][]string{
		{"diff", "--no-ext-diff", "--check", "--"},
		{"diff", "--no-ext-diff", "--cached", "--check", "--"},
	}
	untracked, err := git(root, "ls-files", "--others", "--exclude-standard", "-z")
	if err != nil {
		return err
	}
	for _, name := range strings.Split(string(untracked), "\x00") {
		if name == "" || !allowed(name) {
			continue
		}
		info, err := os.Lstat(filepath.Join(root, name))
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("cannot check untracked source")
		}
		commands = append(commands, []string{"diff", "--no-ext-diff", "--no-index", "--check", "--", os.DevNull, name})
	}
	var failed error
	for _, args := range commands {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		// git --check can echo source lines; retain only the diagnostic hash.
		fmt.Fprintf(log, "%q: diagnostic_sha256=%x\n", cmd.Args, sha256.Sum256(out))
		// --no-index implies --exit-code: 1 with no check diagnostics merely
		// means that the new file differs from the empty file.
		var exit *exec.ExitError
		if len(args) > 2 && args[2] == "--no-index" && len(out) == 0 &&
			errors.As(err, &exit) && exit.ExitCode() == 1 {
			err = nil
		}
		if err != nil {
			failed = errors.Join(failed, err)
		}
	}
	return failed
}

func testSkips(path, logName string, policy skipContext) ([]skip, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var skips []skip
	// Hash the output associated with a test; retain its reason in the private log.
	type testOutput struct {
		hash    hash.Hash
		reason  string
		noTests bool
	}
	outputs := map[string]*testOutput{}
	terminal := false
	for {
		var event struct{ Action, Package, Test, Output string }
		err := dec.Decode(&event)
		if err == io.EOF {
			break
		}
		if err != nil {
			return skips, errors.New("incomplete go test JSON log")
		}
		key := event.Package + "\x00" + event.Test
		if event.Action == "output" {
			if outputs[key] == nil {
				outputs[key] = &testOutput{hash: sha256.New()}
			}
			out := outputs[key]
			out.hash.Write([]byte(event.Output))
			if event.Test == "" {
				out.noTests = out.noTests || strings.Join(strings.Fields(event.Output), " ") == "? "+event.Package+" [no test files]"
			} else {
				out.reason = skipReason(out.reason, event.Output)
			}
		}
		if event.Action == "skip" {
			reasonHash := fmt.Sprintf("%x", sha256.Sum256(nil))
			reason, noTests := "", false
			if out := outputs[key]; out != nil {
				reasonHash = fmt.Sprintf("%x", out.hash.Sum(nil))
				reason, noTests = out.reason, out.noTests
			}
			classification, policyID := classifySkip(policy, event.Package, event.Test, reason, noTests)
			skips = append(skips, skip{
				Package: event.Package, Test: event.Test,
				Classification: classification, PolicyID: policyID,
				Reason: "go test skip; unverified; reason in private log", ReasonHash: reasonHash, Log: logName,
			})
		}
		if event.Action == "pass" || event.Action == "fail" || event.Action == "skip" {
			delete(outputs, key)
			if event.Test == "" {
				terminal = true
			}
		}
	}
	if !terminal {
		return skips, errors.New("go test log has no package completion")
	}
	return skips, nil
}
