package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func put(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func repository(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "repo with spaces")
	put(t, root, "go.mod", "module example.test/verifyfixture\n\ngo 1.27.0\n")
	put(t, root, "sample.go", "package sample\n")
	// Existing bad formatting must not fail the changed-files-only check.
	put(t, root, "legacy.go", "package sample\nfunc old( ) { }\n")
	for _, args := range [][]string{
		{"init", "-q"}, {"add", "."},
		{"-c", "user.name=verify", "-c", "user.email=verify@example.invalid", "commit", "-qm", "fixture"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git fixture: %v: %s", err, out)
		}
	}
	return root
}

func take(t *testing.T, root string) fingerprint {
	t.Helper()
	f, err := snapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestFingerprintScope(t *testing.T) {
	root := repository(t)
	a := take(t, root)
	for _, name := range []string{
		".env", "credentials/key.go", ".ate/journal/live.go", "results/run.go",
		"testdata/generated/run.go", "testdata/secrets/token", "testdata/key.pem",
	} {
		put(t, root, name, "private material")
	}
	b := take(t, root)
	if a.SHA256 != b.SHA256 || !b.Dirty {
		t.Fatal("excluded files changed source digest or dirty metadata missing")
	}
	put(t, root, "engine/new file.go", "package engine\n")
	c := take(t, root)
	if b.SHA256 == c.SHA256 {
		t.Fatal("untracked source missing")
	}
	put(t, root, "testdata/journal/fixture.bin", "fixture-one")
	d := take(t, root)
	put(t, root, "testdata/journal/fixture.bin", "fixture-two")
	e := take(t, root)
	if c.SHA256 == d.SHA256 || d.SHA256 == e.SHA256 {
		t.Fatal("untracked fixture contents missing")
	}
	put(t, root, ".gitignore", "engine/ignored.go\n")
	put(t, root, "engine/ignored.go", "package engine\n")
	beforeDelete := take(t, root)
	if e.SHA256 == beforeDelete.SHA256 {
		t.Fatal("ignored source in allowed scope must still be hashed")
	}
	if err := os.Remove(filepath.Join(root, "sample.go")); err != nil {
		t.Fatal(err)
	}
	f := take(t, root)
	if f.SHA256 == beforeDelete.SHA256 {
		t.Fatal("deletion not represented")
	}
	data, err := json.Marshal(f)
	if err != nil || bytes.Contains(data, []byte("private material")) || bytes.Contains(data, []byte("fixture-two")) {
		t.Fatal("fingerprint exposed file contents")
	}
}

func TestFingerprintRegistryResource(t *testing.T) {
	root := repository(t)
	const resource = "formats/registry.json"
	put(t, root, resource, "{\"version\":1}\n")
	before := take(t, root)
	found := false
	for _, file := range before.Files {
		if file.Path == resource {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("embedded registry resource missing from fingerprint")
	}
	put(t, root, resource, "{\"version\":2}\n")
	after := take(t, root)
	if before.SHA256 == after.SHA256 {
		t.Fatal("registry resource content change did not change fingerprint")
	}
}

func TestFingerprintCoversRepositoryPackages(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "list", "-f", "{{.Dir}}", "./...")
	cmd.Dir = root
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("enumerate repository packages: %v", err)
	}
	for _, directory := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		relative, err := filepath.Rel(root, directory)
		if err != nil {
			t.Fatal(err)
		}
		if !allowed(filepath.Join(relative, "probe.go")) {
			t.Errorf("package %s is missing from fingerprint scope", relative)
		}
	}
}

func TestFingerprintObservationSource(t *testing.T) {
	root := repository(t)
	before := take(t, root)
	put(t, root, "observation/collector.go", "package observation\nconst Capacity = 1\n")
	added := take(t, root)
	put(t, root, "observation/collector.go", "package observation\nconst Capacity = 2\n")
	changed := take(t, root)
	if before.SHA256 == added.SHA256 || added.SHA256 == changed.SHA256 {
		t.Fatal("observation source is not covered by content fingerprint")
	}
}

func TestPlanIncludesExplicitBenchmarkAndNativeLanes(t *testing.T) {
	benchmark := plan("benchmark")
	if len(benchmark) != 5 || benchmark[len(benchmark)-1].name != "benchmark" ||
		!benchmark[len(benchmark)-1].test {
		t.Fatalf("benchmark lane was not added after quick checks: %+v", benchmark)
	}
	benchmarkCommand := strings.Join(benchmark[len(benchmark)-1].command, " ")
	if !strings.Contains(benchmarkCommand, "-json") ||
		!strings.Contains(benchmarkCommand, "tests/benchmarks") {
		t.Fatalf("benchmark command does not target benchmark package: %+v", benchmark[len(benchmark)-1])
	}

	native := plan("native")
	if len(native) != 5 || native[len(native)-1].name != "native" ||
		!native[len(native)-1].test {
		t.Fatalf("native lane was not added after quick checks: %+v", native)
	}
	if !strings.Contains(strings.Join(native[len(native)-1].command, " "), "./storage") {
		t.Fatalf("native command does not target storage package: %+v", native[len(native)-1])
	}
}

func TestReportsAndSymlinks(t *testing.T) {
	root := repository(t)
	before := take(t, root)
	if _, err := reportDir(root, filepath.Join(root, "report")); err == nil {
		t.Fatal("in-repository report accepted")
	}
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := reportDir(root, filepath.Join(alias, "report")); err == nil {
		t.Fatal("symlink alias bypassed report boundary")
	}
	dir := filepath.Join(parent, "report with spaces")
	if _, err := reportDir(root, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := reportDir(root, dir); err == nil {
		t.Fatal("existing report accepted")
	}
	t.Setenv("TMPDIR", parent)
	tempReport, err := reportDir(root, "")
	if err != nil || !inside(parent, tempReport) || take(t, root).SHA256 != before.SHA256 {
		t.Fatal("default temporary report polluted source")
	}
	t.Setenv("TMPDIR", root)
	if _, err := reportDir(root, ""); err == nil {
		t.Fatal("in-repository TMPDIR accepted")
	}
	if err := os.Symlink(filepath.Join(root, "sample.go"), filepath.Join(root, "link.go")); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot(root); err == nil {
		t.Fatal("source symlink was followed")
	}
}

func TestChangedFormatAndDiff(t *testing.T) {
	root := repository(t)
	var log bytes.Buffer
	if err := checkFormat(root, &log); err != nil {
		t.Fatal("unchanged legacy formatting checked:", err)
	}
	put(t, root, "new file.go", "package sample\nfunc newOne( ) { }\n")
	if err := checkFormat(root, &log); err == nil {
		t.Fatal("untracked badly formatted source passed")
	}
	put(t, root, "new file.go", "package sample\n")
	if err := checkFormat(root, &log); err != nil {
		t.Fatal(err)
	}
	put(t, root, "testdata/new fixture.txt", "secret-source-line \t\n")
	if err := checkDiff(root, &log); err == nil {
		t.Fatal("untracked whitespace error passed")
	}
	if strings.Contains(log.String(), "secret-source-line") {
		t.Fatal("diff leaked source")
	}
	put(t, root, "testdata/new fixture.txt", "valid\n")
	put(t, root, "sample.go", "package sample \t\n")
	if _, err := git(root, "add", "sample.go"); err != nil {
		t.Fatal(err)
	}
	put(t, root, "sample.go", "package sample\n")
	if err := checkDiff(root, &log); err == nil {
		t.Fatal("staged whitespace error hidden by working-tree repair")
	}
	if _, err := git(root, "add", "sample.go"); err != nil {
		t.Fatal(err)
	}
	if err := checkDiff(root, &log); err != nil {
		t.Fatal("valid diff failed:", err)
	}
}

// A subprocess exercises real exit codes and deterministic mutation without a shell.
func TestHelperProcess(t *testing.T) {
	for i, arg := range os.Args {
		if arg != "--" {
			continue
		}
		switch os.Args[i+1] {
		case "verify":
			os.Exit(cli([]string{"-report", os.Args[i+2], "full"}))
		case "fail":
			fmt.Println("intentional failure log")
			os.Exit(7)
		case "mutate":
			if err := os.WriteFile(os.Args[i+2], []byte("package sample\n// changed\n"), 0600); err != nil {
				os.Exit(8)
			}
			os.Exit(0)
		}
	}
}

func loadManifest(t *testing.T, dir string) manifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestFailureMutationAndStableRun(t *testing.T) {
	for _, action := range []string{"fail", "mutate", "stable"} {
		t.Run(action, func(t *testing.T) {
			root := repository(t)
			dir := t.TempDir()
			steps := []step{}
			if action != "stable" {
				steps = append(steps, step{"helper", []string{
					os.Args[0], "-test.run=^TestHelperProcess$", "--", action, filepath.Join(root, "sample.go"),
				}, false})
			}
			code := verify(root, dir, "fingerprint", steps)
			m := loadManifest(t, dir)
			if action == "stable" {
				if code != 0 || !m.Stable || m.Status != "fingerprint_only" || len(m.Skipped) == 0 {
					t.Fatalf("invalid fingerprint result: %+v", m)
				}
				return
			}
			if code == 0 || m.Status != "failed" {
				t.Fatal("failure/mutation reported success")
			}
			if action == "fail" {
				log, err := os.ReadFile(filepath.Join(dir, "helper.log"))
				if err != nil || !bytes.Contains(log, []byte("intentional failure log")) ||
					m.Results[0].ExitCode != 7 || !m.Stable {
					t.Fatal("failure log/exit code lost")
				}
			} else if m.Stable || m.Results[0].ExitCode != 0 {
				t.Fatal("successful command with source mutation not detected")
			}
		})
	}
}

func TestRealGoTestSkipAndFailure(t *testing.T) {
	root := repository(t)
	put(t, root, "sample_test.go", `package sample

import "testing"

func TestSkip(t *testing.T) { t.Skip("intentional fixture skip") }
`)
	s := step{"test", []string{"go", "test", "-count=1", "-json", "./..."}, true}
	dir := filepath.Join(t.TempDir(), "cli-report")
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$", "--", "verify", dir)
	cmd.Dir = root // Full CLI checks only this tiny temporary module.
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("unexpected skip did not fail the CLI: %v %s", err, out)
	}
	m := loadManifest(t, dir)
	if len(m.Results) != len(plan("full")) || !m.Stable {
		t.Fatalf("CLI did not complete stable fixture checks: %+v", m)
	}
	for _, r := range m.Results[:len(m.Results)-1] {
		if r.Status != "passed" || r.ExitCode != 0 {
			t.Fatalf("fixture failed before its unexpected skip: %+v", r)
		}
	}
	last := m.Results[len(m.Results)-1]
	if m.Status != "failed" || last.Status != "failed" || last.ExitCode != 1 ||
		len(last.Skips) != 1 || last.Skips[0].Classification != "unexpected" ||
		last.Skips[0].Test != "TestSkip" || last.Skips[0].ReasonHash == "" {
		t.Fatalf("skip not represented: %+v", m.Results)
	}
	log, err := os.ReadFile(filepath.Join(dir, last.Log))
	if err != nil || !bytes.Contains(log, []byte(`"Action":"pass","Package":"example.test/verifyfixture"`)) {
		t.Fatal("fixture go test did not pass before runner rejected its skip")
	}
	put(t, root, "failure_test.go", `package sample
import "testing"
func TestFailure(t *testing.T) { t.Fatal("intentional test failure") }
`)
	r := runStep(root, t.TempDir(), "full", s)
	if r.Status != "failed" || r.ExitCode == 0 || len(r.Skips) != 1 {
		t.Fatalf("go test failure lost: %+v", r)
	}
}

func TestSkipPolicyBoundaries(t *testing.T) {
	env := map[string]string{}
	c := skipContext{mode: "full", goos: "linux", getenv: func(k string) string { return env[k] }}
	for _, p := range skipPolicies {
		t.Run(p.id, func(t *testing.T) {
			local := c
			local.mode = p.modes[0]
			if !p.condition(local) {
				local.goos = "windows"
			}
			reason := p.reason
			if p.id == "native-unix-host-bind-denied" {
				reason = "native Unix socket binding is denied by this execution environment: listen unix /tmp/private-token/agent.sock: bind: permission denied"
			}
			if p.id == "native-unix-formats-bind-denied" {
				reason = "native Unix socket unavailable in this test sandbox: listen unix /tmp/private-token/local.sock: bind: operation not permitted"
			}
			for _, test := range p.tests {
				class, id := classifySkip(local, modulePath+p.pkg, test, reason, false)
				if class != "allowed_environment" || id != p.id {
					t.Fatalf("policy %s test %s not allowed: %s %s", p.id, test, class, id)
				}
				for _, bad := range []struct{ pkg, test, reason string }{
					{modulePath + p.pkg + "/other", test, reason},
					{modulePath + p.pkg, test + "/new-child", reason},
					{modulePath + p.pkg, test, ""},
					{modulePath + p.pkg, test, reason + " unexpected detail"},
					{modulePath + p.pkg, test, "unrelated skip"},
				} {
					if got, _ := classifySkip(local, bad.pkg, bad.test, bad.reason, false); got != "unexpected" {
						t.Fatalf("widened policy %s accepted mismatched package/test/reason", p.id)
					}
				}
			}
			local.mode = "quick"
			if got, _ := classifySkip(local, modulePath+p.pkg, p.tests[0], reason, false); got != "unexpected" {
				t.Fatal("quick lane inherited a test exception")
			}
		})
	}
	for _, tc := range []struct{ id, key, value string }{
		{"linux-ext4-full-disk-opt-in", "ATE_NATIVE_FULL_DISK_ROOT", "/configured"},
		{"linux-ext4-reboot-opt-in", "ATE_NATIVE_REBOOT_PHASE", "verify"},
		{"model-evaluation-opt-in", "ATE_RUN_MODEL_EVAL", "1"},
	} {
		for _, p := range skipPolicies {
			if p.id != tc.id {
				continue
			}
			env[tc.key] = tc.value
			if got, _ := classifySkip(c, modulePath+p.pkg, p.tests[0], p.reason, false); got != "unexpected" {
				t.Fatalf("configured environment still allowed %s", tc.id)
			}
			delete(env, tc.key)
		}
	}
	for _, p := range skipPolicies {
		if !strings.HasPrefix(p.id, "linux-ext4-") && p.id != "native-directory-platform" {
			continue
		}
		local := c
		local.goos = "darwin"
		if got, _ := classifySkip(local, modulePath+p.pkg, p.tests[0], p.reason, false); got != "unexpected" {
			t.Fatalf("wrong platform allowed %s", p.id)
		}
	}
}

func TestSkipParserIsolationAndPrivateReasons(t *testing.T) {
	const pkg = modulePath + "/service"
	const parent = "TestWorkSummaryEmissionEmbeddedSDKConformance"
	const child = parent + "/unix"
	const reason = "sandbox disallows native Unix sockets; pipe conformance runs separately"
	const private = "credential=private-secret"
	type event struct{ Action, Package, Test, Output string }
	events := []event{
		{"output", pkg, child, "=== RUN   " + child + "\n"},
		{"output", pkg, child, "    source_test.go:1: " + private + "\n"},
		{"output", pkg, child, "    source_test.go:2: " + reason + "\n"},
		{"output", pkg, child, "--- SKIP: " + child + " (0.00s)\n"},
		{"skip", pkg, child, ""},
		{"output", pkg, parent, "--- SKIP: " + parent + " (0.00s)\n"},
		{"skip", pkg, parent, ""}, // A child must not justify a reasonless parent.
		{"output", pkg, "TestOther", "    source_test.go:3: " + private + "\n"},
		{"skip", pkg, "TestOther", ""},
		{"output", pkg, child, "    source_test.go:4: " + reason + "\n"},
		{"output", pkg, child, "    source_test.go:5: \n"},
		{"skip", pkg, child, ""}, // Count/repeated run must not inherit an old reason.
		{"pass", pkg, "", ""},
		{"output", "example.test/empty", "", "?   \texample.test/empty\t[no test files]\n"},
		{"skip", "example.test/empty", "", ""},
		{"skip", "example.test/unknown", "", ""},
	}
	var log bytes.Buffer
	enc := json.NewEncoder(&log)
	for _, e := range events {
		if err := enc.Encode(e); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	put(t, dir, "test.log", log.String())
	skips, err := testSkips(filepath.Join(dir, "test.log"), "test.log",
		skipContext{mode: "full", goos: "linux", getenv: func(string) string { return "" }})
	if err != nil || len(skips) != 6 {
		t.Fatalf("parse: %v %+v", err, skips)
	}
	want := []string{"allowed_environment", "unexpected", "unexpected", "unexpected", "no_test_files", "unexpected"}
	for i, s := range skips {
		if s.Classification != want[i] || s.PolicyID == "" || s.ReasonHash == "" {
			t.Fatalf("event %d: %+v", i, s)
		}
	}
	sum := sha256.Sum256([]byte(events[0].Output + events[1].Output + events[2].Output + events[3].Output))
	if skips[0].ReasonHash != fmt.Sprintf("%x", sum) {
		t.Fatal("skip output hash changed")
	}
	raw, err := json.Marshal(skips)
	if err != nil || bytes.Contains(raw, []byte(private)) || bytes.Contains(raw, []byte(reason)) {
		t.Fatal("skip manifest leaked reason")
	}
	if hasTestSkips(skips[4:5]) {
		t.Fatal("no-test-files package counted as skipped test")
	}
}

func TestRealRunnerSkipPolicyStatus(t *testing.T) {
	t.Setenv("ATE_RUN_MODEL_EVAL", "")
	const modelTest = "TestRealModelRecursiveSummaryAndEvidenceRecall"
	const modelReason = "opt-in real Responses evaluation; set ATE_RUN_MODEL_EVAL=1 and explicit endpoint/model/artifact directory"
	for _, tc := range []struct {
		name, source, mode, status, classification string
		code, skips                                int
	}{
		{"allowed", `func ` + modelTest + `(t *testing.T) { t.Skip(` + fmt.Sprintf("%q", modelReason) + `) }`, "full", "selected_passed_with_skips", "allowed_environment", 0, 1},
		{"empty", `func ` + modelTest + `(t *testing.T) { t.Skip() }`, "full", "failed", "unexpected", 1, 1},
		{"skip-now", `func TestUnknown(t *testing.T) { t.SkipNow() }`, "full", "failed", "unexpected", 1, 1},
		{"unknown", `func ` + modelTest + `(t *testing.T) { t.Skip("private-unknown-reason") }`, "full", "failed", "unexpected", 1, 1},
		{"child", `func ` + modelTest + `(t *testing.T) { t.Run("new-child", func(t *testing.T) { t.Skip(` + fmt.Sprintf("%q", modelReason) + `) }) }`, "full", "failed", "unexpected", 1, 1},
		{"parent-child", `func TestParent(t *testing.T) { t.Run("child", func(t *testing.T) { t.Skip("child") }); t.Skip() }`, "full", "failed", "unexpected", 1, 2},
		{"benchmark", `func BenchmarkUnknown(b *testing.B) { b.Skip("private-unknown-reason") }`, "benchmark", "failed", "unexpected", 1, 1},
		{"no-tests", "", "full", "selected_passed", "no_test_files", 0, 1},
		{"pass", `func TestPass(t *testing.T) {}`, "full", "selected_passed", "", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := repository(t)
			put(t, root, "go.mod", "module "+modulePath+"/tests/evaluation\n\ngo 1.27.0\n")
			if tc.source != "" {
				put(t, root, "sample_test.go", "package sample\nimport \"testing\"\n"+tc.source+"\n")
			}
			s := step{"test", []string{"go", "test", "-count=1", "-json", "./..."}, true}
			if tc.mode == "benchmark" {
				s.command = append(s.command, "-run=^$", "-bench=.", "-benchtime=1x")
			}
			dir := t.TempDir()
			code := verify(root, dir, tc.mode, []step{s})
			m := loadManifest(t, dir)
			if code != tc.code || m.Status != tc.status || !m.Stable || len(m.Results) != 1 {
				t.Fatalf("runner code=%d manifest=%+v", code, m)
			}
			r := m.Results[0]
			if len(r.Skips) != tc.skips || (r.ExitCode != 0) != (tc.code != 0) ||
				(tc.code != 0 && r.Status != "failed") {
				t.Fatalf("step did not enforce policy: %+v", r)
			}
			for _, s := range r.Skips {
				if s.Classification != tc.classification || s.PolicyID == "" {
					t.Fatalf("skip classification: %+v", s)
				}
			}
			for _, lane := range m.Skipped {
				if lane.Lane != "power-loss" && lane.Classification != "not_selected" {
					t.Fatalf("lane confused with a test skip: %+v", lane)
				}
			}
			raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
			if err != nil || bytes.Contains(raw, []byte("private-unknown-reason")) ||
				bytes.Contains(raw, []byte(modelReason)) {
				t.Fatal("runner manifest leaked a reason")
			}
			info, err := os.Stat(filepath.Join(dir, r.Log))
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal("raw log is not private")
			}
		})
	}
}
