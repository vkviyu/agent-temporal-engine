package main

import (
	"regexp"
	"slices"
	"strings"
)

const modulePath = "github.com/vkviyu/agent-temporal-engine"

type skipContext struct {
	mode   string
	goos   string
	getenv func(string) string
}

type skipPolicy struct {
	id, pkg, reason string
	modes, tests    []string
	condition       func(skipContext) bool
	// Only the two existing Skipf socket errors have variable text.
	dynamicReason *regexp.Regexp
}

func nativeUnsupported(c skipContext) bool { return c.goos != "darwin" && c.goos != "linux" }
func nativeSupported(c skipContext) bool   { return c.goos == "darwin" || c.goos == "linux" }

var fullRace = []string{"full", "race"}
var storageModes = []string{"full", "race", "native"}

// No package-wide or Test*/Benchmark* exception. New tests/reasons require review.
// The four existing reasonless native Skip calls are deliberately not allowed.
var skipPolicies = []skipPolicy{
	{
		id: "linux-ext4-full-disk-opt-in", pkg: "/storage", modes: storageModes,
		tests:  []string{"TestNativeExt4FullDiskPreservesAcknowledgedPrefix"},
		reason: "set ATE_NATIVE_FULL_DISK_ROOT to a disposable ext4 filesystem of at most 64 MiB",
		condition: func(c skipContext) bool {
			return c.goos == "linux" && c.getenv("ATE_NATIVE_FULL_DISK_ROOT") == ""
		},
	},
	{
		id: "linux-ext4-reboot-opt-in", pkg: "/storage", modes: storageModes,
		tests:  []string{"TestNativeExt4RebootPreservesAcknowledgedPrefix"},
		reason: "set ATE_NATIVE_REBOOT_PHASE=create or verify and persistent root/witness paths",
		condition: func(c skipContext) bool {
			return c.goos == "linux" && c.getenv("ATE_NATIVE_REBOOT_PHASE") == ""
		},
	},
	{
		id: "model-evaluation-opt-in", pkg: "/tests/evaluation", modes: fullRace,
		tests: []string{
			"TestRealModelRecursiveSummaryAndEvidenceRecall",
			"TestRealModelTypedSummaryReviewAndEvidenceRecallV2",
			"TestRealModelCrossProjectMemoryApplicability",
		},
		reason:    "opt-in real Responses evaluation; set ATE_RUN_MODEL_EVAL=1 and explicit endpoint/model/artifact directory",
		condition: func(c skipContext) bool { return c.getenv("ATE_RUN_MODEL_EVAL") != "1" },
	},
	{
		id: "native-unix-service-bind-denied", pkg: "/service", modes: fullRace,
		tests:  []string{"TestLocalServiceClientConformance"},
		reason: "sandbox disallows Unix sockets", condition: nativeSupported,
	},
	{
		id: "native-unix-session-bind-denied", pkg: "/service", modes: fullRace,
		tests:  []string{"TestListenRefusesExistingSocketAndCleanupPreservesReplacement", "TestServeContextStopsListenerAndSessions"},
		reason: "native Unix sockets denied; session security is covered by net.Pipe", condition: nativeSupported,
	},
	{
		id: "native-unix-conformance-bind-denied", pkg: "/service", modes: fullRace,
		tests: []string{
			"TestWorkSummaryEmissionEmbeddedSDKConformance/unix",
			"TestQueryOnlyEmbeddedSDKConformanceAndResume/unix",
			"TestDomainServiceSDKCanonicalCommandsRecoveryAndKeyIsolation/unix",
			"TestTransferEmbeddedSDKCrossSpaceApprovalAndRecovery/unix",
			"TestR12StateViewEmbeddedSDKConformance/unix",
			"TestR12TaskTraceEmbeddedSDKConformance/unix",
			"TestR12GraphQueryEmbeddedSDKConformance/unix",
			"TestR12DurableCommitLostACKReconcilesEmbeddedSDK/unix",
			"TestR12ContentBudgetEmbeddedSDKConformance/unix",
			"TestAgentStateRecordsServiceConformance/unix",
			"TestAgentStateRecordsLargeRealStoreUnixReassemblesAndLegacyRemainsUsable",
		},
		reason: "sandbox disallows native Unix sockets; pipe conformance runs separately", condition: nativeSupported,
	},
	{
		id: "native-unix-cli-bind-denied", pkg: "/cmd/ate", modes: fullRace,
		tests: []string{
			"TestServeCLIRealSocketOwnsStoreAndDrains/core-only",
			"TestServeCLIRealSocketOwnsStoreAndDrains/agent-profile",
			"TestServeCLIRealSocketOwnsStoreAndDrains/event-profile",
			"TestServeCLIRealSocketOwnsStoreAndDrains/both-profiles",
		},
		reason: "sandbox disallows Unix socket bind", condition: nativeSupported,
	},
	{
		id: "native-unix-host-bind-denied", pkg: "/tests/host", modes: fullRace,
		tests: []string{"TestLongTaskPublicHostAcceptance/unix-sdk"}, condition: nativeSupported,
		dynamicReason: regexp.MustCompile(`^native Unix socket binding is denied by this execution environment: listen unix /[^\r\n]+/agent\.sock: bind: (operation not permitted|permission denied)$`),
	},
	{
		id: "native-unix-formats-bind-denied", pkg: "/formats", modes: fullRace,
		tests: []string{"TestLocalCapabilityGoldenMatchesRealUnixHello"}, condition: nativeSupported,
		dynamicReason: regexp.MustCompile(`^native Unix socket unavailable in this test sandbox: listen unix /[^\r\n]+/local\.sock: bind: (operation not permitted|permission denied)$`),
	},
	{
		id: "native-directory-platform", pkg: "/storage", modes: storageModes,
		tests: []string{
			"TestNativeSegmentCloseKeepsMetadataResponsiveAndOwnerHeld",
			"TestNativeOwnerCloseKeepsMetadataResponsive",
			"TestNativeRetirementIOKeepsCurrentPinnedLayoutReadable/close",
			"TestNativeRetirementIOKeepsCurrentPinnedLayoutReadable/remove",
			"TestNativePinnedReadStartsWhilePublicationSyncIsPaused/segment-sync",
			"TestNativePinnedReadStartsWhilePublicationSyncIsPaused/current-synced",
			"TestNativeFenceDuringPublicationRetainsUnknownResources/before-current",
			"TestNativeFenceDuringPublicationRetainsUnknownResources/after-current",
			"TestNativeVerifyReleasesMetadataAndHonorsConcurrentFence/read",
			"TestNativeVerifyReleasesMetadataAndHonorsConcurrentFence/fence",
			"TestNativeActiveReadDoesNotBlockPublisherOrMetadata/journal",
			"TestNativeActiveReadDoesNotBlockPublisherOrMetadata/pin",
			"TestNativeActiveLeaseSurvivesPinCloseAndCompaction/temporary",
			"TestNativeActiveLeaseSurvivesPinCloseAndCompaction/closed-pin",
			"TestNativeCloseWaitsForActiveReadAndPreservesFence/close",
			"TestNativeCloseWaitsForActiveReadAndPreservesFence/fence-then-close",
			"TestNativePinSnapshotSurvivesConcurrentRotation",
		},
		reason: "native directory backend requires Darwin or Linux", condition: nativeUnsupported,
	},
	{
		id: "native-scale-platform", pkg: "/tests/benchmarks", modes: []string{"benchmark"},
		tests: []string{
			"BenchmarkNativeScale/EngineKeys128Batch1Writers8",
			"BenchmarkNativeScale/EngineKeys128Batch16Writers8",
			"BenchmarkNativeScale/EngineKeys1024Batch1Writers8",
			"BenchmarkNativeScale/EngineKeys1024Batch16Writers8",
			"BenchmarkNativeScale/PhysicalJournalPinHold0ms",
			"BenchmarkNativeScale/PhysicalJournalPinHold10ms",
		},
		reason: "native DirectoryIO requires Darwin/Linux", condition: nativeUnsupported,
	},
	{
		id: "native-formats-platform", pkg: "/formats", modes: fullRace,
		tests:  []string{"TestLocalCapabilityGoldenMatchesRealUnixHello"},
		reason: "native store requires Darwin/Linux", condition: nativeUnsupported,
	},
}

func classifySkip(c skipContext, pkg, test, reason string, noTests bool) (string, string) {
	if test == "" && noTests {
		return "no_test_files", "go-package-no-test-files"
	}
	if test == "" || strings.TrimSpace(reason) == "" {
		return "unexpected", "unexpected-missing-reason"
	}
	for _, p := range skipPolicies {
		if pkg != modulePath+p.pkg || !slices.Contains(p.modes, c.mode) ||
			!slices.Contains(p.tests, test) || !p.condition(c) {
			continue
		}
		if (p.dynamicReason == nil && reason == p.reason) ||
			(p.dynamicReason != nil && p.dynamicReason.MatchString(reason)) {
			return "allowed_environment", p.id
		}
	}
	return "unexpected", "unexpected-unlisted-skip"
}

func hasTestSkips(skips []skip) bool {
	for _, s := range skips {
		if s.Classification != "no_test_files" {
			return true
		}
	}
	return false
}

var sourceMessage = regexp.MustCompile(`^\s+[^ \t\r\n]+\.go:[0-9]+: ?(.*)$`)

// Use only the last source-attributed message, never an arbitrary substring of
// a test's output. Skip() emits an empty message and must erase preceding logs.
// Each package/test has its own state: child output cannot justify a parent skip.
func skipReason(previous, output string) string {
	for _, line := range strings.Split(strings.TrimSuffix(output, "\n"), "\n") {
		if match := sourceMessage.FindStringSubmatch(line); match != nil {
			previous = match[1]
		} else if text := strings.TrimSpace(line); text != "" &&
			!strings.HasPrefix(text, "=== ") && !strings.HasPrefix(text, "--- ") {
			previous += "\n" + line
		}
		// A long reason cannot match any policy; keep memory bounded. Its full
		// bytes remain in the private log and the streaming output hash.
		if len(previous) > 16<<10 {
			previous = "<oversized skip reason>"
		}
	}
	return previous
}
