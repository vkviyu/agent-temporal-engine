package memory

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vkviyu/agent-temporal-engine/model"
)

type memoryBenchmarkFixture struct {
	name                string
	store               *Store
	raw                 []byte
	rootSource          Source
	latestMemory        Memory
	latestPath          ExperiencePath
	query               Query
	memoryCanonicalSize int
	pathCanonicalSize   int
	dependencyNodes     int
}

const (
	memoryBenchmarkSpace        = model.SpaceID("memory-benchmark")
	memoryBenchmarkStore        = model.StoreID("memory-benchmark-store")
	memoryBenchmarkTargetSmall  = 64 << 10
	memoryBenchmarkTargetMedium = 1 << 20
)

func memoryBenchmarkDigest(n int) model.ObjectID {
	return model.ObjectID(fmt.Sprintf("%064x", n))
}

func memoryBenchmarkAnchor(lsn uint64) model.SnapshotAnchor {
	return model.SnapshotAnchor{
		Fence: model.CommitFence{
			Store:        memoryBenchmarkStore,
			LSN:          lsn,
			CommitDigest: model.CommitDigest(memoryBenchmarkDigest(0xffff)),
		},
		Space: memoryBenchmarkSpace,
	}
}

func memoryBenchmarkSource(n int, lsn uint64) Source {
	return Source{SourceID: memoryBenchmarkDigest(n), Anchor: memoryBenchmarkAnchor(lsn)}
}

func memoryBenchmarkApplicability(tier string) Applicability {
	return Applicability{
		Goal:          "memory-capacity",
		Environment:   "benchmark",
		SchemaVersion: 1,
		Tags:          []string{"r16", "memory", tier},
	}
}

func memoryBenchmarkPromote(s *Store, m Memory) (Memory, error) {
	candidate, err := s.PutCandidate(m)
	if err != nil {
		return Memory{}, err
	}
	validated, err := s.Validate(candidate.ID, Validation{
		Passed:          true,
		StrategyVersion: 1,
		Evidence:        []Source{m.Sources[len(m.Sources)-1]},
	})
	if err != nil {
		return Memory{}, err
	}
	return s.Promote(validated.ID)
}

func memoryBenchmarkMemory(index int, previous model.ObjectID, root Source, tier string) Memory {
	observation := memoryBenchmarkSource(0x100000+index, uint64(index+2))
	dependency := root
	if previous != "" {
		dependency = Source{SourceID: previous, Anchor: memoryBenchmarkAnchor(uint64(index + 1))}
	}
	return Memory{
		Space:   memoryBenchmarkSpace,
		Kind:    KindFact,
		Claim:   []byte(fmt.Sprintf("r16 benchmark transitive memory record %08d", index)),
		Sources: []Source{dependency, observation},
		Evidence: []Evidence{
			{Source: observation, Note: "benchmark evidence"},
			{Source: root, Counter: index%17 == 0, Note: "benchmark root provenance"},
		},
		Applicability: memoryBenchmarkApplicability(tier),
		Confidence:    900,
		RecordedAt:    time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC),
	}
}

func memoryBenchmarkBuild(tier string, targetBytes int) (memoryBenchmarkFixture, error) {
	store := New()
	root := memoryBenchmarkSource(0x0f0000, 1)
	promoted := make([]Memory, 0, 256)
	raw, err := EncodeSnapshot(store.Snapshot())
	if err != nil {
		return memoryBenchmarkFixture{}, err
	}

	batch := 8
	for len(promoted) < MaxSnapshotRecords-16 {
		before := raw
		start := len(promoted)
		for i := 0; i < batch && len(promoted) < MaxSnapshotRecords-16; i++ {
			previous := model.ObjectID("")
			if len(promoted) != 0 {
				previous = promoted[len(promoted)-1].ID
			}
			m, err := memoryBenchmarkPromote(store, memoryBenchmarkMemory(len(promoted), previous, root, tier))
			if err != nil {
				return memoryBenchmarkFixture{}, err
			}
			promoted = append(promoted, m)
		}
		raw, err = EncodeSnapshot(store.Snapshot())
		if err == nil {
			if len(raw) >= targetBytes {
				break
			}
			if batch < 256 {
				batch *= 2
			}
			continue
		}

		// Restore the last valid public snapshot and retry with a smaller batch.
		encodeErr := err
		store, err = RestoreSnapshot(before)
		if err != nil {
			return memoryBenchmarkFixture{}, err
		}
		promoted = promoted[:start]
		raw = before
		if batch == 1 {
			return memoryBenchmarkFixture{}, fmt.Errorf("snapshot target exceeded at one record: %w", encodeErr)
		}
		batch /= 2
	}
	if len(promoted) == 0 || len(raw) > MaxSnapshotBytes {
		return memoryBenchmarkFixture{}, ErrInvalidMemory
	}

	pathAnchor := memoryBenchmarkAnchor(uint64(len(promoted) + 20))
	applicability := memoryBenchmarkApplicability(tier)
	paths := make([]ExperiencePath, 0, 3)
	memberIndexes := []int{len(promoted) / 3, (len(promoted) * 2) / 3, len(promoted) - 1}
	for i, memberIndex := range memberIndexes {
		member := promoted[memberIndex]
		path, err := store.BuildExperiencePath(
			memoryBenchmarkSpace,
			pathAnchor,
			[]Step{{
				Node:    model.NodeRef{Kind: model.NodeObject, Object: memoryBenchmarkDigest(0x200000 + i)},
				Memory:  member.ID,
				Order:   0,
				Outcome: "confirmed",
			}},
			[]model.NodeRef{{
				Kind:   model.NodeObject,
				Object: memoryBenchmarkDigest(0x210000 + i),
			}},
			Validation{Passed: true, StrategyVersion: 1, Evidence: []Source{root}},
			applicability,
			func() []model.ObjectID {
				if len(paths) == 0 {
					return nil
				}
				return []model.ObjectID{paths[len(paths)-1].ID}
			}(),
		)
		if err != nil {
			return memoryBenchmarkFixture{}, err
		}
		paths = append(paths, path)
	}

	raw, err = EncodeSnapshot(store.Snapshot())
	if err != nil || len(raw) > MaxSnapshotBytes {
		if err == nil {
			err = ErrInvalidMemory
		}
		return memoryBenchmarkFixture{}, err
	}
	latestMemory := promoted[len(promoted)-1]
	latestPath := paths[len(paths)-1]
	memoryRaw, err := Canonical(latestMemory)
	if err != nil {
		return memoryBenchmarkFixture{}, err
	}
	pathRaw, err := CanonicalPath(latestPath)
	if err != nil {
		return memoryBenchmarkFixture{}, err
	}
	policy := store.AccessPolicy()
	closure, err := policy.DependencyClosure([]model.ObjectID{latestPath.ID})
	if err != nil {
		return memoryBenchmarkFixture{}, err
	}
	return memoryBenchmarkFixture{
		name:         tier,
		store:        store,
		raw:          raw,
		rootSource:   root,
		latestMemory: latestMemory,
		latestPath:   latestPath,
		query: Query{
			Space:         memoryBenchmarkSpace,
			Goal:          "memory-capacity",
			Environment:   "benchmark",
			SchemaVersion: 1,
			Tags:          []string{"r16", "memory", tier},
			Text:          "r16 benchmark transitive",
			At:            pathAnchor,
		},
		memoryCanonicalSize: len(memoryRaw),
		pathCanonicalSize:   len(pathRaw),
		dependencyNodes:     len(closure),
	}, nil
}

func memoryBenchmarkFixtures(b *testing.B) []memoryBenchmarkFixture {
	b.Helper()
	memoryBenchmarkFixtureCache.once.Do(func() {
		nearTarget := MaxSnapshotBytes * 7 / 8
		for _, spec := range []struct {
			name   string
			target int
		}{
			{name: "small", target: memoryBenchmarkTargetSmall},
			{name: "medium", target: memoryBenchmarkTargetMedium},
			{name: "near_snapshot_limit", target: nearTarget},
		} {
			fixture, err := memoryBenchmarkBuild(spec.name, spec.target)
			if err != nil {
				memoryBenchmarkFixtureCache.err = fmt.Errorf("build %s fixture: %w", spec.name, err)
				return
			}
			memoryBenchmarkFixtureCache.fixtures = append(memoryBenchmarkFixtureCache.fixtures, fixture)
		}
	})
	if memoryBenchmarkFixtureCache.err != nil {
		b.Fatal(memoryBenchmarkFixtureCache.err)
	}
	return memoryBenchmarkFixtureCache.fixtures
}

var memoryBenchmarkFixtureCache struct {
	once     sync.Once
	fixtures []memoryBenchmarkFixture
	err      error
}

func memoryBenchmarkReportMetrics(b *testing.B, f memoryBenchmarkFixture) {
	b.ReportMetric(float64(len(f.store.Snapshot().Memories)), "memory_records")
	b.ReportMetric(float64(len(f.store.Snapshot().Paths)), "path_records")
	b.ReportMetric(float64(len(f.raw)), "snapshot_bytes")
	b.ReportMetric(float64(f.memoryCanonicalSize), "memory_record_bytes")
	b.ReportMetric(float64(f.pathCanonicalSize), "path_record_bytes")
	b.ReportMetric(float64(f.dependencyNodes), "dependency_nodes")
}

func memoryBenchmarkPreflight(b *testing.B, f memoryBenchmarkFixture) {
	b.Helper()
	raw, err := EncodeSnapshot(f.store.Snapshot())
	if err != nil {
		b.Fatalf("encode preflight snapshot: %v", err)
	}
	restored, err := RestoreSnapshot(raw)
	if err != nil {
		b.Fatalf("restore preflight snapshot: %v", err)
	}
	reencoded, err := EncodeSnapshot(restored.Snapshot())
	if err != nil || !bytes.Equal(raw, reencoded) {
		b.Fatalf("snapshot roundtrip mismatch: encode=%v raw=%d reencoded=%d", err, len(raw), len(reencoded))
	}
	if _, err := f.store.Get(f.latestMemory.ID); err != nil {
		b.Fatalf("latest memory audit read: %v", err)
	}
	if _, err := f.store.GetPath(f.latestPath.ID); err != nil {
		b.Fatalf("latest path audit read: %v", err)
	}
	if _, err := f.store.ReusableMemoryFor(f.latestMemory.ID, f.query); err != nil {
		b.Fatalf("baseline memory reuse: %v", err)
	}
	if _, err := f.store.ReusablePathFor(f.latestPath.ID, f.query); err != nil {
		b.Fatalf("baseline path reuse: %v", err)
	}
	policy := f.store.AccessPolicy()
	if err := policy.Check([]model.ObjectID{f.latestPath.ID}, AccessAudit); err != nil {
		b.Fatalf("baseline audit policy: %v", err)
	}
	if err := policy.Check([]model.ObjectID{f.latestPath.ID}, AccessReuse); err != nil {
		b.Fatalf("baseline reuse policy: %v", err)
	}

	withdrawn, err := RestoreSnapshot(raw)
	if err != nil {
		b.Fatalf("restore withdrawal preflight: %v", err)
	}
	if _, err := withdrawn.RetractSource(f.rootSource.SourceID, Retraction{
		Kind:       RetractionUseBan,
		Reason:     "benchmark withdrawal preflight",
		SourceID:   f.rootSource.SourceID,
		RecordedAt: time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		b.Fatalf("withdrawal preflight: %v", err)
	}
	if _, err := withdrawn.Get(f.latestMemory.ID); err != nil {
		b.Fatalf("withdrawn memory audit read: %v", err)
	}
	if _, err := withdrawn.GetPath(f.latestPath.ID); err != nil {
		b.Fatalf("withdrawn path audit read: %v", err)
	}
	if _, err := withdrawn.ReusableMemoryFor(f.latestMemory.ID, f.query); !errors.Is(err, ErrNotEligible) {
		b.Fatalf("withdrawn memory reuse was not denied: %v", err)
	}
	if _, err := withdrawn.ReusablePathFor(f.latestPath.ID, f.query); !errors.Is(err, ErrNotEligible) {
		b.Fatalf("withdrawn path reuse was not denied: %v", err)
	}
	withdrawnPolicy := withdrawn.AccessPolicy()
	if err := withdrawnPolicy.Check([]model.ObjectID{f.latestPath.ID}, AccessAudit); err != nil {
		b.Fatalf("use-ban audit policy: %v", err)
	}
	if err := withdrawnPolicy.Check([]model.ObjectID{f.latestPath.ID}, AccessReuse); !errors.Is(err, ErrUseDenied) {
		b.Fatalf("use-ban reuse policy was not denied: %v", err)
	}

	disclosed, err := RestoreSnapshot(raw)
	if err != nil {
		b.Fatalf("restore disclosure preflight: %v", err)
	}
	if _, err := disclosed.RetractSource(f.rootSource.SourceID, Retraction{
		Kind:       RetractionPrivacyErasure,
		Reason:     "benchmark disclosure preflight",
		SourceID:   f.rootSource.SourceID,
		RecordedAt: time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		b.Fatalf("disclosure withdrawal preflight: %v", err)
	}
	if err := disclosed.AccessPolicy().Check([]model.ObjectID{f.latestPath.ID}, AccessAudit); !errors.Is(err, ErrHistoryAccessDenied) {
		b.Fatalf("privacy denial semantics changed: %v", err)
	}
}

func memoryBenchmarkRunSubtests(b *testing.B, fn func(*testing.B, memoryBenchmarkFixture)) {
	b.Helper()
	for _, fixture := range memoryBenchmarkFixtures(b) {
		fixture := fixture
		b.Run(fixture.name, func(b *testing.B) {
			memoryBenchmarkPreflight(b, fixture)
			b.ResetTimer()
			b.StopTimer()
			memoryBenchmarkReportMetrics(b, fixture)
			b.StartTimer()
			fn(b, fixture)
		})
	}
}

func BenchmarkMemoryStoreApplyCommand(b *testing.B) {
	memoryBenchmarkRunSubtests(b, func(b *testing.B, f memoryBenchmarkFixture) {
		candidate := Memory{
			Space:         memoryBenchmarkSpace,
			Kind:          KindDecision,
			Claim:         []byte("r16 benchmark apply command candidate"),
			Sources:       []Source{f.rootSource},
			Applicability: memoryBenchmarkApplicability(f.name),
			RecordedAt:    time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC),
		}
		command := Command{PutCandidate: &candidate}
		key := model.RequestKey{Space: memoryBenchmarkSpace, Principal: "benchmark", RequestID: "apply"}
		for i := 0; i < b.N; i++ {
			store, err := RestoreSnapshot(f.raw)
			if err != nil {
				b.Fatal(err)
			}
			result, err := store.ApplyCommand(key, command)
			if err != nil || result.Memory == nil || result.Memory.Status != StatusCandidate {
				b.Fatalf("apply command result=%+v err=%v", result, err)
			}
		}
	})
}

func BenchmarkMemoryStoreGetAndSearch(b *testing.B) {
	memoryBenchmarkRunSubtests(b, func(b *testing.B, f memoryBenchmarkFixture) {
		for i := 0; i < b.N; i++ {
			m, err := f.store.Get(f.latestMemory.ID)
			if err != nil {
				b.Fatal(err)
			}
			results, err := f.store.Search(f.query, 32)
			if err != nil || len(results) == 0 || m.ID != f.latestMemory.ID {
				b.Fatalf("get/search result=%d memory=%s err=%v", len(results), m.ID, err)
			}
		}
	})
}

func BenchmarkMemoryStoreReusableMemoryFor(b *testing.B) {
	memoryBenchmarkRunSubtests(b, func(b *testing.B, f memoryBenchmarkFixture) {
		for i := 0; i < b.N; i++ {
			if _, err := f.store.ReusableMemoryFor(f.latestMemory.ID, f.query); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMemoryStoreAccessPolicy(b *testing.B) {
	memoryBenchmarkRunSubtests(b, func(b *testing.B, f memoryBenchmarkFixture) {
		for i := 0; i < b.N; i++ {
			policy := f.store.AccessPolicy()
			if policy.HasDisclosureDenials() {
				b.Fatal("baseline fixture unexpectedly has disclosure denial")
			}
			if err := policy.Check([]model.ObjectID{f.latestPath.ID}, AccessReuse); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMemoryStoreEncodeRestoreSnapshot(b *testing.B) {
	memoryBenchmarkRunSubtests(b, func(b *testing.B, f memoryBenchmarkFixture) {
		for i := 0; i < b.N; i++ {
			raw, err := EncodeSnapshot(f.store.Snapshot())
			if err != nil {
				b.Fatal(err)
			}
			if _, err := RestoreSnapshot(raw); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMemoryStoreRetractSource(b *testing.B) {
	memoryBenchmarkRunSubtests(b, func(b *testing.B, f memoryBenchmarkFixture) {
		retraction := Retraction{
			Kind:       RetractionUseBan,
			Reason:     "r16 benchmark source withdrawal",
			SourceID:   f.rootSource.SourceID,
			RecordedAt: time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC),
		}
		for i := 0; i < b.N; i++ {
			store, err := RestoreSnapshot(f.raw)
			if err != nil {
				b.Fatal(err)
			}
			changed, err := store.RetractSource(f.rootSource.SourceID, retraction)
			if err != nil || len(changed) == 0 {
				b.Fatalf("retract changed=%d err=%v", len(changed), err)
			}
		}
	})
}
