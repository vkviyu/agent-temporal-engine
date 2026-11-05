package reference_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/memory"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func TestIndependentPolicyMatchesSharedSourceDAGWithdrawals(t *testing.T) {
	for _, kind := range []memory.RetractionKind{memory.RetractionFactualCorrection, memory.RetractionUseBan, memory.RetractionPrivacyErasure, memory.RetractionACLRevocation} {
		t.Run(string(kind), func(t *testing.T) {
			s := memory.New()
			want := oracle.NewPolicy()
			rng := rand.New(rand.NewSource(8149))
			base := []model.ObjectID{digest("a"), digest("b"), digest("c")}
			ids := append([]model.ObjectID(nil), base...)
			anchor := model.SnapshotAnchor{Space: "space", Fence: model.CommitFence{Store: "store", LSN: 1, CommitDigest: model.CommitDigest(digest("d"))}}
			for i := range 32 {
				sources := []memory.Source{{SourceID: ids[rng.Intn(len(ids))], Anchor: anchor}, {SourceID: ids[rng.Intn(len(ids))], Anchor: anchor}}
				m, err := s.PutCandidate(memory.Memory{Space: "space", Kind: memory.KindFact, Claim: []byte(fmt.Sprintf("fact-%d", i)), Sources: sources})
				if err != nil {
					t.Fatal(err)
				}
				want.Sources[string(m.ID)] = []string{string(sources[0].SourceID), string(sources[1].SourceID)}
				ids = append(ids, m.ID)
			}
			for _, seed := range base {
				if _, err := s.RetractSource(seed, memory.Retraction{Kind: kind, Reason: "test withdrawal"}); err != nil {
					t.Fatal(err)
				}
				want.Withdraw(string(seed), string(kind))
				for _, id := range ids {
					for _, reuse := range []bool{false, true} {
						purpose := memory.AccessAudit
						if reuse {
							purpose = memory.AccessReuse
						}
						expected := want.Check([]string{string(id)}, reuse)
						actual := s.AccessPolicy().Check([]model.ObjectID{id}, purpose)
						if (expected == nil) != (actual == nil) {
							t.Fatalf("source=%s item=%s reuse=%v expected=%v actual=%v", seed, id, reuse, expected, actual)
						}
					}
				}
			}
		})
	}
}
