package reference_test

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/graph"
	"github.com/vkviyu/agent-temporal-engine/model"
	oracle "github.com/vkviyu/agent-temporal-engine/tests/reference"
)

func mergeEntries(state map[string]string) []graph.Entry {
	keys := []string{}
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []graph.Entry{}
	for _, k := range keys {
		out = append(out, graph.Entry{Key: []byte(k), Value: model.ObjectID(state[k])})
	}
	return out
}
func mergeState(entries []graph.Entry) map[string]string {
	out := map[string]string{}
	for _, e := range entries {
		out[string(e.Key)] = string(e.Value)
	}
	return out
}
func mergeChanges(changes []graph.Change) map[string]oracle.MergeConflict {
	out := map[string]oracle.MergeConflict{}
	for _, c := range changes {
		out[string(c.Key)] = oracle.MergeConflict{Left: string(c.Before), Right: string(c.After)}
	}
	return out
}
func TestIndependentMergeAllPerKeyDecisionsAndExplicitResolution(t *testing.T) {
	values := []string{"", string(digest("a")), string(digest("b"))}
	for _, b := range values {
		for _, l := range values {
			for _, r := range values {
				label := fmt.Sprintf("%s/%s/%s", shortMerge(b), shortMerge(l), shortMerge(r))
				t.Run(label, func(t *testing.T) {
					states := []map[string]string{{"stable": string(digest("c"))}, {"stable": string(digest("c"))}, {"stable": string(digest("c"))}}
					for i, v := range []string{b, l, r} {
						if v != "" {
							states[i]["key"] = v
						}
					}
					store := graph.New()
					revisions := make([]graph.Revision, 3)
					for i := range revisions {
						parent := model.RevisionID("")
						if i > 0 {
							parent = revisions[0].ID
						}
						rev, err := graph.BuildRevision("space", parent, nil, digest("d"), []model.EventID{model.EventID(digest(fmt.Sprintf("%x", i+1)))}, mergeEntries(states[i]))
						if err != nil {
							t.Fatal(err)
						}
						if err := store.PutRevision(rev); err != nil {
							t.Fatal(err)
						}
						revisions[i] = rev
					}
					want := oracle.Merge(states[0], states[1], states[2])
					got, err := store.PlanJoin(revisions[1].ID, revisions[2].ID)
					if err != nil {
						t.Fatal(err)
					}
					conflicts := map[string]oracle.MergeConflict{}
					for _, c := range got.Conflicts {
						conflicts[string(c.Key)] = oracle.MergeConflict{Base: string(c.Base), Left: string(c.Left), Right: string(c.Right)}
					}
					if got.Base != revisions[0].ID || !reflect.DeepEqual(mergeState(got.State), want.State) || !reflect.DeepEqual(conflicts, want.Conflicts) || !reflect.DeepEqual(mergeChanges(got.Changes), want.ChangesFrom(states[1])) {
						t.Fatalf("merge plan mismatch got=%+v want=%+v", got, want)
					}
					if len(want.Conflicts) > 0 {
						if _, err := store.PreviewJoin(got, digest("e"), nil); !errors.Is(err, graph.ErrJoinConflict) {
							t.Fatalf("unresolved merge accepted: %v", err)
						}
						for _, resolution := range []string{"", string(digest("a")), string(digest("b")), string(digest("f"))} {
							candidate := got
							candidate.State = append([]graph.Entry(nil), got.State...)
							candidate.Conflicts = append([]graph.Conflict(nil), got.Conflicts...)
							expected := want.Clone()
							if err := candidate.Resolve([]byte("key"), model.ObjectID(resolution)); err != nil {
								t.Fatal(err)
							}
							if err := expected.Resolve("key", resolution); err != nil {
								t.Fatal(err)
							}
							if !reflect.DeepEqual(mergeState(candidate.State), expected.State) || len(candidate.Conflicts) != 0 || !reflect.DeepEqual(mergeChanges(candidate.Changes), expected.ChangesFrom(states[1])) {
								t.Fatal("explicit decision differed")
							}
							joined, err := store.PreviewJoin(candidate, digest("e"), nil)
							if err != nil || !reflect.DeepEqual(mergeState(joined.Entries), expected.State) {
								t.Fatalf("preview: %v", err)
							}
							forged := candidate
							forged.State = append([]graph.Entry(nil), candidate.State...)
							for i := range forged.State {
								if string(forged.State[i].Key) == "stable" {
									forged.State[i].Value = digest("f")
								}
							}
							if _, err := store.PreviewJoin(forged, digest("e"), nil); !errors.Is(err, graph.ErrInvalidJoinPlan) {
								t.Fatalf("invented state accepted: %v", err)
							}
						}
					} else {
						joined, err := store.PreviewJoin(got, digest("e"), nil)
						if err != nil || !reflect.DeepEqual(mergeState(joined.Entries), want.State) {
							t.Fatalf("automatic merge differs: %v", err)
						}
						if err := got.Resolve([]byte("stable"), digest("a")); !errors.Is(err, graph.ErrJoinConflict) {
							t.Fatalf("nonconflict resolution admitted: %v", err)
						}
					}
				})
			}
		}
	}
}
func shortMerge(v string) string {
	if v == "" {
		return "absent"
	}
	return v[:1]
}
