package agentprofile

import (
	"bytes"
	"encoding/json"
)

const MaxLifecycleRecords = 65536

// lifecycleSnapshotWire uses fixed struct fields and sorted slices. JSON is
// only the private snapshot envelope; the parent snapshot still has a binary
// versioned envelope and rejects a noncanonical re-encoding.
type lifecycleSnapshotWire struct {
	Queries       []Query              `json:"queries,omitempty"`
	Activities    []Activity           `json:"activities,omitempty"`
	Attempts      []Attempt            `json:"attempts,omitempty"`
	Results       []ActivityResult     `json:"results,omitempty"`
	Messages      []Message            `json:"messages,omitempty"`
	Consumptions  []MessageConsumption `json:"consumptions,omitempty"`
	Continuations []Continuation       `json:"continuations,omitempty"`
	Recovery      *RecoverySnapshot    `json:"recovery,omitempty"`
}

func validateLifecycleSnapshot(s LifecycleSnapshot) error {
	return validateLifecycleState(s, false)
}

func validateLifecycleState(s LifecycleSnapshot, records bool) error {
	if len(s.Queries) > MaxLifecycleRecords || len(s.Activities) > MaxLifecycleRecords || len(s.Attempts) > MaxLifecycleRecords || len(s.Results) > MaxLifecycleRecords || len(s.Messages) > MaxLifecycleRecords || len(s.Consumptions) > MaxLifecycleRecords || len(s.Continuations) > MaxLifecycleRecords {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, v := range s.Queries {
		if v.ID == "" || v.Space == "" || v.Owner == "" || v.Version == 0 || seen["q/"+v.ID] {
			return ErrInvalid
		}
		seen["q/"+v.ID] = true
	}
	seen = map[string]bool{}
	for _, v := range s.Activities {
		queryOwned := false
		if s.Recovery != nil {
			for _, spec := range s.Recovery.ActivitySpecs {
				queryOwned = queryOwned || (spec.Activity == v.ID && spec.Query != "" && spec.Execution == "")
			}
		}
		if v.ID == "" || (v.Execution == "" && !queryOwned) || v.Version == 0 || v.Status == "" || seen[v.ID] {
			return ErrInvalid
		}
		seen[v.ID] = true
	}
	seen = map[string]bool{}
	for _, v := range s.Attempts {
		if v.ID == "" || v.Activity == "" || v.Sequence == 0 || v.Incarnation == 0 || v.Status == "" || seen[v.ID] {
			return ErrInvalid
		}
		seen[v.ID] = true
	}
	seen = map[string]bool{}
	for _, v := range s.Results {
		if v.ID == "" || v.Activity == "" || v.Attempt == "" || v.Incarnation == 0 || v.Outcome == "" || !validResultCapture(v) || seen[v.ID] {
			return ErrInvalid
		}
		seen[v.ID] = true
	}
	seen = map[string]bool{}
	for _, v := range s.Messages {
		if v.ID == "" || v.Sender == "" || v.Recipient == "" || v.GoalEpoch == 0 || seen[v.ID] {
			return ErrInvalid
		}
		seen[v.ID] = true
	}
	seen = map[string]bool{}
	for _, v := range s.Consumptions {
		if v.Message == "" || v.Recipient == "" || v.Incarnation == 0 || v.GoalEpoch == 0 || seen[v.Message+"/"+string(v.Recipient)] {
			return ErrInvalid
		}
		seen[v.Message+"/"+string(v.Recipient)] = true
	}
	seen = map[string]bool{}
	for _, v := range s.Continuations {
		if v.Execution == "" || v.Version == 0 || v.Incarnation == 0 || v.PendingKey == "" || v.Phase == "" || seen[string(v.Execution)] || validateContinuationPosition(v) != nil {
			return ErrInvalid
		}
		seen[string(v.Execution)] = true
	}
	return validateRecoveryShapeMode(s.Recovery, records)
}

func encodeLifecycleSnapshot(s LifecycleSnapshot) ([]byte, error) {
	if err := validateLifecycleSnapshot(s); err != nil {
		return nil, err
	}
	s = cloneLifecycle(s)
	lifecycleSort(&s)
	w := lifecycleSnapshotWire{Queries: s.Queries, Activities: s.Activities, Attempts: s.Attempts, Results: s.Results, Messages: s.Messages, Consumptions: s.Consumptions, Continuations: s.Continuations, Recovery: s.Recovery}
	b, e := json.Marshal(w)
	if e != nil {
		return nil, e
	}
	return append([]byte("ATE-AGENT-LIFECYCLE\x00"), b...), nil
}
func decodeLifecycleSnapshot(raw []byte) (LifecycleSnapshot, error) {
	const p = "ATE-AGENT-LIFECYCLE\x00"
	if !bytes.HasPrefix(raw, []byte(p)) {
		return LifecycleSnapshot{}, ErrInvalid
	}
	var w lifecycleSnapshotWire
	if err := json.Unmarshal(raw[len(p):], &w); err != nil {
		return LifecycleSnapshot{}, ErrInvalid
	}
	s := LifecycleSnapshot{Queries: w.Queries, Activities: w.Activities, Attempts: w.Attempts, Results: w.Results, Messages: w.Messages, Consumptions: w.Consumptions, Continuations: w.Continuations, Recovery: w.Recovery}
	if err := validateLifecycleSnapshot(s); err != nil {
		return LifecycleSnapshot{}, err
	}
	canonical, e := encodeLifecycleSnapshot(s)
	if e != nil || !bytes.Equal(canonical, raw) {
		return LifecycleSnapshot{}, ErrInvalid
	}
	return s, nil
}
