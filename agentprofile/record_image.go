package agentprofile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"

	"github.com/vkviyu/agent-temporal-engine/codec"
	"github.com/vkviyu/agent-temporal-engine/model"
)

// ErrStateLimit means a record-state resource budget was exceeded, not that
// the state is malformed. Durable readers must treat an over-budget stored
// representation as invalid stored data, never as a rejected new command.
var ErrStateLimit = errors.New("agent record state resource limit exceeded")

const (
	RecordStateVersion  = 1
	MaxStateRecordBytes = 4 << 20
	MaxStateRecords     = 16384
	MaxStateBytes       = 256 << 20
	recordDomain        = "ATE-AGENT-RECORD\x00"
)

// StateRecord is one canonical typed record, never a fragment of a snapshot.
// Key includes its content digest and duplicate ordinal (for legacy multisets).
type StateRecord struct {
	Key []byte
	Raw []byte
}

// RecordImage is an authenticated, normalized state image for the record-backed
// Agent state format. Copies share only private immutable backing state.
type RecordImage struct {
	snapshot *Snapshot
	records  []StateRecord
	bytes    uint64
}

// recordSection binds a fixed format tag to a statically selected Go record
// type. No caller-selected type, map, interface or reflection decoder is used.
type recordSection struct {
	encode func(func([]byte) error) error
	decode func(raw []byte, generated bool) error
}

func recordSlice[T any](xs *[]T) recordSection {
	return normalizedRecordSlice(xs, func(value T) T { return value })
}

func normalizedRecordSlice[T any](xs *[]T, normalize func(T) T) recordSection {
	return recordSection{
		encode: func(emit func([]byte) error) error {
			if len(*xs) > MaxStateRecords {
				return ErrStateLimit
			}
			for _, v := range *xs {
				v = normalize(v)
				// Check a conservative JSON allocation bound before marshaling.
				if recordAllocation(reflect.ValueOf(v), 0) > MaxStateRecordBytes {
					return ErrStateLimit
				}
				raw, err := json.Marshal(v)
				if err != nil {
					return err
				}
				if err = emit(raw); err != nil {
					return err
				}
			}
			return nil
		},
		decode: func(raw []byte, generated bool) error {
			if len(*xs) >= MaxStateRecords {
				return ErrStateLimit
			}
			checkJSON := boundedRecordJSON
			if generated {
				checkJSON = boundedGeneratedRecordJSON
			}
			if err := checkJSON(raw); err != nil {
				return err
			}
			var v T
			d := json.NewDecoder(bytes.NewReader(raw))
			d.DisallowUnknownFields()
			if err := d.Decode(&v); err != nil {
				return ErrInvalid
			}
			v = normalize(v)
			if recordAllocation(reflect.ValueOf(v), 0) > MaxStateRecordBytes {
				return ErrStateLimit
			}
			// These closed structs have no custom JSON methods. Fresh Marshal
			// output is canonical except invalid UTF-8 replacement: Marshal emits
			// \ufffd, whereas marshaling the decoded rune emits literal UTF-8.
			// Keep the exact comparison in that case (and for all public input).
			if !generated || bytes.Contains(raw, []byte(`\ufffd`)) {
				canonical, err := json.Marshal(v)
				if err != nil || !bytes.Equal(raw, canonical) {
					return ErrInvalid
				}
			}
			*xs = append(*xs, v)
			return nil
		},
	}
}

// Only for bytes just produced by json.Marshal on our closed record structs.
// Count the same tokens/depth as Decoder.Token without allocating token values.
// This is not a JSON validator; untrusted bytes always use boundedRecordJSON.
func boundedGeneratedRecordJSON(raw []byte) error {
	depth, tokens := 0, 0
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case ',', ':', ' ', '\n', '\r', '\t':
			continue
		case '{', '[':
			depth++
		case '}', ']':
			depth--
		case '"':
			for i++; i < len(raw); i++ {
				if raw[i] == '\\' {
					i++
				} else if raw[i] == '"' {
					break
				}
			}
		default: // a number, boolean, or null
			for i+1 < len(raw) && raw[i+1] != ',' && raw[i+1] != '}' && raw[i+1] != ']' {
				i++
			}
		}
		tokens++
		if depth > 32 || tokens > 65536 {
			return ErrStateLimit
		}
	}
	return nil
}

// Bound container expansion before the typed decoder allocates nested slices.
func boundedRecordJSON(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	depth, tokens := 0, 0
	for {
		token, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return ErrInvalid
		}
		tokens++
		if tokens > 65536 {
			return ErrStateLimit
		}
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
			if depth > 32 {
				return ErrStateLimit
			}
			if depth < 0 {
				return ErrInvalid
			}
		}
	}
	if depth != 0 {
		return ErrInvalid
	}
	return nil
}

func binaryRecordSlice[T any](xs *[]T, encode func(T) ([]byte, error), decode func([]byte) (T, error)) recordSection {
	return recordSection{
		encode: func(emit func([]byte) error) error {
			if len(*xs) > MaxStateRecords {
				return ErrStateLimit
			}
			for _, value := range *xs {
				if recordAllocation(reflect.ValueOf(value), 0) > MaxStateRecordBytes {
					return ErrStateLimit
				}
				raw, err := encode(value)
				if err != nil {
					return err
				}
				if err = emit(raw); err != nil {
					return err
				}
			}
			return nil
		},
		decode: func(raw []byte, _ bool) error {
			if len(*xs) >= MaxStateRecords {
				return ErrStateLimit
			}
			value, err := decode(raw)
			if err != nil {
				return err
			}
			*xs = append(*xs, value)
			return nil
		},
	}
}

// This is an allocation preflight, not a wire codec. Saturate early so nested
// caller data cannot cause an unbounded marshal allocation or integer overflow.
func recordAllocation(v reflect.Value, depth int) int {
	if depth > 32 {
		return MaxStateRecordBytes + 1
	}
	n := 32
	add := func(x int) {
		if n <= MaxStateRecordBytes {
			n += x
		}
	}
	switch v.Kind() {
	case reflect.Pointer:
		if !v.IsNil() {
			add(recordAllocation(v.Elem(), depth+1))
		}
	case reflect.Struct:
		for i := 0; i < v.NumField() && n <= MaxStateRecordBytes; i++ {
			add(len(v.Type().Field(i).Name) + recordAllocation(v.Field(i), depth+1))
		}
	case reflect.String:
		if v.Len() > MaxStateRecordBytes/6 {
			return MaxStateRecordBytes + 1
		}
		add(6 * v.Len())
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			if v.Len() > MaxStateRecordBytes/2 {
				return MaxStateRecordBytes + 1
			}
			add(4 * ((v.Len() + 2) / 3))
		} else {
			if v.Len() > MaxStateRecords {
				return MaxStateRecordBytes + 1
			}
			for i := 0; i < v.Len() && n <= MaxStateRecordBytes; i++ {
				add(recordAllocation(v.Index(i), depth+1))
			}
		}
	}
	return n
}

// Tags are explicit and append-only. Tag 0 is the recovery-presence marker;
// empty-but-present Recovery must survive independently of its child records.
func recordSections(s *Snapshot) []recordSection {
	if s.Lifecycle.Recovery == nil {
		s.Lifecycle.Recovery = &RecoverySnapshot{}
	}
	l, r := &s.Lifecycle, s.Lifecycle.Recovery
	return []recordSection{
		0: {}, // recovery presence, handled separately
		1: binaryRecordSlice(&s.Tasks, EncodeTask, DecodeTask),
		2: binaryRecordSlice(&s.Executions, EncodeExecution, DecodeExecution),
		3: binaryRecordSlice(&s.Steps, EncodeStep, DecodeStep),
		4: binaryRecordSlice(&s.Invocations, EncodeInvocation, DecodeInvocation),
		5: binaryRecordSlice(&s.Actions, EncodePendingAction, DecodePendingAction),
		6: recordSlice(&l.Queries),
		7: recordSlice(&l.Activities),
		8: recordSlice(&l.Attempts),
		9: normalizedRecordSlice(&l.Results, func(value ActivityResult) ActivityResult {
			// Match cloneLifecycle normalization so the record ID is stable for
			// empty output chunks.
			if len(value.OutputChunks) == 0 {
				value.OutputChunks = nil
			}
			return value
		}),
		10: recordSlice(&l.Messages),
		11: recordSlice(&l.Consumptions),
		12: recordSlice(&l.Continuations),
		13: recordSlice(&r.Authorities),
		14: recordSlice(&r.ActivitySpecs),
		15: recordSlice(&r.ActivityControls),
		16: recordSlice(&r.AttemptControls),
		17: recordSlice(&r.Captures),
		18: recordSlice(&r.Acceptances),
		19: recordSlice(&r.Settlements),
		20: recordSlice(&r.Reconciliations),
		21: recordSlice(&r.Answers),
		22: recordSlice(&r.Starts),
		23: recordSlice(&r.TaskLinks),
		24: recordSlice(&r.Goals),
		25: recordSlice(&r.Outcomes),
		26: recordSlice(&r.Actions),
		27: recordSlice(&r.Resolutions),
		28: recordSlice(&r.Resumes),
		29: recordSlice(&r.Joins),
		30: recordSlice(&r.Imports),
		31: recordSlice(&r.ValidationRequirements),
		32: recordSlice(&r.IdempotencyGuarantees),
		33: recordSlice(&r.RetryAuthorizations),
		34: recordSlice(&r.OutputChunks),
	}
}

func frameStateRecord(tag int, body []byte) []byte {
	b := codec.AppendUint([]byte(recordDomain), RecordStateVersion)
	b = codec.AppendUint(b, uint64(tag))
	return append(b, body...)
}

func decodeStateRecord(raw []byte, sections []recordSection) (uint64, error) {
	if len(raw) > MaxStateRecordBytes {
		return 0, ErrStateLimit
	}
	if !bytes.HasPrefix(raw, []byte(recordDomain)) {
		return 0, ErrInvalid
	}
	d := codec.NewDecoder(raw[len(recordDomain):])
	version, err := d.Uint()
	if err != nil || version != RecordStateVersion {
		return 0, ErrInvalid
	}
	tag, err := d.Uint()
	if err != nil || tag >= uint64(len(sections)) {
		return 0, ErrInvalid
	}
	body := raw[len(raw)-d.Remaining():]
	if tag == 0 {
		if !bytes.Equal(body, []byte("{}")) {
			return 0, ErrInvalid
		}
	} else if err := sections[tag].decode(body, false); err != nil {
		return 0, err
	}
	return tag, nil
}

// ValidateStateRecord authenticates the closed per-record schema and canonical
// encoding. Cross-record invariants are checked by DecodeRecordImage.
func ValidateStateRecord(raw []byte) error {
	s := Snapshot{}
	_, err := decodeStateRecord(raw, recordSections(&s))
	return err
}

// ValidateStateRecordScope checks a single canonical record and any explicit
// partition identity it carries, without requiring its cross-record relations.
func ValidateStateRecordScope(raw []byte, space model.SpaceID, owner model.PrincipalID) error {
	s := Snapshot{}
	if _, err := decodeStateRecord(raw, recordSections(&s)); err != nil {
		return err
	}
	return (RecordImage{snapshot: &s}).CheckScope(space, owner)
}

func stateRecordKey(raw []byte, ordinal uint64) []byte {
	sum := sha256.Sum256(raw)
	return binary.BigEndian.AppendUint64(append([]byte(nil), sum[:]...), ordinal)
}

// EncodeRecordImage normalizes through the typed decoder before validating the
// complete state.
func EncodeRecordImage(s Snapshot) (RecordImage, error) {
	present := s.Lifecycle.Recovery != nil
	sections := recordSections(&s)
	var records []StateRecord
	total := 0
	emit := func(tag int, body []byte) error {
		if len(records) >= MaxStateRecords || len(body)+len(recordDomain)+2 > MaxStateRecordBytes ||
			len(body)+len(recordDomain)+2 > MaxStateBytes-total {
			return ErrStateLimit
		}
		raw := frameStateRecord(tag, body)
		total += len(raw)
		records = append(records, StateRecord{Key: stateRecordKey(raw, 0), Raw: raw})
		return nil
	}
	if present {
		if err := emit(0, []byte("{}")); err != nil {
			return RecordImage{}, err
		}
	}
	for tag := 1; tag < len(sections); tag++ {
		if err := sections[tag].encode(func(b []byte) error { return emit(tag, b) }); err != nil {
			return RecordImage{}, err
		}
	}
	// Content keys make inserting a new identity independent of slice position.
	sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i].Key, records[j].Key) < 0 })
	var ordinal uint64
	for i := range records {
		if i > 0 && bytes.Equal(records[i-1].Raw, records[i].Raw) {
			ordinal++
		} else {
			ordinal = 0
		}
		binary.BigEndian.PutUint64(records[i].Key[32:], ordinal)
	}
	return decodeRecordImage(records, true)
}

func DecodeRecordImage(records []StateRecord) (RecordImage, error) {
	return decodeRecordImage(records, false)
}

// generated is private to EncodeRecordImage: its framed records and keys were
// just allocated, bounded, hashed and sorted here, and can be retained directly.
// Both paths still decode typed values and validate the complete semantic state.
func decodeRecordImage(records []StateRecord, generated bool) (RecordImage, error) {
	if len(records) > MaxStateRecords {
		return RecordImage{}, ErrStateLimit
	}
	s := Snapshot{}
	sections := recordSections(&s)
	present, recoveryChildren := false, false
	var total uint64
	owned := records
	if !generated {
		owned = make([]StateRecord, 0, len(records))
	}
	ordinals := make(map[[32]byte]uint64)
	for i, record := range records {
		if len(record.Raw) > MaxStateRecordBytes || uint64(len(record.Raw)) > MaxStateBytes-total {
			return RecordImage{}, ErrStateLimit
		}
		total += uint64(len(record.Raw))
		var tag uint64
		var err error
		if generated {
			// Schema v1 tags are single-byte integers, framed by our encoder.
			tag = uint64(record.Raw[len(recordDomain)+1])
			if tag != 0 {
				err = sections[tag].decode(record.Raw[len(recordDomain)+2:], true)
			}
		} else {
			if i > 0 && bytes.Compare(records[i-1].Key, record.Key) >= 0 {
				return RecordImage{}, ErrInvalid
			}
			sum := sha256.Sum256(record.Raw)
			ordinal := ordinals[sum]
			if len(record.Key) != 40 || !bytes.Equal(record.Key[:32], sum[:]) || binary.BigEndian.Uint64(record.Key[32:]) != ordinal {
				return RecordImage{}, ErrInvalid
			}
			ordinals[sum]++
			tag, err = decodeStateRecord(record.Raw, sections)
			owned = append(owned, StateRecord{bytes.Clone(record.Key), bytes.Clone(record.Raw)})
		}
		if err != nil {
			return RecordImage{}, err
		}
		if tag == 0 {
			if present {
				return RecordImage{}, ErrInvalid
			}
			present = true
		}
		recoveryChildren = recoveryChildren || tag >= 13
	}
	if !present {
		if recoveryChildren {
			return RecordImage{}, ErrInvalid
		}
		s.Lifecycle.Recovery = nil
	}
	if err := s.validate(true); err != nil {
		return RecordImage{}, err
	}
	// Normalize ordering, including recovery ordering, without collecting all
	// lifecycle records into one monolithic byte string.
	sort.Slice(s.Tasks, func(i, j int) bool { return s.Tasks[i].ID < s.Tasks[j].ID })
	sort.Slice(s.Executions, func(i, j int) bool { return s.Executions[i].ID < s.Executions[j].ID })
	sort.Slice(s.Steps, func(i, j int) bool {
		if s.Steps[i].Execution != s.Steps[j].Execution {
			return s.Steps[i].Execution < s.Steps[j].Execution
		}
		return s.Steps[i].Sequence < s.Steps[j].Sequence
	})
	sort.Slice(s.Invocations, func(i, j int) bool { return s.Invocations[i].ID < s.Invocations[j].ID })
	sort.Slice(s.Actions, func(i, j int) bool { return s.Actions[i].Key < s.Actions[j].Key })
	lifecycleSort(&s.Lifecycle)
	return RecordImage{snapshot: &s, records: owned, bytes: total}, nil
}

func (image RecordImage) Restore() (*Ledger, error) {
	if image.snapshot == nil {
		return nil, ErrInvalid
	}
	return restoreValidatedSnapshot(*image.snapshot), nil
}
func (image RecordImage) Snapshot() (Snapshot, error) {
	if image.snapshot == nil {
		return Snapshot{}, ErrInvalid
	}
	return cloneSnapshot(*image.snapshot), nil
}
func (image RecordImage) CheckScope(space model.SpaceID, owner model.PrincipalID) error {
	if image.snapshot == nil || space == "" || owner == "" {
		return ErrInvalid
	}
	for _, task := range image.snapshot.Tasks {
		if task.Space != space || task.Owner != owner {
			return ErrCommandScope
		}
	}
	for _, q := range image.snapshot.Lifecycle.Queries {
		if q.Space != space || q.Owner != owner {
			return ErrCommandScope
		}
	}
	return nil
}
func (image RecordImage) Records() ([]StateRecord, error) {
	if image.snapshot == nil {
		return nil, ErrInvalid
	}
	out := make([]StateRecord, len(image.records))
	for i, r := range image.records {
		out[i] = StateRecord{bytes.Clone(r.Key), bytes.Clone(r.Raw)}
	}
	return out, nil
}

// RecordKeys returns owned keys in canonical order, without copying payloads.
// RecordAt can then copy only records missing from a persistent content index.
func (image RecordImage) RecordKeys() ([][]byte, error) {
	if image.snapshot == nil {
		return nil, ErrInvalid
	}
	keys := make([][]byte, len(image.records))
	for i, record := range image.records {
		keys[i] = bytes.Clone(record.Key)
	}
	return keys, nil
}

// RecordAt returns an owned record at the same index used by RecordKeys.
func (image RecordImage) RecordAt(index int) (StateRecord, error) {
	if image.snapshot == nil || index < 0 || index >= len(image.records) {
		return StateRecord{}, ErrInvalid
	}
	record := image.records[index]
	return StateRecord{bytes.Clone(record.Key), bytes.Clone(record.Raw)}, nil
}

func (image RecordImage) Size() uint64 { return image.bytes }
