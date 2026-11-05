// Package formats exposes ATE's machine-readable compatibility registry.
// It describes byte formats and admission policy; it is not an alternative
// decoder, authority, or journal migration engine.
package formats

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

//go:embed registry.json
var currentJSON []byte

const RegistrySchema uint64 = 1

var (
	ErrRegistry          = errors.New("invalid format registry")
	ErrUnsupported       = errors.New("unregistered format or version")
	ErrReplayOnly        = errors.New("format version is replay-only")
	ErrMigrationRequired = errors.New("explicit semantic migration is required")
)

type Purpose string

const (
	Decode Purpose = "decode"
	Replay Purpose = "replay"
	Write  Purpose = "write"
)

type Version struct {
	Number uint64 `json:"number"`
	Decode bool   `json:"decode"`
	Replay bool   `json:"replay"`
	Write  bool   `json:"write"`
}

// Family identifies an exact version-selection namespace. Selector distinguishes
// schema numbers from operation numbers; these must never be interchanged.
type Family struct {
	ID        string    `json:"id"`
	Selector  string    `json:"selector"`
	Domain    string    `json:"domain"`
	Layout    string    `json:"layout,omitempty"`
	Status    string    `json:"status"`
	Migration string    `json:"migration"`
	Versions  []Version `json:"versions"`
}

type Capability struct {
	ID      string   `json:"id"`
	Methods []string `json:"methods"`
}

type Registry struct {
	Schema       uint64       `json:"schema"`
	Release      string       `json:"release"`
	Status       string       `json:"status"`
	Families     []Family     `json:"families"`
	Capabilities []Capability `json:"capabilities"`
}

// Current returns an owned registry. Changing it cannot mutate the embedded
// compatibility contract used by another caller.
func Current() (Registry, error) { return Parse(currentJSON) }

// JSON returns the machine-readable registry without exposing shared storage.
func JSON() []byte { return bytes.Clone(currentJSON) }

// Fingerprint identifies these exact registry artifact bytes. It is not a
// journal CommitDigest and grants no authority to use registered formats.
func Fingerprint() [sha256.Size]byte { return sha256.Sum256(currentJSON) }

// Parse accepts one bounded, canonical registry document. Exact re-encoding
// rejects duplicate JSON keys as well as alternate fields, ordering and trailing
// content; registry bytes are themselves an auditable release artifact.
func Parse(raw []byte) (Registry, error) {
	if len(raw) == 0 || len(raw) > 1<<20 {
		return Registry{}, ErrRegistry
	}
	var r Registry
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return Registry{}, fmt.Errorf("%w: %v", ErrRegistry, err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return Registry{}, ErrRegistry
	}
	if err := r.Validate(); err != nil {
		return Registry{}, err
	}
	canonical, err := json.MarshalIndent(r, "", "  ")
	if err != nil || !bytes.Equal(append(canonical, '\n'), raw) {
		return Registry{}, ErrRegistry
	}
	return r, nil
}

func (r Registry) Validate() error {
	if r.Schema != RegistrySchema || r.Release != "v1.0.0" || r.Status != "frozen" || len(r.Families) == 0 || len(r.Capabilities) == 0 {
		return ErrRegistry
	}
	for i, f := range r.Families {
		if f.ID == "" || f.Domain == "" || f.Status != "frozen" || f.Migration != "preserve-history" || (f.Selector != "schema" && f.Selector != "opcode") || len(f.Versions) == 0 || (i > 0 && r.Families[i-1].ID >= f.ID) {
			return ErrRegistry
		}
		for j, v := range f.Versions {
			if v.Number == 0 || !v.Decode || (v.Write && !v.Replay) || (j > 0 && f.Versions[j-1].Number >= v.Number) {
				return ErrRegistry
			}
		}
	}
	methods := map[string]bool{}
	for i, c := range r.Capabilities {
		if c.ID == "" || (i > 0 && r.Capabilities[i-1].ID >= c.ID) {
			return ErrRegistry
		}
		for j, method := range c.Methods {
			if method == "" || methods[method] || (j > 0 && c.Methods[j-1] >= method) {
				return ErrRegistry
			}
			methods[method] = true
		}
	}
	return nil
}

func (r Registry) Family(id string) (Family, bool) {
	i := sort.Search(len(r.Families), func(i int) bool { return r.Families[i].ID >= id })
	if i == len(r.Families) || r.Families[i].ID != id {
		return Family{}, false
	}
	f := r.Families[i]
	f.Versions = append([]Version(nil), f.Versions...)
	return f, true
}

// Check describes policy only. A success never substitutes for canonical byte,
// ownership, replay, or operation validation in the format's actual decoder.
func (r Registry) Check(id string, number uint64, purpose Purpose) error {
	f, ok := r.Family(id)
	if !ok {
		return ErrUnsupported
	}
	for _, v := range f.Versions {
		if v.Number != number {
			continue
		}
		switch purpose {
		case Decode:
			if v.Decode {
				return nil
			}
		case Replay:
			if v.Replay {
				return nil
			}
		case Write:
			if v.Write {
				return nil
			}
			return ErrReplayOnly
		default:
			return ErrRegistry
		}
		return ErrUnsupported
	}
	return ErrUnsupported
}

// MigrationPlan permits byte-for-byte preservation only. Re-encoding an old
// record under a new schema would change identities and receipts, so unsupported
// conversions fail closed rather than pretending that missing metadata can be
// recovered from a new decoder. An application-level export/import must create
// new identities with an explicit source mapping and preserve the old store.
type MigrationPlan struct {
	Family                                              string
	Source, Destination                                 uint64
	PreserveBytes, PreserveIdentities, PreserveReceipts bool
}

func (r Registry) PlanMigration(id string, source, destination uint64) (MigrationPlan, error) {
	if err := r.Check(id, source, Replay); err != nil {
		return MigrationPlan{}, err
	}
	if err := r.Check(id, destination, Decode); err != nil {
		return MigrationPlan{}, err
	}
	if source != destination {
		return MigrationPlan{}, ErrMigrationRequired
	}
	return MigrationPlan{Family: id, Source: source, Destination: destination, PreserveBytes: true, PreserveIdentities: true, PreserveReceipts: true}, nil
}
