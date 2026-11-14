package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
)

type recoveryMetadata struct {
	current     Current
	identity    Identity
	manifest    Manifest
	compactions []stagedCompaction
	genesis     Position
	descriptors []SegmentDescriptor
}

func readRecoveryMetadata(d DurableIO, o JournalOptions) (recoveryMetadata, error) {
	var out recoveryMetadata
	currentBytes, err := readMetadata(d, "CURRENT")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return out, fmt.Errorf("%w: CURRENT missing: %w", ErrIncompleteStore, err)
		}
		return out, recoveryInputError("CURRENT", err)
	}
	out.current, err = DecodeCurrent(currentBytes)
	if err != nil {
		return out, recoveryInputError("CURRENT", err)
	}
	idBytes, err := readMetadata(d, "IDENTITY")
	if err != nil {
		return out, recoveryInputError("IDENTITY", err)
	}
	if Digest(sha256.Sum256(idBytes)) != out.current.IdentityDigest {
		return out, fmt.Errorf("%w: identity digest", ErrCorruption)
	}
	out.identity, err = DecodeIdentity(idBytes)
	if err != nil {
		return out, recoveryInputError("IDENTITY", err)
	}
	if out.identity.Store != out.current.Store {
		return out, fmt.Errorf("%w: identity/store mismatch", ErrCorruption)
	}
	if out.identity.Profile != d.Profile() {
		return out, fmt.Errorf("%w: durability profile differs from identity", ErrUnsupported)
	}
	manifestBytes, err := readMetadata(d, manifestName(out.current.ManifestID))
	if err != nil {
		return out, recoveryInputError("manifest", err)
	}
	if Digest(sha256.Sum256(manifestBytes)) != out.current.ManifestDigest {
		return out, fmt.Errorf("%w: manifest digest", ErrCorruption)
	}
	out.manifest, err = DecodeManifest(manifestBytes)
	if err != nil {
		return out, recoveryInputError("manifest", err)
	}
	if out.manifest.Store != out.identity.Store || out.manifest.Generation != out.current.Generation ||
		out.manifest.ManifestID != out.current.ManifestID {
		return out, fmt.Errorf("%w: CURRENT/manifest binding", ErrCorruption)
	}
	if err = validateRetirementSource(d, out.manifest); err != nil {
		return out, err
	}
	out.compactions, err = readCompactionIntents(d, out.identity.Store)
	if err != nil {
		return out, err
	}
	if out.manifest.PublishedEnd > o.MaxPublishedBytes {
		return out, ErrLimit
	}
	if out.manifest.Position.LSN > o.MaxPublishedTransactions {
		return out, ErrLimit
	}
	out.genesis, err = GenesisPosition(out.identity)
	if err != nil {
		return out, err
	}
	out.descriptors = manifestDescriptors(out.manifest, out.genesis)
	return out, nil
}
