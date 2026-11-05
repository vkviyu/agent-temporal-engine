package storage

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"
)

// One interrupted bounded batch can add frame bytes plus segment headers and
// seals. This independent recovery ceiling also bounds diagnostic copying.
const MaxUnpublishedLayoutBytes uint64 = 128 << 20

type orphanSegment struct {
	name string
	file ReadFile
	size uint64
}

// Orphans are evidence, never an alternative source of publication authority.
// Return every acquired handle on error so the caller's cleanup retains owner.
func collectOrphans(d DurableIO, descriptors []SegmentDescriptor, report *RecoveryReport) (orphans []orphanSegment, err error) {
	names, err := d.List("segments")
	if err != nil {
		return nil, err
	}
	if len(names) > (maxCompactionIntents+3)*MaxSegments {
		return nil, ErrLimit
	}
	sort.Strings(names)
	referenced := make(map[string]bool, len(descriptors))
	for _, desc := range descriptors {
		referenced["SEGMENT."+desc.SegmentID] = true
	}
	for _, name := range names {
		if referenced[name] {
			continue
		}
		report.TailDisposition = "preserved"
		if !strings.HasPrefix(name, "SEGMENT.") {
			return orphans, fmt.Errorf("%w: unmanaged segment entry %q", ErrRecoveryRequired, name)
		}
		if _, err = metadataID(strings.TrimPrefix(name, "SEGMENT.")); err != nil {
			return orphans, fmt.Errorf("%w: unmanaged segment entry %q", ErrRecoveryRequired, name)
		}
		if len(orphans) >= MaxSegments {
			return orphans, ErrLimit
		}
		path := "segments/" + name
		f, openErr := d.OpenRead(path)
		if f != nil {
			orphans = append(orphans, orphanSegment{name: path, file: f})
		}
		if openErr != nil {
			return orphans, openErr
		}
		if f == nil {
			return orphans, fmt.Errorf("%w: orphan open returned nil handle", ErrMalformed)
		}
		size, sizeErr := f.Size()
		if sizeErr != nil {
			return orphans, sizeErr
		}
		if size < 0 || uint64(size) > MaxUnpublishedLayoutBytes || report.TailBytes > MaxUnpublishedLayoutBytes-uint64(size) {
			return orphans, ErrLimit
		}
		orphans[len(orphans)-1].size = uint64(size)
		report.TailBytes += uint64(size)
		report.OrphanSegments = append(report.OrphanSegments, path)
	}
	return orphans, nil
}

func copyEvidence(d DurableIO, name string, source io.ReaderAt, start, size uint64) (err error) {
	f, err := d.CreateExclusive(name)
	if err != nil {
		return cleanupOnError(err, f)
	}
	defer closeInto(&err, f)
	reader := io.NewSectionReader(source, int64(start), int64(size))
	buf := make([]byte, 64<<10)
	for size > 0 {
		n := uint64(len(buf))
		if n > size {
			n = size
		}
		if _, err = io.ReadFull(reader, buf[:int(n)]); err != nil {
			return err
		}
		if err = writeAll(f, buf[:int(n)]); err != nil {
			return err
		}
		size -= n
	}
	return f.Sync()
}

// Every evidence file AND its directory entry becomes durable before the
// first destructive operation. A failed retry may leave extra evidence copies.
func quarantineLayout(d DurableIO, active *journalSegment, orphans []orphanSegment, report *RecoveryReport) error {
	if err := d.Mkdir("quarantine"); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	if err := d.SyncDir("."); err != nil {
		return err
	}
	id, err := randomID()
	if err != nil {
		return err
	}
	activeSize, err := active.file.Size()
	if err != nil {
		return err
	}
	if activeSize < 0 || uint64(activeSize) < active.descriptor.DataEnd {
		return fmt.Errorf("%w: active size changed during quarantine", ErrCorruption)
	}
	tail := uint64(activeSize) - active.descriptor.DataEnd
	if tail > 0 {
		name := "quarantine/TAIL." + id
		if err = copyEvidence(d, name, active.file, active.descriptor.DataEnd, tail); err != nil {
			return err
		}
		report.QuarantineFile = name
		report.QuarantineFiles = append(report.QuarantineFiles, name)
	}
	for i := range orphans {
		name := "quarantine/ORPHAN." + id + "." + strings.TrimPrefix(orphans[i].name, "segments/SEGMENT.")
		if err = copyEvidence(d, name, orphans[i].file, 0, orphans[i].size); err != nil {
			return err
		}
		report.QuarantineFiles = append(report.QuarantineFiles, name)
	}
	if err = d.SyncDir("quarantine"); err != nil {
		return err
	}
	if tail > 0 {
		if err = active.file.Truncate(int64(active.descriptor.DataEnd)); err != nil {
			return err
		}
		if err = active.file.Sync(); err != nil {
			return err
		}
	}
	for _, orphan := range orphans {
		if err = orphan.file.Close(); err != nil {
			return err
		}
		if err = d.Remove(orphan.name); err != nil {
			return err
		}
	}
	if err = d.SyncDir("segments"); err != nil {
		return err
	}
	report.TailDisposition = "quarantined"
	return nil
}
