package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
)

var (
	errFaultIO     = errors.New("faultio: injected I/O failure")
	errFaultLocked = errors.New("faultio: owner lock already held")
)

// faultCall identifies one attempted mutating operation. Step is one-based and
// advances once per call, including calls that return an injected error.
// Op is a DurableIO method name, Write/Truncate/Sync/Close, or OwnerLock.Close.
// Name and To are relative paths; the owner lock uses Name ".".
type faultCall struct {
	Step int
	Op   string
	Name string
	To   string
}

type faultBoundary struct {
	faultCall
	After bool
}

type faultNode struct {
	directory bool
	data      []byte
	synced    []byte
	entries   map[string]uint64
	durable   map[string]uint64
}

// faultIO is a deliberately small model of DurableIO's file-sync contract. File
// Sync copies inode bytes; SyncDir copies only that directory's direct names.
// Neither operation implies the other, and a new inode starts with no durable
// bytes. A crash follows durable names from the root and restores only synced
// bytes. Orphaned inodes and volatile directory trees disappear.
//
// This is a contract model, not a claim to simulate an OS, filesystem, device,
// torn sectors, write reordering, or every possible power-loss outcome. Namespace
// operations are indivisible at this interface boundary; cross-directory
// installation can still leave both durable names unless both dirs are synced.
// The model has no symlinks and rejects absolute/parent-traversal names.
//
// Tests can inspect calls, use failAt(step, after), or set onBoundary for a
// specific operation/path/phase. The hook runs under the model mutex and must
// not call model methods. An after fault reports an error after the state change
// (including Sync/SyncDir durability); Write returns its positive n with error.
// writeLimit=-1 is unrestricted, 0 returns (0,nil), and a positive value limits
// each Write to that many bytes, allowing short-write handling to be exercised.
// OpenAppend, Close, and owner-lock operations are counted too so handle and
// ownership failures can be tested. Read-only operations are not fault points.
type faultIO struct {
	mu           sync.Mutex
	nodes        map[uint64]*faultNode
	nextID       uint64
	dead         bool
	locked       bool
	calls        []faultCall
	failureStep  int
	failureAfter bool
	onBoundary   func(faultBoundary) error
	writeLimit   int
}

var _ DurableIO = (*faultIO)(nil)

func newFaultIO() *faultIO {
	return &faultIO{
		nodes: map[uint64]*faultNode{
			1: {directory: true, entries: map[string]uint64{}, durable: map[string]uint64{}},
		},
		nextID: 2, writeLimit: -1,
	}
}

func (f *faultIO) Profile() DurabilityProfile { return FileSyncExperimental }

// resetFaults starts a new operation trace without changing live/durable state.
func (f *faultIO) resetFaults() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
	f.failureStep = 0
	f.failureAfter = false
	f.onBoundary = nil
	f.writeLimit = -1
}

// failAt arms one injected failure in the current trace. Call resetFaults before
// arming to count from the next operation. With after=true the mutation occurs.
func (f *faultIO) failAt(step int, after bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failureStep, f.failureAfter = step, after
}

func (f *faultIO) begin(op, name, to string) (faultCall, error) {
	if f.dead {
		return faultCall{}, os.ErrClosed
	}
	call := faultCall{Step: len(f.calls) + 1, Op: op, Name: name, To: to}
	f.calls = append(f.calls, call)
	return call, f.boundary(call, false)
}

func (f *faultIO) boundary(call faultCall, after bool) error {
	if f.failureStep == call.Step && f.failureAfter == after {
		return errFaultIO
	}
	if f.onBoundary != nil {
		return f.onBoundary(faultBoundary{faultCall: call, After: after})
	}
	return nil
}

func faultName(name string) (string, error) {
	if name == "" || strings.ContainsAny(name, "\\\x00") || path.IsAbs(name) {
		return "", os.ErrInvalid
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." {
			return "", os.ErrInvalid
		}
	}
	return path.Clean(name), nil
}

// lookup and parent require the model mutex. Namespace traversal always uses
// volatile entries; crash traverses durable entries separately.
func (f *faultIO) lookup(name string) (uint64, *faultNode, error) {
	clean, err := faultName(name)
	if err != nil {
		return 0, nil, err
	}
	id := uint64(1)
	if clean != "." {
		for _, part := range strings.Split(clean, "/") {
			node := f.nodes[id]
			if node == nil || !node.directory {
				return 0, nil, os.ErrNotExist
			}
			var ok bool
			id, ok = node.entries[part]
			if !ok {
				return 0, nil, os.ErrNotExist
			}
		}
	}
	return id, f.nodes[id], nil
}

func (f *faultIO) parent(name string) (*faultNode, string, error) {
	clean, err := faultName(name)
	if err != nil || clean == "." {
		return nil, "", os.ErrInvalid
	}
	_, dir, err := f.lookup(path.Dir(clean))
	if err != nil {
		return nil, "", err
	}
	if !dir.directory {
		return nil, "", os.ErrInvalid
	}
	return dir, path.Base(clean), nil
}

func (f *faultIO) CreateExclusive(name string) (DurableFile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin("CreateExclusive", name, "")
	if err != nil {
		return nil, err
	}
	dir, base, err := f.parent(name)
	if err != nil {
		return nil, err
	}
	if _, exists := dir.entries[base]; exists {
		return nil, os.ErrExist
	}
	id := f.nextID
	f.nextID++
	f.nodes[id] = &faultNode{}
	dir.entries[base] = id
	if err := f.boundary(call, true); err != nil {
		return nil, err
	}
	return &faultFile{fs: f, inode: id, name: name, writable: true}, nil
}

func (f *faultIO) OpenRead(name string) (ReadFile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dead {
		return nil, os.ErrClosed
	}
	id, node, err := f.lookup(name)
	if err != nil {
		return nil, err
	}
	if node.directory {
		return nil, os.ErrInvalid
	}
	return &faultFile{fs: f, inode: id, name: name}, nil
}

func (f *faultIO) OpenAppend(name string) (DurableFile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin("OpenAppend", name, "")
	if err != nil {
		return nil, err
	}
	id, node, err := f.lookup(name)
	if err != nil {
		return nil, err
	}
	if node.directory {
		return nil, os.ErrInvalid
	}
	if err := f.boundary(call, true); err != nil {
		return nil, err
	}
	return &faultFile{fs: f, inode: id, name: name, writable: true, appendMode: true}, nil
}

func (f *faultIO) Mkdir(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin("Mkdir", name, "")
	if err != nil {
		return err
	}
	dir, base, err := f.parent(name)
	if err != nil {
		return err
	}
	if _, exists := dir.entries[base]; exists {
		return os.ErrExist
	}
	id := f.nextID
	f.nextID++
	f.nodes[id] = &faultNode{directory: true, entries: map[string]uint64{}, durable: map[string]uint64{}}
	dir.entries[base] = id
	return f.boundary(call, true)
}

func (f *faultIO) List(name string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dead {
		return nil, os.ErrClosed
	}
	_, dir, err := f.lookup(name)
	if err != nil {
		return nil, err
	}
	if !dir.directory {
		return nil, os.ErrInvalid
	}
	names := make([]string, 0, len(dir.entries))
	for name := range dir.entries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (f *faultIO) InstallNew(from, to string) error {
	return f.install("InstallNew", from, to, false)
}

func (f *faultIO) ReplaceAtomic(from, to string) error {
	return f.install("ReplaceAtomic", from, to, true)
}

func (f *faultIO) install(op, from, to string, replace bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin(op, from, to)
	if err != nil {
		return err
	}
	fromDir, fromBase, err := f.parent(from)
	if err != nil {
		return err
	}
	toDir, toBase, err := f.parent(to)
	if err != nil {
		return err
	}
	id, exists := fromDir.entries[fromBase]
	if !exists {
		return os.ErrNotExist
	}
	if f.nodes[id].directory {
		return os.ErrInvalid
	}
	if old, exists := toDir.entries[toBase]; exists {
		if !replace {
			return os.ErrExist
		}
		if f.nodes[old].directory {
			return os.ErrInvalid
		}
	}
	if fromDir != toDir || fromBase != toBase {
		toDir.entries[toBase] = id
		delete(fromDir.entries, fromBase)
	}
	return f.boundary(call, true)
}

func cloneFaultEntries(entries map[string]uint64) map[string]uint64 {
	copy := make(map[string]uint64, len(entries))
	for name, id := range entries {
		copy[name] = id
	}
	return copy
}

func (f *faultIO) SyncDir(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin("SyncDir", name, "")
	if err != nil {
		return err
	}
	_, dir, err := f.lookup(name)
	if err != nil {
		return err
	}
	if !dir.directory {
		return os.ErrInvalid
	}
	dir.durable = cloneFaultEntries(dir.entries)
	return f.boundary(call, true)
}

func (f *faultIO) Remove(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin("Remove", name, "")
	if err != nil {
		return err
	}
	dir, base, err := f.parent(name)
	if err != nil {
		return err
	}
	id, exists := dir.entries[base]
	if !exists {
		return os.ErrNotExist
	}
	if node := f.nodes[id]; node.directory && len(node.entries) != 0 {
		return fmt.Errorf("faultio: directory %q is not empty", name)
	}
	delete(dir.entries, base)
	return f.boundary(call, true)
}

func (f *faultIO) AcquireOwnerLock() (OwnerLock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin("AcquireOwnerLock", ".", "")
	if err != nil {
		return nil, err
	}
	if f.locked {
		return nil, errFaultLocked
	}
	f.locked = true
	if err := f.boundary(call, true); err != nil {
		return &faultOwnerLock{fs: f}, err
	}
	return &faultOwnerLock{fs: f}, nil
}

// crash invalidates all old handles/locks and returns an independent rebooted
// model. Fault configuration and traces do not cross reboot boundaries. Nothing
// in this method promotes volatile bytes or volatile names to durable state.
func (f *faultIO) crash() *faultIO {
	f.mu.Lock()
	defer f.mu.Unlock()
	reboot := newFaultIO()
	reboot.nodes = make(map[uint64]*faultNode)
	reboot.nextID = f.nextID
	var visit func(uint64)
	visit = func(id uint64) {
		if _, exists := reboot.nodes[id]; exists {
			return
		}
		node := f.nodes[id]
		copy := &faultNode{directory: node.directory, data: bytes.Clone(node.synced), synced: bytes.Clone(node.synced)}
		if node.directory {
			copy.entries = cloneFaultEntries(node.durable)
			copy.durable = cloneFaultEntries(node.durable)
		}
		reboot.nodes[id] = copy
		for _, child := range node.durable {
			visit(child)
		}
	}
	visit(1)
	f.dead = true
	f.locked = false
	return reboot
}

type faultFile struct {
	fs         *faultIO
	inode      uint64
	name       string
	pos        int64
	writable   bool
	appendMode bool
	closed     bool
}

var _ DurableFile = (*faultFile)(nil)

func (file *faultFile) node() (*faultNode, error) {
	if file.closed || file.fs.dead {
		return nil, os.ErrClosed
	}
	return file.fs.nodes[file.inode], nil
}

func (file *faultFile) ReadAt(p []byte, off int64) (int, error) {
	file.fs.mu.Lock()
	defer file.fs.mu.Unlock()
	node, err := file.node()
	if err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, os.ErrInvalid
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= int64(len(node.data)) {
		return 0, io.EOF
	}
	n := copy(p, node.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (file *faultFile) Size() (int64, error) {
	file.fs.mu.Lock()
	defer file.fs.mu.Unlock()
	node, err := file.node()
	if err != nil {
		return 0, err
	}
	return int64(len(node.data)), nil
}

func (file *faultFile) Write(p []byte) (int, error) {
	f := file.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin("Write", file.name, "")
	if err != nil {
		return 0, err
	}
	node, err := file.node()
	if err != nil {
		return 0, err
	}
	if !file.writable {
		return 0, os.ErrPermission
	}
	n := len(p)
	if f.writeLimit >= 0 && n > f.writeLimit {
		n = f.writeLimit
	}
	if n == 0 {
		return 0, f.boundary(call, true)
	}
	if file.appendMode {
		file.pos = int64(len(node.data))
	}
	end := file.pos + int64(n)
	if end < file.pos || uint64(end) > uint64(^uint(0)>>1) {
		return 0, os.ErrInvalid
	}
	if end > int64(len(node.data)) {
		node.data = append(node.data, make([]byte, int(end)-len(node.data))...)
	}
	copy(node.data[file.pos:end], p[:n])
	file.pos = end
	return n, f.boundary(call, true)
}

func (file *faultFile) Truncate(size int64) error {
	f := file.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin("Truncate", file.name, "")
	if err != nil {
		return err
	}
	node, err := file.node()
	if err != nil {
		return err
	}
	if !file.writable {
		return os.ErrPermission
	}
	if size < 0 || uint64(size) > uint64(^uint(0)>>1) {
		return os.ErrInvalid
	}
	if size <= int64(len(node.data)) {
		node.data = bytes.Clone(node.data[:size])
	} else {
		node.data = append(node.data, make([]byte, int(size)-len(node.data))...)
	}
	return f.boundary(call, true)
}

func (file *faultFile) Sync() error {
	f := file.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin("Sync", file.name, "")
	if err != nil {
		return err
	}
	node, err := file.node()
	if err != nil {
		return err
	}
	if !file.writable {
		return os.ErrPermission
	}
	node.synced = bytes.Clone(node.data)
	return f.boundary(call, true)
}

func (file *faultFile) Close() error {
	f := file.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin("Close", file.name, "")
	if err != nil {
		return err
	}
	if file.closed {
		return nil
	}
	if _, err := file.node(); err != nil {
		return err
	}
	file.closed = true
	return f.boundary(call, true)
}

type faultOwnerLock struct {
	fs     *faultIO
	closed bool
}

func (lock *faultOwnerLock) Close() error {
	f := lock.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	call, err := f.begin("OwnerLock.Close", ".", "")
	if err != nil {
		return err
	}
	if lock.closed {
		return nil
	}
	lock.closed = true
	f.locked = false
	return f.boundary(call, true)
}
