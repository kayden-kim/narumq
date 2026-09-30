package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type ioDelay struct {
	milliseconds atomic.Int64
}

func (delay *ioDelay) sleep() {
	if milliseconds := delay.milliseconds.Load(); milliseconds > 0 {
		time.Sleep(time.Duration(milliseconds) * time.Millisecond)
	}
}

func (delay *ioDelay) refresh(path string) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		delay.milliseconds.Store(0)
		return
	}
	if err != nil {
		return
	}
	if milliseconds, ok := parseDelayValue(data); ok {
		delay.milliseconds.Store(milliseconds)
	}
}

func (delay *ioDelay) refreshLoop(ctx context.Context, path string) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			delay.refresh(path)
		}
	}
}

type storageMode string

const (
	storageModeNormal       storageMode = "normal"
	storageModeHoldWALFsync storageMode = "hold-wal-fsync"
	storageModeFailFsync    storageMode = "fail-fsync"
	storageModeFailDirsync  storageMode = "fail-dirsync"
)

type storageStatus struct {
	Mode          string   `json:"mode"`
	HeldWalFsyncs int      `json:"heldWalFsyncs"`
	HeldPaths     []string `json:"heldPaths"`
	PoweredOff    bool     `json:"poweredOff"`
}

type chaosController struct {
	delay         *ioDelay
	mu            sync.Mutex
	mode          storageMode
	held          []string
	release       chan struct{}
	poweredOff    bool
	opMu          sync.RWMutex
	cut           bool
	cutGeneration uint64
	journal       *powerLossJournal
}

func newChaosController(delay *ioDelay) *chaosController {
	return &chaosController{delay: delay, mode: storageModeNormal, release: make(chan struct{})}
}

func (controller *chaosController) setMode(mode string) error {
	next := storageMode(mode)
	switch next {
	case storageModeNormal, storageModeHoldWALFsync, storageModeFailFsync, storageModeFailDirsync:
	default:
		return fmt.Errorf("unknown storage mode %q", mode)
	}

	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.mode == storageModeHoldWALFsync && next != storageModeHoldWALFsync {
		close(controller.release)
		controller.release = make(chan struct{})
	}
	controller.mode = next
	return nil
}

func (controller *chaosController) status() storageStatus {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return storageStatus{
		Mode:          string(controller.mode),
		HeldWalFsyncs: len(controller.held),
		HeldPaths:     append([]string(nil), controller.held...),
		PoweredOff:    controller.poweredOff,
	}
}

func (controller *chaosController) setPoweredOff(poweredOff bool) {
	controller.mu.Lock()
	controller.poweredOff = poweredOff
	controller.mu.Unlock()
}

func (controller *chaosController) beginCut() {
	controller.opMu.Lock()
	controller.cut = true
	controller.cutGeneration++
	controller.opMu.Unlock()
}

func (controller *chaosController) releaseHeld() int {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	held := len(controller.held)
	close(controller.release)
	controller.release = make(chan struct{})
	controller.held = nil
	return held
}

func (controller *chaosController) fsyncInterrupted(generation uint64) bool {
	controller.opMu.RLock()
	defer controller.opMu.RUnlock()
	return controller.cut || controller.cutGeneration != generation
}

func (controller *chaosController) waitHeldFsync(ctx context.Context, rel string, release <-chan struct{}, generation uint64) syscall.Errno {
	select {
	case <-release:
	case <-ctx.Done():
		controller.removeHeldPath(rel)
		return syscall.EINTR
	}
	controller.removeHeldPath(rel)
	if controller.fsyncInterrupted(generation) {
		return syscall.EIO
	}
	return 0
}

func (controller *chaosController) resetAfterRemount(journal *powerLossJournal) {
	controller.opMu.Lock()
	controller.cut = false
	controller.mu.Lock()
	controller.mode = storageModeNormal
	controller.held = nil
	controller.journal = journal
	controller.release = make(chan struct{})
	controller.mu.Unlock()
	controller.opMu.Unlock()
}

func (controller *chaosController) currentJournal() *powerLossJournal {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.journal
}

type chaosMount struct {
	backing    string
	mountPoint string
	controller *chaosController
	server     *fuse.Server
	done       chan struct{}
	journal    *powerLossJournal
}

func mountChaosFS(backing, mountPoint string, controller *chaosController) (*chaosMount, error) {
	dataRoot := filepath.Join(backing, "data")
	graveRoot := filepath.Join(backing, "grave")
	for _, directory := range []string{dataRoot, graveRoot, mountPoint} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return nil, fmt.Errorf("create chaos filesystem directory: %w", err)
		}
	}
	var backingStat syscall.Stat_t
	if err := syscall.Stat(dataRoot, &backingStat); err != nil {
		return nil, fmt.Errorf("stat chaos filesystem backing: %w", err)
	}
	journal := newPowerLossJournal(dataRoot, graveRoot)
	newNode := func(root *fs.LoopbackRoot, _ *fs.Inode, _ string, _ *syscall.Stat_t) fs.InodeEmbedder {
		return &chaosNode{LoopbackNode: fs.LoopbackNode{RootData: root}, controller: controller}
	}
	root := &fs.LoopbackRoot{Path: dataRoot, Dev: uint64(backingStat.Dev), NewNode: newNode}
	root.RootNode = newNode(root, nil, "", &backingStat)
	server, err := fs.Mount(mountPoint, root.RootNode, &fs.Options{
		MountOptions: fuse.MountOptions{
			DirectMount:       true,
			DirectMountStrict: true,
			FsName:            "narumq-chaosfs",
			Name:              "narumq-chaosfs",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("mount chaos filesystem: %w", err)
	}
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		return nil, fmt.Errorf("wait for chaos filesystem mount: %w", err)
	}
	mount := &chaosMount{
		backing:    backing,
		mountPoint: mountPoint,
		controller: controller,
		server:     server,
		done:       make(chan struct{}),
		journal:    journal,
	}
	controller.mu.Lock()
	controller.journal = journal
	controller.mu.Unlock()
	go func() {
		server.Wait()
		close(mount.done)
	}()
	return mount, nil
}

func (mount *chaosMount) doneChannel() <-chan struct{} {
	if mount == nil {
		return nil
	}
	return mount.done
}

func (mount *chaosMount) unmount() error {
	for retries := 0; ; retries++ {
		err := mount.server.Unmount()
		if err == nil {
			<-mount.done
			return nil
		}
		if !errors.Is(err, syscall.EBUSY) || retries >= 20 {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (mount *chaosMount) unmountAndRollback(torn bool) (powerCutResult, error) {
	if err := mount.unmount(); err != nil {
		return powerCutResult{}, fmt.Errorf("unmount chaos filesystem: %w", err)
	}
	result, err := mount.journal.rollback(torn)
	if err != nil {
		return result, fmt.Errorf("rollback chaos filesystem: %w", err)
	}
	return result, nil
}

func (mount *chaosMount) remount() error {
	remounted, err := mountChaosFS(mount.backing, mount.mountPoint, mount.controller)
	if err != nil {
		return err
	}
	mount.server = remounted.server
	mount.done = remounted.done
	mount.journal = remounted.journal
	mount.controller.resetAfterRemount(remounted.journal)
	return nil
}

type chaosNode struct {
	fs.LoopbackNode
	controller *chaosController
}

func (node *chaosNode) relativePath() string {
	root := node.RootData.RootNode.EmbeddedInode()
	return cleanRelativeDirectory(node.Path(root))
}

func (node *chaosNode) backingPath() string {
	return filepath.Join(node.RootData.Path, filepath.FromSlash(node.relativePath()))
}

func (node *chaosNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	node.controller.opMu.RLock()
	defer node.controller.opMu.RUnlock()
	if node.controller.cut {
		return nil, 0, syscall.EIO
	}
	if flags&syscall.O_TRUNC != 0 {
		if errno := node.recordTruncatePath(node.backingPath(), 0); errno != 0 {
			return nil, 0, errno
		}
	}
	handle, fuseFlags, errno := node.LoopbackNode.Open(ctx, flags)
	if errno != 0 {
		return nil, 0, errno
	}
	loopback, ok := handle.(*fs.LoopbackFile)
	if !ok {
		return nil, 0, syscall.EIO
	}
	file, errno := node.wrapFile(loopback, node.backingPath())
	if errno != 0 {
		loopback.Release(ctx)
		return nil, 0, errno
	}
	return file, fuseFlags, 0
}

func (node *chaosNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	node.controller.opMu.RLock()
	defer node.controller.opMu.RUnlock()
	if node.controller.cut {
		return nil, nil, 0, syscall.EIO
	}
	path := filepath.Join(node.backingPath(), name)
	_, statErr := os.Lstat(path)
	existed := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, nil, 0, fs.ToErrno(statErr)
	}
	if existed && flags&syscall.O_TRUNC != 0 {
		if errno := node.recordTruncatePath(path, 0); errno != 0 {
			return nil, nil, 0, errno
		}
	}
	inode, handle, fuseFlags, errno := node.LoopbackNode.Create(ctx, name, flags, mode, out)
	if errno != 0 {
		return inode, nil, 0, errno
	}
	if !existed {
		node.controller.currentJournal().recordCreate(node.relativePath(), name)
	}
	loopback, ok := handle.(*fs.LoopbackFile)
	if !ok {
		return inode, nil, 0, syscall.EIO
	}
	file, errno := node.wrapFile(loopback, path)
	if errno != 0 {
		loopback.Release(ctx)
		return inode, nil, 0, errno
	}
	return inode, file, fuseFlags, 0
}

func (node *chaosNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	node.controller.opMu.RLock()
	defer node.controller.opMu.RUnlock()
	if node.controller.cut {
		return nil, syscall.EIO
	}
	inode, errno := node.LoopbackNode.Mkdir(ctx, name, mode, out)
	if errno == 0 {
		node.controller.currentJournal().recordMkdir(node.relativePath(), name)
	}
	return inode, errno
}

func (node *chaosNode) Unlink(_ context.Context, name string) syscall.Errno {
	node.controller.opMu.RLock()
	defer node.controller.opMu.RUnlock()
	if node.controller.cut {
		return syscall.EIO
	}
	if err := node.controller.currentJournal().unlink(node.relativePath(), name); err != nil {
		return fs.ToErrno(err)
	}
	return 0
}

func (node *chaosNode) Rmdir(_ context.Context, name string) syscall.Errno {
	node.controller.opMu.RLock()
	defer node.controller.opMu.RUnlock()
	if node.controller.cut {
		return syscall.EIO
	}
	if err := node.controller.currentJournal().rmdir(node.relativePath(), name); err != nil {
		return fs.ToErrno(err)
	}
	return 0
}

func (node *chaosNode) Rename(_ context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags != 0 {
		return syscall.EINVAL
	}
	destination, ok := newParent.(*chaosNode)
	if !ok || destination.relativePath() != node.relativePath() {
		return syscall.EXDEV
	}
	node.controller.opMu.RLock()
	defer node.controller.opMu.RUnlock()
	if node.controller.cut {
		return syscall.EIO
	}
	oldPath := filepath.Join(node.backingPath(), name)
	info, err := os.Lstat(oldPath)
	if err != nil {
		return fs.ToErrno(err)
	}
	if info.IsDir() {
		return syscall.EXDEV
	}
	if err := node.controller.currentJournal().rename(node.relativePath(), name, newName); err != nil {
		return fs.ToErrno(err)
	}
	return 0
}

func (node *chaosNode) Link(context.Context, fs.InodeEmbedder, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOTSUP
}

func (node *chaosNode) Symlink(context.Context, string, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOTSUP
}

func (node *chaosNode) Mknod(context.Context, string, uint32, uint32, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOTSUP
}

func (node *chaosNode) CopyFileRange(context.Context, fs.FileHandle, uint64, *fs.Inode, fs.FileHandle, uint64, uint64, uint64) (uint32, syscall.Errno) {
	return 0, syscall.ENOSYS
}

func (node *chaosNode) Setattr(ctx context.Context, file fs.FileHandle, input *fuse.SetAttrIn, output *fuse.AttrOut) syscall.Errno {
	node.controller.opMu.RLock()
	defer node.controller.opMu.RUnlock()
	if node.controller.cut {
		return syscall.EIO
	}
	if size, ok := input.GetSize(); ok {
		var reader *os.File
		closeReader := false
		if chaosFile, ok := file.(*chaosFile); ok {
			reader = chaosFile.reader
		} else {
			opened, err := os.Open(node.backingPath())
			if err != nil {
				return fs.ToErrno(err)
			}
			reader = opened
			closeReader = true
		}
		ino, err := fileInode(reader)
		if err == nil {
			err = node.controller.currentJournal().recordTruncate(ino, reader, int64(size))
		}
		if closeReader {
			if closeErr := reader.Close(); err == nil {
				err = closeErr
			}
		}
		if err != nil {
			return syscall.EIO
		}
	}
	return node.LoopbackNode.Setattr(ctx, file, input, output)
}

func (node *chaosNode) Fsync(ctx context.Context, handle fs.FileHandle, flags uint32) syscall.Errno {
	file, ok := handle.(*chaosFile)
	if !ok {
		return syscall.EIO
	}
	node.controller.delay.sleep()
	rel := node.relativePath()

	node.controller.opMu.RLock()
	if node.controller.cut {
		node.controller.opMu.RUnlock()
		return syscall.EIO
	}
	generation := node.controller.cutGeneration
	node.controller.mu.Lock()
	hold := node.controller.mode == storageModeHoldWALFsync && filepath.Base(filepath.FromSlash(rel)) == "wal.bin"
	var release <-chan struct{}
	if hold {
		node.controller.held = append(node.controller.held, rel)
		release = node.controller.release
	}
	node.controller.mu.Unlock()
	if hold {
		node.controller.opMu.RUnlock()
		if errno := node.controller.waitHeldFsync(ctx, rel, release, generation); errno != 0 {
			return errno
		}
		node.controller.opMu.RLock()
	}
	defer node.controller.opMu.RUnlock()
	if node.controller.cut || node.controller.cutGeneration != generation {
		return syscall.EIO
	}

	node.controller.mu.Lock()
	mode := node.controller.mode
	journal := node.controller.journal
	node.controller.mu.Unlock()
	if mode == storageModeFailFsync {
		return syscall.EIO
	}
	if errno := file.LoopbackFile.Fsync(ctx, flags); errno != 0 {
		return errno
	}
	journal.fileSynced(file.ino)
	return 0
}

func (node *chaosNode) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	node.controller.opMu.RLock()
	defer node.controller.opMu.RUnlock()
	if node.controller.cut {
		return nil, 0, syscall.EIO
	}
	handle, fuseFlags, errno := node.LoopbackNode.OpendirHandle(ctx, flags)
	if errno != 0 {
		return nil, 0, errno
	}
	directory, ok := handle.(loopbackDirHandle)
	if !ok {
		return nil, 0, syscall.EIO
	}
	return &chaosDir{loopbackDirHandle: directory, node: node}, fuseFlags, 0
}

func (node *chaosNode) syncDirectory() syscall.Errno {
	node.controller.opMu.RLock()
	defer node.controller.opMu.RUnlock()
	if node.controller.cut {
		return syscall.EIO
	}
	node.controller.mu.Lock()
	mode := node.controller.mode
	node.controller.mu.Unlock()
	if mode == storageModeFailDirsync {
		return syscall.EIO
	}
	if err := node.controller.currentJournal().directorySynced(node.relativePath()); err != nil {
		return syscall.EIO
	}
	return 0
}

func (node *chaosNode) recordTruncatePath(path string, size int64) syscall.Errno {
	reader, err := os.Open(path)
	if err != nil {
		return fs.ToErrno(err)
	}
	defer reader.Close()
	ino, err := fileInode(reader)
	if err != nil {
		return syscall.EIO
	}
	if err := node.controller.currentJournal().recordTruncate(ino, reader, size); err != nil {
		return syscall.EIO
	}
	return 0
}

func (node *chaosNode) wrapFile(loopback *fs.LoopbackFile, path string) (*chaosFile, syscall.Errno) {
	reader, err := os.Open(path)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	ino, err := fileInode(reader)
	if err != nil {
		reader.Close()
		return nil, syscall.EIO
	}
	return &chaosFile{LoopbackFile: loopback, reader: reader, ino: ino, controller: node.controller}, 0
}

func fileInode(file *os.File) (uint64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("file %s has no inode number", file.Name())
	}
	return stat.Ino, nil
}

type chaosFile struct {
	*fs.LoopbackFile
	reader     *os.File
	ino        uint64
	controller *chaosController
}

func (file *chaosFile) Write(ctx context.Context, data []byte, offset int64) (uint32, syscall.Errno) {
	file.controller.delay.sleep()
	file.controller.opMu.RLock()
	defer file.controller.opMu.RUnlock()
	if file.controller.cut {
		return 0, syscall.EIO
	}
	if err := file.controller.currentJournal().recordWrite(file.ino, file.reader, offset, len(data)); err != nil {
		return 0, syscall.EIO
	}
	return file.LoopbackFile.Write(ctx, data, offset)
}

func (file *chaosFile) Allocate(context.Context, uint64, uint64, uint32) syscall.Errno {
	return syscall.ENOSYS
}

func (file *chaosFile) Release(ctx context.Context) syscall.Errno {
	readerErr := file.reader.Close()
	loopbackErr := file.LoopbackFile.Release(ctx)
	if readerErr != nil {
		return syscall.EIO
	}
	return loopbackErr
}

type loopbackDirHandle interface {
	fs.DirStream
	fs.FileReaddirenter
	fs.FileReleasedirer
	fs.FileSeekdirer
}

type chaosDir struct {
	loopbackDirHandle
	node *chaosNode
}

func (directory *chaosDir) Fsyncdir(context.Context, uint32) syscall.Errno {
	return directory.node.syncDirectory()
}

func (controller *chaosController) removeHeldPath(path string) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	for index := len(controller.held) - 1; index >= 0; index-- {
		if controller.held[index] == path {
			controller.held = append(controller.held[:index], controller.held[index+1:]...)
			return
		}
	}
}
