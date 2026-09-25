package main

import (
	"context"
	"errors"
	"os"
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

func mountSlowFS(backing, mount string, delay *ioDelay) (*fuse.Server, error) {
	if err := os.MkdirAll(backing, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(mount, 0o755); err != nil {
		return nil, err
	}
	var backingStat syscall.Stat_t
	if err := syscall.Stat(backing, &backingStat); err != nil {
		return nil, err
	}
	newNode := func(root *fs.LoopbackRoot, _ *fs.Inode, _ string, _ *syscall.Stat_t) fs.InodeEmbedder {
		return &slowNode{LoopbackNode: fs.LoopbackNode{RootData: root}, delay: delay}
	}
	root := &fs.LoopbackRoot{Path: backing, Dev: uint64(backingStat.Dev), NewNode: newNode}
	root.RootNode = newNode(root, nil, "", &backingStat)
	server, err := fs.Mount(mount, root.RootNode, &fs.Options{
		MountOptions: fuse.MountOptions{
			DirectMount:       true,
			DirectMountStrict: true,
			FsName:            "narumq-slowfs",
			Name:              "narumq-slowfs",
		},
	})
	if err != nil {
		return nil, err
	}
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		return nil, err
	}
	return server, nil
}

type slowNode struct {
	fs.LoopbackNode
	delay *ioDelay
}

func (node *slowNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	handle, fuseFlags, errno := node.LoopbackNode.Open(ctx, flags)
	if errno != 0 {
		return nil, 0, errno
	}
	file, ok := handle.(*fs.LoopbackFile)
	if !ok {
		return nil, 0, syscall.EIO
	}
	return &slowFile{LoopbackFile: file, delay: node.delay}, fuseFlags, 0
}

func (node *slowNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	inode, handle, fuseFlags, errno := node.LoopbackNode.Create(ctx, name, flags, mode, out)
	if errno != 0 {
		return inode, nil, 0, errno
	}
	file, ok := handle.(*fs.LoopbackFile)
	if !ok {
		return inode, nil, 0, syscall.EIO
	}
	return inode, &slowFile{LoopbackFile: file, delay: node.delay}, fuseFlags, 0
}

type slowFile struct {
	*fs.LoopbackFile
	delay *ioDelay
}

func (file *slowFile) Write(ctx context.Context, data []byte, offset int64) (uint32, syscall.Errno) {
	file.delay.sleep()
	return file.LoopbackFile.Write(ctx, data, offset)
}

func (file *slowFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	file.delay.sleep()
	return file.LoopbackFile.Fsync(ctx, flags)
}
