// Package diskspace measures local filesystem space at the platform boundary.
package diskspace

// FreeBytes returns bytes available to an unprivileged caller on the filesystem containing path.
func FreeBytes(path string) (uint64, error) {
	return freeBytes(path)
}
