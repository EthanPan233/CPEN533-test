package pa2lib

import (
	"runtime"
)

// Maximum number of bytes the program can use,
// actually value is 128MB but some margin is given
var maxMemBytes uint64 = 104 * 1024 * 1024

// Checks how much memory is currently in use by
// the program and if a new allocation is possible.
//
// Arguments:
//		numBytes: number of bytes desired to allocate
// Returns:
//		True if it's possible to allocate that many bytes,
//		false otherwise
func IsAllocatePossible(numBytes int)(bool) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	bytesUsed := m.Alloc + m.StackSys + uint64(numBytes)

	if bytesUsed < maxMemBytes {
		return true
	} else {
		return false
	}
}
