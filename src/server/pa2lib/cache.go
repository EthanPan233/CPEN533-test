package pa2lib

import (
	"sync"
	"time"
	"bytes"
	"runtime"
)

// Data type used to store a cache entry
type CacheVal struct {
	id []byte
	response []byte
	ttl int8
}

// In-memory response cache
var Cache = []CacheVal{}

// Mutex for locking the Cache map
var cacheMutex = &sync.Mutex{}

// Caches a response for a particular message ID
//
// Arguments:
// 		msgID: the ID to cache for
//		respMsgBytes: the raw response message
// Returns:
//		True if cached successfully, false if not enough room
func CacheResponse(msgID []byte, respMsgBytes []byte) (bool) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	if IsAllocatePossible(len(msgID) + len(respMsgBytes) + 1) {
		cacheVal := CacheVal {id: msgID, response: respMsgBytes, ttl: 4}
		Cache = append(Cache, cacheVal)

		return true
	} else {
		return false
	}
}

// Returns the cached response for a particular message ID if
// it exists
//
// Arguments:
//		msgID: the ID to get a response for
// Returns:
//		The raw response if it was found, nil otherwise
//		True if the response was found, false otherwise
func GetCachedResponse(msgID []byte) ([]byte, bool) {
	cacheMutex.Lock()
	for _, v := range Cache {
		if bytes.Equal(v.id, msgID) {
			cacheMutex.Unlock()
			return v.response, true
		}
	}

	cacheMutex.Unlock()
	return nil, false
}

// Loops every second to remove all items in the cache whose
// TTL has run out. Should be called as a goroutine so it can
// run in the background
func CacheTTLManager() () {
	for {
		cacheMutex.Lock()
		i := 0
		for _, v := range Cache {
			// If TTL is zero remove from cache
			if v.ttl > 0 {
				v.ttl -= 1
				Cache[i] = v
				i += 1
			}
		}
		Cache = Cache[:i]
		cacheMutex.Unlock()

		// Run the GC explicitly since many items may have been removed 
		runtime.GC()

		// Sleep for 1 second
		time.Sleep(time.Second)
	}
}
