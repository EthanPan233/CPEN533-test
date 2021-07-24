package pa2lib

import (
	"bytes"
	"sync"
)

// Constants defining the maximum allowable length in
// bytes of the key and value in the KVStore
const maxKeyLengthBytes = 32
const maxValLengthBytes = 10000

// Data type used to represent a K-V pair
type StoreVal struct {
	key []byte
	value []byte
	version int32
}

// In-memory key-value store data structure
var KVStore = []StoreVal{}
var repKVStore = [2][]StoreVal{};

// Mutex for locking the KVStore object
var mutex = &sync.Mutex{}

// Get the value and version for a particular key
//
// Arguments:
//		key: key to get the value and version for
// Returns:
//		Byte array containing the value if the key exists
//		Version of the entry if the key exists
//		NO_ERR if key exists, otherwise KEY_DNE_ERR
func Get(key []byte) ([]byte, int32, uint32) {
	mutex.Lock()
	defer mutex.Unlock()
	for _, value := range KVStore {
		if bytes.Equal(key, value.key) {
			return value.value, value.version, NO_ERR
		}
	}

	return nil, 0, KEY_DNE_ERR 
}

// Put a new key-value pair or update an existing one
//
// Arguments:
// 		key: key for the pair
//		value: value for the pair
//		version: pointer to the version of the pair
// Returns:
//		Error code 
func Put(key []byte, value []byte, version *int32) (uint32) {
	if len(key) > maxKeyLengthBytes {
		return INVALID_KEY_ERR
	} else if len(value) > maxValLengthBytes {
		return INVALID_VAL_ERR
	} else if !IsAllocatePossible(len(key) + len(value) + 4) {
		return NO_SPC_ERR
	} else {
		storeVal := StoreVal {key: key, value: value}
		if version == nil {
			storeVal.version = 0
		} else {
			storeVal.version = *version
		}

		mutex.Lock()
		defer mutex.Unlock()
		for i, value := range KVStore {
			if bytes.Equal(key, value.key) {
				KVStore[i] = storeVal
				return NO_ERR
			}
		}

		KVStore = append(KVStore, storeVal)

		return NO_ERR
	}
}

func PutReplicate(key []byte, value []byte, version *int32, flag int) (uint32) {
	if len(key) > maxKeyLengthBytes {
		return INVALID_KEY_ERR
	} else if len(value) > maxValLengthBytes {
		return INVALID_VAL_ERR
	} else if !IsAllocatePossible(len(key) + len(value) + 4) {
		return NO_SPC_ERR
	} else {
		storeVal := StoreVal {key: key, value: value}
		if version == nil {
			storeVal.version = 0
		} else {
			storeVal.version = *version
		}

		mutex.Lock()
		defer mutex.Unlock()
		for i, value := range repKVStore[flag] {
			if bytes.Equal(key, value.key) {
				repKVStore[flag][i] = storeVal
				return NO_ERR
			}
		}

		repKVStore[flag] = append(repKVStore[flag], storeVal)

		return NO_ERR
	}
}

// Remove a key-value pair 
//
// Arguments:
// 		key: key to remove
// Returns:
//		NO_ERR if key exists, otherwise KEY_DNE_ERR
func Remove(key []byte) (uint32) {
	mutex.Lock()
	defer mutex.Unlock()
	for i, value := range KVStore {
		if bytes.Equal(key, value.key) {
			KVStore[i] = KVStore[len(KVStore) - 1]
			KVStore = KVStore[:len(KVStore) -1]
			return NO_ERR
		}
	}
	return KEY_DNE_ERR
}

func RemoveReplicate(key []byte, flag int) (uint32) {
	mutex.Lock()
	defer mutex.Unlock()
	for i, value := range repKVStore[flag] {
		if bytes.Equal(key, value.key) {
			repKVStore[flag][i] = repKVStore[flag][len(repKVStore[flag]) - 1]
			repKVStore[flag] = repKVStore[flag][:len(repKVStore[flag]) -1]
			return NO_ERR
		}
	}

	return KEY_DNE_ERR
}

func WipeoutReplicate(flag int){
	mutex.Lock()
	defer mutex.Unlock()
	repKVStore[flag] = []StoreVal{}
}

// Removes all the key-value pairs in the system
//
// Returns:
//		NO_ERR
func RemoveAll() (uint32) {
	mutex.Lock()
	KVStore = []StoreVal{}
	mutex.Unlock()

	return NO_ERR
}
