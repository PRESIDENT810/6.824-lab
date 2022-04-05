package kvraft

import "sync"

// Storage
//
// General interface for our kvserver storage
// The specific implementation can be based on a simple map, or more complex databases such as mysql,
// as long as this implementation satisfies the following functions
type Storage interface {
	Get(key string) string
	Put(key string, value string)
	Append(key string, value string)
}

// MapStorage
//
// An implementation of Storage interface based on a simple map as Go's primitive, thread-safe
type MapStorage struct {
	m  map[string]string
	mu sync.Mutex
	me int
}

// Get
//
// Fetches the current value for the key; a Get for a non-existent key should return an empty string.
func (ms *MapStorage) Get(key string) string {
	defer ms.LogStorage("Get")
	ms.mu.Lock()
	ms.LogStorageMutexLock()
	defer ms.LogStorageMutexUnlock()
	defer ms.mu.Unlock()
	if value, ok := ms.m[key]; ok {
		return value
	}
	return ""
}

// Put
//
// Set the value corresponding to our key to our value
func (ms *MapStorage) Put(key string, value string) {
	defer ms.LogStorage("Put")
	ms.mu.Lock()
	ms.LogStorageMutexLock()
	defer ms.LogStorageMutexUnlock()
	defer ms.mu.Unlock()
	ms.m[key] = value
	return
}

// Append
//
// Append arg to key's value; appending to a non-existent key should act like Put.
func (ms *MapStorage) Append(key string, value string) {
	defer ms.LogStorage("Append")
	ms.mu.Lock()
	ms.LogStorageMutexLock()
	defer ms.LogStorageMutexUnlock()
	defer ms.mu.Unlock()
	if oldValue, ok := ms.m[key]; ok {
		ms.m[key] = oldValue + value
		return
	}
	ms.m[key] = value
}
