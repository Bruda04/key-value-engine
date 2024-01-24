package cache

import (
	"container/list"
	"key-value-engine/structs/record"
)

// LRUCache represents a simple implementation of an LRU cache with Record instances.
type LRUCache struct {
	Capacity      int
	CacheElements map[string]*list.Element
	KeyList       *list.List
}

// NewLRUCache creates a new LRUCache with the given capacity.
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		Capacity:      capacity,
		CacheElements: make(map[string]*list.Element),
		KeyList:       list.New(),
	}
}

/*
Get retrieves the Record associated with the given key from the cache.
If the key is found, it is moved to the front of the LRU list.

Parameters:
  - key: A string representing the key to be retrieved.

Returns:
  - *record.Record: Pointer to the Record associated with the key.
  - bool: Indicates whether the key was found in the cache.
*/
func (lru *LRUCache) Get(key string) (*record.Record, bool) {
	if elem, exists := lru.CacheElements[key]; exists {
		lru.KeyList.MoveToFront(elem)
		return elem.Value.(*record.Record), true
	}
	return nil, false
}

/*
Put adds a Record to the cache. If the key already exists, it updates the Record and moves it to the front.
If the cache is at capacity, it removes the least recently used element.

Parameters:
  - rec: Pointer to a Record instance to be added or updated in the cache.
*/
func (lru *LRUCache) Put(rec *record.Record) {
	key := rec.GetKey()
	if elem, exists := lru.CacheElements[key]; exists {
		if rec.IsTombstone() {
			lru.KeyList.Remove(elem)
			delete(lru.CacheElements, key)
		} else {
			elem.Value = rec
			lru.KeyList.MoveToFront(elem)
		}
		return
	}

	if lru.KeyList.Len() >= lru.Capacity {
		oldest := lru.KeyList.Back()
		delete(lru.CacheElements, oldest.Value.(*record.Record).GetKey())
		lru.KeyList.Remove(oldest)
	}

	elem := lru.KeyList.PushFront(rec)
	lru.CacheElements[key] = elem
}
