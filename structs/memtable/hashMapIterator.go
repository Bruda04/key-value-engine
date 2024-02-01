package memtable

import (
	"key-value-engine/structs/iterator"
	"key-value-engine/structs/record"
	"sort"
)

// MapIterator is a custom iterator for your hashmap.
type MapIterator struct {
	minRange string
	maxRange string
	keys     []string
	index    int
	data     map[string]*record.Record // Replace YourRecord with the actual type of your records
}

// NewYourStructIterator creates a new iterator for the given hashmap.
func (mm *MemTable) NewMapIterator(minRange, maxRange string) iterator.Iterator {
	sort.Strings(mm.keys)

	index := 0
	for index < len(mm.keys) && mm.keys[index] < minRange {
		index++
	}

	return &MapIterator{
		minRange: minRange,
		maxRange: maxRange,
		keys:     mm.keys,
		index:    index,
		data:     mm.hashMap,
	}
}

// Valid checks if the iterator is in a valid state.
func (it *MapIterator) Valid() bool {
	return it.index >= 0 && it.index < len(it.keys) && it.keys[it.index] <= it.maxRange
}

// Next moves the iterator to the next element.
func (it *MapIterator) Next() {
	it.index++
}

// Get returns the record at the current iterator position.
func (it *MapIterator) Get() *record.Record {
	key := it.keys[it.index]
	return it.data[key] // Assuming only one record per key, modify if needed
}
