package memtable

import (
	"key-value-engine/structs/iterator"
	"key-value-engine/structs/record"
	"sort"
	"strings"
)

type MapIterator struct {
	minRange      string
	maxRange      string
	prefix        string
	keys          []string
	index         int
	data          map[string]*record.Record // Replace YourRecord with the actual type of your records
	rangeIterator bool
}

// NewMapRangeIterator creates a new iterator for a hashmap within memtable
func (mm *MemTable) NewMapRangeIterator(minRange, maxRange string) iterator.Iterator {
	sort.Strings(mm.keys)

	index := 0
	for index < len(mm.keys) && mm.keys[index] < minRange {
		index++
	}

	return &MapIterator{
		minRange:      minRange,
		maxRange:      maxRange,
		keys:          mm.keys,
		index:         index,
		data:          mm.hashMap,
		rangeIterator: true,
	}
}

func (mm *MemTable) NewMapPrefixIterator(prefix string) iterator.Iterator {
	sort.Strings(mm.keys)

	index := 0
	for index < len(mm.keys) && !strings.HasPrefix(mm.keys[index], prefix) {
		index++
	}

	return &MapIterator{
		prefix:        prefix,
		keys:          mm.keys,
		index:         index,
		data:          mm.hashMap,
		rangeIterator: false,
	}
}

// Valid checks if the iterator is in a valid state.
func (it *MapIterator) Valid() bool {
	return it.index >= 0 && it.index < len(it.keys) && it.checkStopCondition()
}

// Next moves the iterator to the next element.
func (it *MapIterator) Next() {
	it.index++
}

// Get returns the record at the current iterator position.
func (it *MapIterator) Get() *record.Record {
	key := it.keys[it.index]
	return it.data[key]
}

func (it *MapIterator) checkStopCondition() bool {
	if it.rangeIterator {
		return it.keys[it.index] <= it.maxRange
	} else {
		return strings.HasPrefix(it.keys[it.index], it.prefix)
	}
}
