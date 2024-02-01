package scan

import (
	"key-value-engine/structs/iterator"
	"key-value-engine/structs/memtable"
	"key-value-engine/structs/record"
)

/*
MAIN EXAMPLE:
	rit := scan.MakeRangeIterateMem("a", "f", mm)
	fmt.Println(rit.Next().GetKey())
	fmt.Println(rit.Next().GetKey())

*/

type RangeIterator struct {
	iterators []iterator.Iterator
}

// FOR SSTABLE JUST ADD ITS ITERATORS TO THE ITERATORS
func MakeRangeIterateMem(minRange, maxRange string, manager *memtable.MemManager) *RangeIterator {
	return &RangeIterator{
		manager.GetMemIterators(minRange, maxRange),
	}
}

func (rit *RangeIterator) Next() *record.Record {
	var ret *record.Record

	incrementId := -1
	for id, it := range rit.iterators {
		if it.Valid() {
			if ret == nil || it.Get().GetKey() < ret.GetKey() {
				ret = it.Get()
				incrementId = id
			}
		}
	}
	if incrementId != -1 {
		rit.iterators[incrementId].Next()
	}

	if ret == nil {
		rit.Stop()
	}
	return ret
}

func (rit *RangeIterator) Stop() {
	rit.iterators = nil
}
