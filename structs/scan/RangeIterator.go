package scan

import (
	"key-value-engine/structs/iterator"
	"key-value-engine/structs/memtable"
	"key-value-engine/structs/record"
	"key-value-engine/structs/sstable"
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

/*
MakeRangeIterate
FOR SSTABLE JUST ADD ITS ITERATORS TO THE ITERATORS, and pass sst manager

	Accepts the range within the key values should be
	memmanager - in order to extract memtable iterators
	sstable /manager - in order to extract sstable iterators
*/
func MakeRangeIterate(minRange, maxRange string, manager *memtable.MemManager, sst *sstable.SSTable) *RangeIterator {
	iterators := sst.GetSSTRangeIterators(minRange, maxRange)
	iterators = append(iterators, manager.GetMemRangeIterators(minRange, maxRange)...)
	return &RangeIterator{
		iterators,
	}
}

func (rit *RangeIterator) Next() *record.Record {
	var ret *record.Record

	incrementId := -1
	for id, it := range rit.iterators {
		if it.Valid() {
			//can also be replaced if == because it might be a newer version
			if ret == nil || (ret != nil && it.Get().GetKey() <= ret.GetKey()) {
				if ret != nil && ret.GetKey() == it.Get().GetKey() {
					//if they're duplicates only replace it if its newer
					if ret.GetTimestamp() < it.Get().GetTimestamp() {
						rit.iterators[incrementId].Next() //if ret had an old version skip it, so it doesnt appear in next round
						ret = it.Get()
						incrementId = id
					} else {
						it.Next() //if ret had the good version, skip the old you found, so it doesnt appear again next round
					}
				} else {
					ret = it.Get()
					incrementId = id
				}

			}
		}
	}
	if incrementId != -1 {
		rit.iterators[incrementId].Next()
	}

	if ret == nil {
		rit.Stop()
	}

	if ret != nil && ret.IsTombstone() {
		return rit.Next()
	} else {
		return ret
	}
}

func (rit *RangeIterator) Stop() {
	rit.iterators = nil
}
