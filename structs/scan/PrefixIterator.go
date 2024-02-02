package scan

import (
	"key-value-engine/structs/iterator"
	"key-value-engine/structs/memtable"
	"key-value-engine/structs/record"
	"key-value-engine/structs/sstable"
)

type PrefixIterator struct {
	iterators []iterator.Iterator
}

/*
MakePrefixIterate
FOR SSTABLE JUST ADD ITS ITERATORS TO THE ITERATORS, and pass sst manager

	Accepts the prefix the key values should have
	memmanager - in order to extract memtable iterators
	sstable /manager - in order to extract sstable iterators
*/
func MakePrefixIterate(prefix string, manager *memtable.MemManager, sst *sstable.SSTable) *PrefixIterator {
	iterators := sst.GetSSTPrefixIterators(prefix)
	iterators = append(iterators, manager.GetMemPrefixIterators(prefix)...)

	return &PrefixIterator{
		iterators,
	}
}

func (pit *PrefixIterator) Next() *record.Record {
	var ret *record.Record

	incrementId := -1
	for id, it := range pit.iterators {
		if it.Valid() {
			//can also be replaced if == because it might be a newer version
			if ret == nil || (it.Get().GetKey() <= ret.GetKey() && !it.Get().IsTombstone()) {
				if ret != nil && ret.GetKey() == it.Get().GetKey() {
					//if they're duplicates only replace it if its newer
					if ret.GetTimestamp() > it.Get().GetTimestamp() {
						pit.iterators[incrementId].Next() //if ret had an old version skip it, so it doesnt appear in next round
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
		pit.iterators[incrementId].Next()
	}

	if ret == nil {
		pit.Stop()
	}
	return ret
}

func (pit *PrefixIterator) Stop() {
	pit.iterators = nil
}
