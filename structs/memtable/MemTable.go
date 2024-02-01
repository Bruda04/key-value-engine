package memtable

import (
	"key-value-engine/structs/btree"
	"key-value-engine/structs/iterator"
	"key-value-engine/structs/record"
	"key-value-engine/structs/skipList"
	"sort"
)

// keys exists to be able to sort hashmap
type MemTable struct {
	maxCapacity int
	capacity    int
	structType  string
	skipList    *skipList.SkipList
	bTree       *btree.BTree
	hashMap     map[string]*record.Record
	keys        []string
}

// In case of incorrectly passed values creates default memtable/*
func MakeDefaultMemtable() (mem *MemTable) {
	return &MemTable{
		maxCapacity: 100,
		capacity:    0,
		structType:  "skiplist",
		bTree:       nil,
		skipList:    skipList.MakeSkipList(100),
		hashMap:     nil,
		keys:        nil,
	}
}

/*
Initialize Memtable

	-how many elements we want each table to contain
	-structures to be used for implementation
		-btree
		-skiplist
		-hashmap
*/
func MakeMemTable(maxCapacity int, structType string) *MemTable {
	if maxCapacity <= 0 {
		return MakeDefaultMemtable()
	}

	if structType == "btree" {
		bTree, _ := btree.MakeBTree(maxCapacity)
		return &MemTable{
			maxCapacity: maxCapacity,
			capacity:    0,
			structType:  structType,
			bTree:       bTree,
			skipList:    nil,
			hashMap:     nil,
			keys:        nil,
		}
	} else if structType == "skiplist" {
		return &MemTable{
			maxCapacity: maxCapacity,
			capacity:    0,
			structType:  structType,
			bTree:       nil,
			skipList:    skipList.MakeSkipList(maxCapacity),
			hashMap:     nil,
			keys:        nil,
		}
	} else if structType == "hashmap" {
		return &MemTable{
			maxCapacity: maxCapacity,
			capacity:    0,
			structType:  structType,
			bTree:       nil,
			skipList:    nil,
			hashMap:     make(map[string]*record.Record),
			keys:        []string{},
		}
	} else {
		return MakeDefaultMemtable()
	}
}

func (mem *MemTable) Clear() {
	if mem.structType == "btree" {
		bt, _ := btree.MakeBTree(mem.maxCapacity)
		mem.bTree = bt
	} else if mem.structType == "skiplist" {
		mem.skipList = skipList.MakeSkipList(mem.maxCapacity)
	} else if mem.structType == "hashmap" {
		mem.hashMap = make(map[string]*record.Record)
		mem.keys = []string{}
	}

	mem.capacity = 0
}

func (mem *MemTable) Find(key string) bool {
	if mem.structType == "btree" {
		return mem.bTree.Find(key)
	} else if mem.structType == "skiplist" {
		return mem.skipList.Find(key)
	} else {
		_, found := mem.hashMap[key]
		return found
	}
}

// Supports replace
func (mem *MemTable) Put(rec *record.Record) {
	if mem.structType == "btree" {
		mem.bTree.Insert(rec)
	} else if mem.structType == "skiplist" {
		mem.skipList.Insert(rec)
	} else {
		mem.hashMap[rec.GetKey()] = rec
		mem.keys = append(mem.keys, rec.GetKey())
	}

	mem.capacity += 1
}

func (mem *MemTable) isEmpty() bool {
	if mem.bTree == nil && mem.hashMap == nil && mem.skipList == nil {
		return true
	}
	return false
}

func (mem *MemTable) GetSorted() []*record.Record {
	if mem.structType == "btree" {
		return mem.bTree.GetSorted()
	} else if mem.structType == "skiplist" {
		return mem.skipList.GetSortedList()
	}
	return mem.getSortedMap()
}

func (mem *MemTable) getSortedMap() []*record.Record {
	sort.Strings(mem.keys)

	var ret []*record.Record
	for i := 0; i < len(mem.keys); i++ {
		ret = append(ret, mem.hashMap[mem.keys[i]])
	}

	return ret
}

func (mem *MemTable) getSortedRangeMap(minRange, maxRange string) []*record.Record {
	sort.Strings(mem.keys)

	var ret []*record.Record
	for i := 0; i < len(mem.keys); i++ {
		if mem.keys[i] >= minRange && mem.keys[i] <= maxRange {
			ret = append(ret, mem.hashMap[mem.keys[i]])
		}
	}
	return ret
}

func (mem *MemTable) getSortedRange(minRange, maxRange string) []*record.Record {
	if mem.structType == "btree" {
		return mem.bTree.GetRangeSorted(minRange, maxRange)
	} else if mem.structType == "skiplist" {
		return mem.skipList.GetRangeSortedList(minRange, maxRange)
	}
	return mem.getSortedRangeMap(minRange, maxRange)
}

func (mem *MemTable) GetIterator(minRange, maxRange string) iterator.Iterator {
	if mem.structType == "btree" {
		return mem.bTree.NewBTreeIterator(minRange, maxRange)
	} else if mem.structType == "skiplist" {
		return mem.skipList.NewSkipListIterator(minRange, maxRange)
	}
	//implement hashmap iterator
	return mem.NewMapIterator(minRange, maxRange)
}
