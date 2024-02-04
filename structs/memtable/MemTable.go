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

/*
MakeMemTable
-how many elements we want each table to contain
-structures to be used for implementation

	-btree
	-skiplist
	-hashmap
*/
func MakeMemTable(maxCapacity int, structType string) *MemTable {
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
	} else { //structType == "hashmap"
		return &MemTable{
			maxCapacity: maxCapacity,
			capacity:    0,
			structType:  structType,
			bTree:       nil,
			skipList:    nil,
			hashMap:     make(map[string]*record.Record),
			keys:        []string{},
		}
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

func (mem *MemTable) Find(key string) (bool, *record.Record) {
	if mem.structType == "btree" {
		return mem.bTree.Find(key)
	} else if mem.structType == "skiplist" {
		return mem.skipList.Find(key)
	} else {
		element, found := mem.hashMap[key]
		return found, element
	}
}

// Put - adds elements (if need be, replaces)
func (mem *MemTable) Put(rec *record.Record) {
	if mem.structType == "btree" {
		mem.bTree.Insert(rec)
	} else if mem.structType == "skiplist" {
		mem.skipList.Insert(rec)
	} else {
		_, exists := mem.hashMap[rec.GetKey()]
		if !exists {
			mem.keys = append(mem.keys, rec.GetKey())
		}
		mem.hashMap[rec.GetKey()] = rec
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
	return mem.getSortedMap() //hashmap
}

func (mem *MemTable) GetRangeIterator(minRange, maxRange string) iterator.Iterator {
	if mem.structType == "btree" {
		return mem.bTree.NewBTreeRangeIterator(minRange, maxRange)
	} else if mem.structType == "skiplist" {
		return mem.skipList.NewSkipListRangeIterator(minRange, maxRange)
	}
	//implement hashmap iterator
	return mem.NewMapRangeIterator(minRange, maxRange)
}

func (mem *MemTable) GetPrefixIterator(prefix string) iterator.Iterator {
	if mem.structType == "btree" {
		return mem.bTree.NewBTreePrefixIterator(prefix)
	} else if mem.structType == "skiplist" {
		return mem.skipList.NewSkipListPrefixIterator(prefix)
	}
	//implement hashmap iterator
	return mem.NewMapPrefixIterator(prefix)
}

// getSortedMap returns sorted hashmap
func (mem *MemTable) getSortedMap() []*record.Record {
	sort.Strings(mem.keys)

	var ret []*record.Record
	for i := 0; i < len(mem.keys); i++ {
		ret = append(ret, mem.hashMap[mem.keys[i]])
	}

	return ret
}
