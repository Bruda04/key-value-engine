package memtable

import (
	"key-value-engine/structs/btree"
	"key-value-engine/structs/record"
	"key-value-engine/structs/skipList"
)

type MemTable struct {
	maxCapacity int
	capacity    int
	structType  string
	skipList    *skipList.SkipList
	bTree       *btree.BTree
	hashMap     map[string]*record.Record
}

func MakeDefaultMemtable() (mem *MemTable) {
	return &MemTable{
		maxCapacity: 100,
		capacity:    0,
		structType:  "skiplist",
		bTree:       nil,
		skipList:    skipList.MakeSkipList(100),
	}
}

func MakeMemTable(maxCapacity int, structType string) *MemTable {
	if structType == "btree" {
		bTree, _ := btree.MakeBTree(maxCapacity)
		return &MemTable{
			maxCapacity: maxCapacity,
			capacity:    0,
			structType:  structType,
			bTree:       bTree,
			skipList:    nil,
		}
	} else if structType == "skiplist" {
		return &MemTable{
			maxCapacity: maxCapacity,
			capacity:    0,
			structType:  structType,
			bTree:       nil,
			skipList:    skipList.MakeSkipList(maxCapacity),
		}
	} else if structType == "hashmap" {
		return &MemTable{
			maxCapacity: maxCapacity,
			capacity:    0,
			structType:  structType,
			bTree:       nil,
			skipList:    nil,
		}
	} else {
		panic("Invalid structType!")
	}
}

func (mem *MemTable) Clear() {
	if mem.structType == "btree" {
		mem.bTree = nil
	} else if mem.structType == "skiplist" {
		mem.skipList = nil
	} else if mem.structType == "hashmap" {
		mem.hashMap = make(map[string]*record.Record)
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

func (mem *MemTable) Put(rec *record.Record) {
	if mem.structType == "btree" {
		mem.bTree.Insert(rec)
	} else if mem.structType == "skiplist" {
		mem.skipList.Insert(rec)
	} else {
		mem.hashMap[rec.GetKey()] = rec
	}

	mem.capacity += 1

	if mem.capacity >= mem.maxCapacity {
		mem.FlushMem()
	}
}

func (mem *MemTable) FlushMem() {
	mem.capacity = 0
}
