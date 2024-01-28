package memtable

import (
	"key-value-engine/structs/record"
)

type MemManager struct {
	currentTable *MemTable
	tables       []*MemTable
	currentIndex int
	maxTables    int
	initialFill  bool
}

/*
Initialize Memtable Manager

	-accepts number of mem tables we plan to have
	-how many elements we want each table to contain
	-structures to be used for implementation
		-btree
		-skiplist
		-hashmap
*/
func NewMemTableManager(maxTables int, maxCapacity int, structType string) *MemManager {
	// Create initial MemTables
	tables := make([]*MemTable, maxTables)
	for i := 0; i < maxTables; i++ {
		tables[i] = MakeMemTable(maxCapacity, structType)
	}

	return &MemManager{
		currentTable: tables[0],
		tables:       tables,
		currentIndex: 0,
		maxTables:    maxTables,
		initialFill:  false, //initial round of tables hasn't been filled
	}
}

// add new element to the current memtable
func (mm *MemManager) PutMem(rec *record.Record) {
	mm.currentTable.Put(rec)

	if mm.currentTable.capacity >= mm.currentTable.maxCapacity {
		mm.SwitchTable()
		if mm.initialFill { //if all the tables have been filled
			mm.FlushMem()
		}
	}
}

// find if element exists in any of the memtables
func (mm *MemManager) FindInMem(key string) bool {
	for i := 0; i < mm.maxTables; i++ {
		if mm.tables[i].Find(key) {
			return true
		}
	}
	return false
}

// flush oldest memtable/create sstable/flush wal
func (mm *MemManager) FlushMem() {
	//create sst
	mm.currentTable.Clear()
	//flush acompanying wal
}

func (mm *MemManager) GetCurrentTable() *MemTable {
	return mm.currentTable
}

func (mm *MemManager) SwitchTable() {
	// Switch to the next table
	if mm.currentIndex == mm.maxTables-1 {
		mm.initialFill = true
	}
	mm.currentIndex = (mm.currentIndex + 1) % mm.maxTables
	mm.currentTable = mm.tables[mm.currentIndex]
}
