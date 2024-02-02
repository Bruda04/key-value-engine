package memtable

import (
	"fmt"
	"key-value-engine/structs/iterator"
	"key-value-engine/structs/record"
	"key-value-engine/structs/sstable"
)

type MemManager struct {
	currentTable *MemTable
	tables       []*MemTable
	sstmanager   *sstable.SSTable
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

func MakeMemTableManager(maxTables int, maxCapacity int, structType string, sstmanager *sstable.SSTable) *MemManager {
	tables := make([]*MemTable, maxTables)
	for i := 0; i < maxTables; i++ {
		tables[i] = MakeMemTable(maxCapacity, structType)
	}

	return &MemManager{
		currentTable: tables[0],
		tables:       tables,
		sstmanager:   sstmanager,
		currentIndex: 0,
		maxTables:    maxTables,
		initialFill:  false, //initial round of tables hasn't been filled
	}
}

// FlushMem flush oldest memtable/create sstable/flush wal
func (mm *MemManager) FlushMem() {
	err := mm.sstmanager.Flush(mm.currentTable.GetSorted())
	if err != nil {
		return
	}
	mm.currentTable.Clear()
	//flush acompanying wal
	fmt.Print("Flush this shit")
}

func (mm *MemManager) SwitchTable() {
	// Switch to the next table
	if mm.currentIndex == mm.maxTables-1 {
		mm.initialFill = true
	}
	mm.currentIndex = (mm.currentIndex + 1) % mm.maxTables
	mm.currentTable = mm.tables[mm.currentIndex]
}

// PutMem add new element to the current memtable
func (mm *MemManager) PutMem(rec *record.Record) bool {
	mm.currentTable.Put(rec)

	if mm.currentTable.capacity >= mm.currentTable.maxCapacity {
		mm.SwitchTable()
		if mm.initialFill { //if all the tables have been filled
			mm.FlushMem()
		}
		return true
	}
	return false
}

func (mm *MemManager) GetCurrentTable() *MemTable {
	return mm.currentTable
}

// FindInMem find if element exists in any of the memtables
func (mm *MemManager) FindInMem(key string) (bool, *record.Record) {
	for i := 0; i < mm.maxTables; i++ {
		found, el := mm.tables[i].Find(key)
		if found {
			return found, el
		}
	}
	return false, nil
}

func (mm *MemManager) GetMemRangeIterators(minRange, maxRange string) []iterator.Iterator {
	var memIterators []iterator.Iterator

	for i := 0; i < mm.maxTables; i++ {
		memIterators = append(memIterators, mm.tables[i].GetRangeIterator(minRange, maxRange))
	}

	return memIterators
}

func (mm *MemManager) GetMemPrefixIterators(prefix string) []iterator.Iterator {
	var memIterators []iterator.Iterator

	for i := 0; i < mm.maxTables; i++ {
		memIterators = append(memIterators, mm.tables[i].GetPrefixIterator(prefix))
	}

	return memIterators
}
