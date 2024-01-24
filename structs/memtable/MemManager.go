package memtable

import (
	"key-value-engine/structs/record"
	"sync"
)

type MemManager struct {
	currentTable *MemTable
	tables       []*MemTable
	currentIndex int
	maxTables    int
	lock         sync.RWMutex
}

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
	}
}

func (mm *MemManager) GetCurrentTable() *MemTable {
	mm.lock.RLock()
	defer mm.lock.RUnlock()
	return mm.currentTable
}

func (mm *MemManager) SwitchTable() {
	mm.lock.Lock()
	defer mm.lock.Unlock()

	// Switch to the next table
	mm.currentIndex = (mm.currentIndex + 1) % mm.maxTables
	mm.currentTable = mm.tables[mm.currentIndex]
}

func (mm *MemManager) Put(rec *record.Record) {
	currentTable := mm.GetCurrentTable()
	currentTable.Put(rec)

	if currentTable.capacity >= currentTable.maxCapacity {
		mm.tables[0].FlushMem()
	}
}
