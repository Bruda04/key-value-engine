package scan

import (
	"key-value-engine/structs/memtable"
	"key-value-engine/structs/record"
	"key-value-engine/structs/sstable"
)

/*
PrefixScan
Accepts:

	the prefix which the found keys should have
	page number on which we want the values
	page size, how many records are written per page
	memmanager - in order to extract memtable iterators
	sstable /manager - in order to extract sstable iterators
*/
func PrefixScan(prefix string, pageNumber, pageSize int, mm *memtable.MemManager, sst *sstable.SSTable) []*record.Record {
	rit := MakePrefixIterate(prefix, mm, sst)
	var lista []*record.Record
	var current *record.Record

	for i := 0; i < pageSize*pageNumber; i++ {
		current = rit.Next()

		if current == nil {
			return lista
		}

		if i >= pageSize*(pageNumber-1) {
			lista = append(lista, current)
		}
	}

	return lista
}
