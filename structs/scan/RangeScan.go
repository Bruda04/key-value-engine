package scan

import (
	"key-value-engine/structs/memtable"
	"key-value-engine/structs/record"
	"key-value-engine/structs/sstable"
)

/*
RangeScan
Accepts:

	the range in which the found keys should be
	page number on which we want the values
	page size, how many records are written per page
	memmanager - in order to extract memtable iterators
	sstable /manager - in order to extract sstable iterators
*/
func RangeScan(minRange, maxRange string, pageNumber, pageSize int, mm *memtable.MemManager, sst *sstable.SSTable) []*record.Record {
	rit := MakeRangeIterate(minRange, maxRange, mm, sst)
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
