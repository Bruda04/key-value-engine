package scan

import (
	"key-value-engine/structs/memtable"
	"key-value-engine/structs/record"
)

func RangeScan(minRange, maxRange string, pageNumber, pageSize int, mm *memtable.MemManager) []*record.Record {
	rit := MakeRangeIterateMem(minRange, maxRange, mm)
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
