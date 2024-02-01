package iterator

import "key-value-engine/structs/record"

type Iterator interface {
	Valid() bool
	Next()
	Get() *record.Record
}
