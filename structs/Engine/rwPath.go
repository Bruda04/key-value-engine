package Engine

import (
	"errors"
	"key-value-engine/structs/record"
	"key-value-engine/structs/wputils"
)

func (e *Engine) writePath(key string, value []byte, deleted bool) error {
	rec := record.MakeRecord(key, value, deleted)

	newRestoreOffset, err := wputils.AddRecord(e.memMan, e.commitLog, e.walRestoreOffset, rec)
	if err != nil {
		return err
	}

	e.walRestoreOffset = newRestoreOffset

	e.lruCache.Put(rec)

	return nil
}

func (e *Engine) readPath(key string) (*record.Record, error) {
	fnd, rec := e.memMan.FindInMem(key)
	if fnd {
		return rec, nil
	}

	rec, _ = e.lruCache.Get(key)
	if rec != nil {
		return rec, nil
	}

	rec, _ = e.sst.Get(key)

	if rec != nil {
		return rec, nil
	}

	return nil, errors.New("Not Found")
}
