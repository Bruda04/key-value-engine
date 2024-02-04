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
		crc := record.CrcHash(rec.GetValue())

		if crc != rec.GetCrc() {
			return nil, errors.New("crc error")
		}

		return rec, nil
	}

	rec, _ = e.lruCache.Get(key)
	if rec != nil {
		if rec.IsTombstone() {
			return nil, nil
		}

		crc := record.CrcHash(rec.GetValue())

		if crc != rec.GetCrc() {
			return nil, errors.New("crc error")
		}

		return rec, nil
	}

	rec, err := e.sst.Get(key)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		if rec.IsTombstone() {
			return nil, nil
		}

		crc := record.CrcHash(rec.GetValue())

		if crc != rec.GetCrc() {
			return nil, errors.New("crc error")
		}

		return rec, nil
	}

	return nil, nil
}
