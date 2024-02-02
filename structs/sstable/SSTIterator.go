package sstable

import (
	"encoding/binary"
	"key-value-engine/structs/iterator"
	"key-value-engine/structs/record"
	"os"
	"strings"
)

type SSTableIterator struct {
	dirPath  string
	minRange string
	maxRange string
	prefix   string
	sst      *SSTable

	current *record.Record
	offset  uint64

	finish        bool
	rangeIterator bool
}

func (sst *SSTable) NewSSTRangeIterator(minRange, maxRange, dirPath string) iterator.Iterator {
	//if the table doesn't have any fitting values skip it
	it := &SSTableIterator{
		dirPath:       dirPath,
		minRange:      minRange,
		maxRange:      maxRange,
		sst:           sst,
		offset:        0, //initial offset
		rangeIterator: true,
		finish:        !sst.iteratorSummaryCheck(dirPath, minRange, maxRange), //should end if not in summary
	}

	//geting first valid
	it.Next()

	return it

}

func (sst *SSTable) NewSSTPrefixIterator(prefix, dirPath string) iterator.Iterator {
	//if the table doesn't have any fitting values skip it
	it := &SSTableIterator{
		dirPath:       dirPath,
		prefix:        prefix,
		sst:           sst,
		offset:        0, //initial offset
		rangeIterator: false,
		finish:        false,
	}

	//geting first valid
	it.Next()

	return it

}

func (it *SSTableIterator) Valid() bool {
	//I need to be able to see if the next is null without moving the offset.
	if it.finish {
		return false //if I already did this before
	}
	oldOffset := it.offset
	oldCurrent := it.current
	it.Next()

	//If I moved the offset I wouldn't be able to access current (get)
	it.offset = oldOffset
	it.current = oldCurrent
	return !it.finish
}

// Get returns the record at the current iterator position.
func (it *SSTableIterator) Get() *record.Record {
	return it.current
}

func (sst *SSTable) iteratorSummaryCheck(dirPath, minRange, maxRange string) bool {
	var err error
	var file *os.File
	var header []uint64
	var eof int64
	files, err := readTOC(dirPath)
	if err != nil {
		return false
	}

	if len(files) > 1 {
		path := dirPath + SUMMARYNAME
		file, err = os.Open(path)
		if err != nil {
			return false
		}
		defer file.Close()

		eof, err = file.Seek(0, 2)
		if err != nil {
			return false
		}

		_, err = file.Seek(0, 0)
		if err != nil {
			return false
		}

	} else {
		path := dirPath + SINGLEFILENAME
		file, err = os.Open(path)
		if err != nil {
			return false
		}
		defer file.Close()

		headerBytes := make([]byte, 5*OFFSETSIZE)
		_, err = file.Read(headerBytes)
		if err != nil {
			return false
		}

		header = []uint64{
			binary.LittleEndian.Uint64(headerBytes[:OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[OFFSETSIZE : 2*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[2*OFFSETSIZE : 3*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[3*OFFSETSIZE : 4*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[4*OFFSETSIZE:]),
		}

		_, err = file.Seek(int64(header[2]), 0)
		if err != nil {
			return false
		}

		eof = int64(header[3])
	}

	// reading low-key size
	keySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return false
	}

	keySize := binary.LittleEndian.Uint64(keySizeBytes)

	// reading low-key
	lowKey := make([]byte, keySize)
	_, err = file.Read(lowKey)
	if err != nil {
		return false
	}

	// readinf high-key size
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return false
	}
	keySize = binary.LittleEndian.Uint64(keySizeBytes)

	// reading high-key
	highKey := make([]byte, keySize)
	_, err = file.Read(highKey)
	if err != nil {
		return false
	}

	_ = eof
	// if out of range
	if maxRange < string(lowKey) || minRange > string(highKey) {
		return false
	} else {
		return true
	}
}

func (it *SSTableIterator) Next() {
	var file *os.File
	var header []uint64
	var eof int64
	files, _ := readTOC(it.dirPath)

	if len(files) > 1 {
		path := it.dirPath + INDEXNAME
		file, _ = os.Open(path)

		defer file.Close()

		eof, _ = file.Seek(0, 2)

		_, _ = file.Seek(0, 0)

	} else {
		path := it.dirPath + SINGLEFILENAME
		file, _ = os.Open(path)

		defer file.Close()

		headerBytes := make([]byte, 5*OFFSETSIZE)
		file.Read(headerBytes)

		header = []uint64{
			binary.LittleEndian.Uint64(headerBytes[:OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[OFFSETSIZE : 2*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[2*OFFSETSIZE : 3*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[3*OFFSETSIZE : 4*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[4*OFFSETSIZE:]),
		}

		file.Seek(int64(header[1]), 0)

		eof = int64(header[2])
	}

	file.Seek(int64(it.offset), 0)

	// looping through all entries in one range of index
	for {
		position, _ := file.Seek(0, 1)
		if position == eof {
			break
		}

		// reading key size
		keySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
		file.Read(keySizeBytes)

		keySize := binary.LittleEndian.Uint64(keySizeBytes)

		// reading key
		readKey := make([]byte, keySize)
		file.Read(readKey)

		// reading offset
		offsetBytes := make([]byte, OFFSETSIZE)
		file.Read(offsetBytes)

		offsetData := binary.LittleEndian.Uint64(offsetBytes)

		if it.rangeIterator {
			// stop condition
			if string(readKey) > it.maxRange {
				it.finish = true
				return
			} else if string(readKey) < it.minRange { //if we haven't reached first good element
				continue
			} else { // minRange < string(readKey) < maxRange
				// continue search in Data
				it.offset = offsetData
				rec, _ := it.sst.checkData(offsetData, it.dirPath)
				it.current = rec
				return
			}
		} else {
			if strings.HasPrefix(string(readKey), it.prefix) {
				// continue search in Data
				it.offset = offsetData
				rec, _ := it.sst.checkData(offsetData, it.dirPath)
				it.current = rec
				return
			} else if it.current == nil { //first iteration
				continue //keep searching for the first element
			} else {
				it.finish = true
				return //if not first, and no longer has pre-fix it has no excuse break
			}
		}

	}

	it.finish = true
	return
}
