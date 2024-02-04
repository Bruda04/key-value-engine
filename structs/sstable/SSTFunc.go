package sstable

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"key-value-engine/structs/bloomFilter"
	"key-value-engine/structs/merkleTree"
	"key-value-engine/structs/record"
	"os"
)

func (sst *SSTable) putData(rec *record.Record, dirPath string) error {
	var sstEntry []byte
	if sst.compression {
		globalDictData, err := os.ReadFile(dirPath + string(os.PathSeparator) + GLOBALDICTNAME)
		if err != nil {
			return errors.New("error reading sst file")
		}
		globalDict := make(map[string]int)
		err = json.Unmarshal(globalDictData, &globalDict)
		if err != nil {
			return errors.New("error reading json file")
		}
		dictIndex := len(globalDict) + 1

		_, exists := globalDict[rec.GetKey()]
		if !exists {
			globalDict[rec.GetKey()] = dictIndex
		} else {
			dictIndex = globalDict[rec.GetKey()]
		}

		sstEntry = rec.SSTRecordToBytes(dictIndex)

		marshalled, err := json.MarshalIndent(globalDict, "", "  ")
		if err != nil {
			return errors.New("error converting to json file")

		}

		// Write the JSON data to the file
		err = os.WriteFile(dirPath+string(os.PathSeparator)+GLOBALDICTNAME, marshalled, 0644)
		if err != nil {
			return errors.New("error writting sst file")
		}

		entrySerSizeBytes := make([]byte, binary.MaxVarintLen64)
		entrySizeLen := binary.PutUvarint(entrySerSizeBytes, uint64(len(sstEntry)))
		sstEntry = append(entrySerSizeBytes[:entrySizeLen], sstEntry...)

	} else {
		sstEntry = rec.RecordToBytes()
	}

	var file *os.File
	var err error
	if sst.multipleFiles {
		path := dirPath + string(os.PathSeparator) + DATANAME
		file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer file.Close()

		_, err := file.Seek(0, 2)
		if err != nil {
			return errors.New("error reading sst file")
		}

	} else {
		path := dirPath + string(os.PathSeparator) + SINGLEFILENAME
		file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer file.Close()

		pos, err := file.Seek(0, 2)
		if err != nil {
			return errors.New("error reading sst file")
		}
		if pos < HEADERSIZE {
			_, err = file.Seek(0, 0)
			if err != nil {
				return errors.New("error reading sst file")
			}

			dataOffsetBytes := make([]byte, OFFSETSIZE)
			binary.LittleEndian.PutUint64(dataOffsetBytes, HEADERSIZE)

			_, err = file.Write(dataOffsetBytes)
			if err != nil {
				return errors.New("error writting sst file")
			}

			_, err := file.Seek(HEADERSIZE, 0)
			if err != nil {
				return errors.New("error reading sst file")
			}
		}
	}

	_, err = file.Write(sstEntry)
	if err != nil {
		return errors.New("error writting sst file")
	}

	if !sst.multipleFiles {
		pos, err := file.Seek(0, 1)
		if err != nil {
			return errors.New("error reading sst file")
		}

		indexOffsetBytes := make([]byte, OFFSETSIZE)
		binary.LittleEndian.PutUint64(indexOffsetBytes, uint64(pos))

		_, err = file.Seek(OFFSETSIZE, 0)
		if err != nil {
			return errors.New("error reading sst file")
		}

		_, err = file.Write(indexOffsetBytes)
		if err != nil {
			return errors.New("error writting sst file")
		}

	}

	return nil
}

func (sst *SSTable) formIndex(dirPath string) error {
	var err error
	var file *os.File
	var dataFile *os.File
	var eofData int64
	var dataPos int64
	var indexPos int64
	if sst.multipleFiles {
		path := dirPath + string(os.PathSeparator) + INDEXNAME
		file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer file.Close()

		pathData := dirPath + string(os.PathSeparator) + DATANAME
		dataFile, err = os.OpenFile(pathData, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer dataFile.Close()

		eofData, err = dataFile.Seek(0, 2)
		if err != nil {
			return errors.New("error reading sst file")
		}

		dataPos, err = dataFile.Seek(0, 0)
		if err != nil {
			return errors.New("error reading sst file")
		}

	} else {
		path := dirPath + string(os.PathSeparator) + SINGLEFILENAME
		file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer file.Close()

		dataFile = file

		eofData, err = file.Seek(0, 2)
		if err != nil {
			return errors.New("error reading sst file")
		}

		indexPos = eofData

		dataPos, err = dataFile.Seek(HEADERSIZE, 0)
		if err != nil {
			return errors.New("error reading sst file")
		}
	}

	var globalDict map[string]int
	if sst.compression {
		globalDictData, err := os.ReadFile(dirPath + string(os.PathSeparator) + GLOBALDICTNAME)
		if err != nil {
			return errors.New("error reading json file")
		}
		globalDict = make(map[string]int)
		err = json.Unmarshal(globalDictData, &globalDict)
		if err != nil {
			return errors.New("error converting json file")
		}
	}

	var offset int64
	for {
		if dataPos >= eofData {
			break
		}

		offset = dataPos

		if !sst.multipleFiles {
			_, err = dataFile.Seek(dataPos, 0)
			if err != nil {
				return errors.New("error reading sst file")
			}
		}

		// READ RECORD
		var sstEntry *record.Record
		if sst.compression {

			var bufSize [binary.MaxVarintLen64]byte

			// Read the SSTEntry size from the file into the buffer
			_, err = dataFile.Read(bufSize[:])
			if err != nil {
				return errors.New("error reading sst file")
			}

			// Decode the SSTEntry size from the buffer
			entrySize, bytesRead := binary.Uvarint(bufSize[:])

			_, err = dataFile.Seek(-int64(binary.MaxVarintLen64-bytesRead), 1)
			if err != nil {
				return errors.New("error reading sst file")
			}

			entryBytes := make([]byte, entrySize)

			_, err = dataFile.Read(entryBytes)
			if err != nil {
				return errors.New("error reading sst file")
			}

			sstEntry, err = record.SSTBytesToRecord(entryBytes, &globalDict)
			if err != nil {
				return err
			}

			dataPos += int64(len(entryBytes) + bytesRead)

		} else {
			// reading header without value-size
			headerBytes := make([]byte, record.RECORD_HEADER_SIZE)
			_, err = dataFile.Read(headerBytes)
			if err != nil {
				return errors.New("error reading sst file")
			}

			var recBytes []byte

			keySize := binary.LittleEndian.Uint64(headerBytes[record.KEY_SIZE_START:record.VALUE_SIZE_START])
			valSize := binary.LittleEndian.Uint64(headerBytes[record.VALUE_SIZE_START:record.KEY_START])

			// reading rest of the bytes
			secondPartBytes := make([]byte, keySize+valSize)
			_, err = dataFile.Read(secondPartBytes)
			if err != nil {
				return errors.New("error reading sst file")
			}

			recBytes = append(headerBytes, secondPartBytes...)

			sstEntry = record.BytesToRecord(recBytes)
			dataPos += int64(len(recBytes))

		}

		if !sst.multipleFiles {
			_, err = file.Seek(indexPos, 0)
			if err != nil {
				return errors.New("error reading sst file")
			}
		}

		indexEntry := sst.indexFormatToBytes(sstEntry, int(offset))

		_, err = file.Write(indexEntry)
		if err != nil {
			return errors.New("error writting sst file")
		}

		if !sst.multipleFiles {
			indexPos += int64(len(indexEntry))
		}

	}

	if !sst.multipleFiles {
		_, err = file.Seek(2*OFFSETSIZE, 0)
		if err != nil {
			return errors.New("error reading sst file")
		}

		summaryOffsetBytes := make([]byte, OFFSETSIZE)
		binary.LittleEndian.PutUint64(summaryOffsetBytes, uint64(indexPos))

		_, err = file.Write(summaryOffsetBytes)
		if err != nil {
			return errors.New("error writting sst file")
		}
	}

	return nil

}

func (sst *SSTable) formSummary(dirPath string) error {
	var err error
	var summFile *os.File
	var indexFile *os.File
	var eofIndex int64
	var indexPos int64
	if sst.multipleFiles {
		path := dirPath + string(os.PathSeparator) + SUMMARYNAME
		summFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer summFile.Close()

		pathIndex := dirPath + string(os.PathSeparator) + INDEXNAME
		indexFile, err = os.OpenFile(pathIndex, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer indexFile.Close()

		eofIndex, err = indexFile.Seek(0, 2)
		if err != nil {
			return errors.New("error reading sst file")
		}

		indexPos, err = indexFile.Seek(0, 0)
		if err != nil {
			return errors.New("error reading sst file")
		}

	} else {
		path := dirPath + string(os.PathSeparator) + SINGLEFILENAME
		summFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer summFile.Close()

		indexFile = summFile

		eofIndex, err = summFile.Seek(0, 2)
		if err != nil {
			return errors.New("error reading sst file")
		}

		_, err = summFile.Seek(0, 0)
		if err != nil {
			return errors.New("error reading sst file")
		}

		headerBytes := make([]byte, 5*OFFSETSIZE)
		_, err = summFile.Read(headerBytes)
		if err != nil {
			return errors.New("error reading sst file")
		}

		indexOffset := binary.LittleEndian.Uint64(headerBytes[OFFSETSIZE : 2*OFFSETSIZE])

		indexPos, err = indexFile.Seek(int64(indexOffset), 0)
		if err != nil {
			return errors.New("error reading sst file")
		}
	}

	var offset int64
	var summData []byte
	var minKey []byte
	var maxKey []byte
	i := 0
	for {
		if indexPos >= eofIndex {
			break
		}

		offset = indexPos

		// READ ENTRY
		keySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
		_, err = indexFile.Read(keySizeBytes)
		if err != nil {
			return errors.New("error reading sst file")
		}
		keySize := binary.LittleEndian.Uint64(keySizeBytes)

		// reading key
		readKey := make([]byte, keySize)
		_, err = indexFile.Read(readKey)
		if err != nil {
			return errors.New("error reading sst file")
		}

		if len(summData) == 0 {
			minKey = readKey
		}

		maxKey = readKey

		_, err = indexFile.Seek(OFFSETSIZE, 1)
		if err != nil {
			return errors.New("error reading sst file")
		}

		if i%sst.summaryFactor == 0 {
			offsetBytes := make([]byte, OFFSETSIZE)
			binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))

			summData = append(summData, keySizeBytes...)
			summData = append(summData, readKey...)
			summData = append(summData, offsetBytes...)
		}

		indexPos, err = indexFile.Seek(0, 1)
		i++
	}

	minKeyLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(minKeyLen, uint64(len(minKey)))

	maxKeyLen := make([]byte, 8)
	binary.LittleEndian.PutUint64(maxKeyLen, uint64(len(maxKey)))

	headerData := append(minKeyLen, minKey...)
	headerData = append(headerData, maxKeyLen...)
	headerData = append(headerData, maxKey...)

	summData = append(headerData, summData...)

	_, err = summFile.Write(summData)
	if err != nil {
		return errors.New("error writting sst file")
	}

	if !sst.multipleFiles {
		pos, err := summFile.Seek(0, 1)
		if err != nil {
			return errors.New("error reading sst file")
		}

		_, err = summFile.Seek(3*OFFSETSIZE, 0)
		if err != nil {
			return errors.New("error reading sst file")
		}

		bloomOffsetBytes := make([]byte, OFFSETSIZE)
		binary.LittleEndian.PutUint64(bloomOffsetBytes, uint64(pos))

		_, err = summFile.Write(bloomOffsetBytes)
		if err != nil {
			return errors.New("error writting sst file")
		}
	}

	return nil
}

func (sst *SSTable) formBfMt(dirPath string, dataLen int) error {
	var err error
	var dataFile *os.File
	var eofData int64
	if sst.multipleFiles {
		path := dirPath + string(os.PathSeparator) + DATANAME
		dataFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer dataFile.Close()

		eofData, err = dataFile.Seek(0, 2)
		if err != nil {
			return errors.New("error reading sst file")
		}

		_, err = dataFile.Seek(0, 0)
		if err != nil {
			return errors.New("error reading sst file")
		}
	} else {
		path := dirPath + string(os.PathSeparator) + SINGLEFILENAME
		dataFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer dataFile.Close()

		headerBytes := make([]byte, 5*OFFSETSIZE)
		_, err = dataFile.Read(headerBytes)
		if err != nil {
			return errors.New("error reading sst file")
		}

		dataOffset := binary.LittleEndian.Uint64(headerBytes[:OFFSETSIZE])
		eofData = int64(binary.LittleEndian.Uint64(headerBytes[OFFSETSIZE : 2*OFFSETSIZE]))

		_, err = dataFile.Seek(int64(dataOffset), 0)
		if err != nil {
			return errors.New("error reading sst file")
		}
	}

	bf := bloomFilter.MakeBloomFilter(uint64(dataLen), sst.filterProbability)
	mt := merkleTree.MakeMerkleTree()
	i := 0
	for {
		pos, err := dataFile.Seek(0, 1)
		if err != nil {
			return errors.New("error reading sst file")
		}

		if pos >= eofData {
			break
		}

		// READ RECORD
		var entryBytes []byte
		var rec *record.Record
		if sst.compression {
			globalDictData, err := os.ReadFile(dirPath + string(os.PathSeparator) + GLOBALDICTNAME)
			if err != nil {
				return errors.New("error reading sst file")
			}
			globalDict := make(map[string]int)
			err = json.Unmarshal(globalDictData, &globalDict)
			if err != nil {
				return errors.New("error converting json file")
			}

			var bufSize [binary.MaxVarintLen64]byte

			// Read the SSTEntry size from the file into the buffer
			_, err = dataFile.Read(bufSize[:])
			if err != nil {
				return errors.New("error reading sst file")
			}

			// Decode the SSTEntry size from the buffer
			entrySize, bytesRead := binary.Uvarint(bufSize[:])

			_, err = dataFile.Seek(-int64(binary.MaxVarintLen64-bytesRead), 1)
			if err != nil {
				return errors.New("error reading sst file")
			}

			entryBytes = make([]byte, entrySize)

			_, err = dataFile.Read(entryBytes)
			if err != nil {
				return errors.New("error reading sst file")
			}
			rec, err = record.SSTBytesToRecord(entryBytes, &globalDict)
			if err != nil {
				return err
			}
		} else {
			// reading header without value-size
			headerBytes := make([]byte, record.RECORD_HEADER_SIZE)
			_, err = dataFile.Read(headerBytes)
			if err != nil {
				return errors.New("error reading sst file")
			}

			keySize := binary.LittleEndian.Uint64(headerBytes[record.KEY_SIZE_START:record.VALUE_SIZE_START])
			valSize := binary.LittleEndian.Uint64(headerBytes[record.VALUE_SIZE_START:record.KEY_START])

			// reading rest of the bytes
			secondPartBytes := make([]byte, keySize+valSize)
			_, err = dataFile.Read(secondPartBytes)
			if err != nil {
				return errors.New("error reading sst file")
			}

			entryBytes = append(headerBytes, secondPartBytes...)
			rec = record.BytesToRecord(entryBytes)
		}

		bf.Add([]byte(rec.GetKey()))
		mt.Add(entryBytes)
		i++
	}

	mt.FormMerkleTree()

	bfBytes := bf.BloomFilterToBytes()
	mtBytes, err := merkleTree.MerkleTreeToBytes(mt)
	if err != nil {
		return err
	}

	if sst.multipleFiles {
		path := dirPath + string(os.PathSeparator) + BLOOMNAME
		bfFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer bfFile.Close()

		path = dirPath + string(os.PathSeparator) + MERKLENAME
		mtFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return errors.New("error reading sst file")
		}
		defer mtFile.Close()

		// writting bloom filter
		_, err = bfFile.Write(bfBytes)
		if err != nil {
			return err
		}

		// writting merkle
		_, err = mtFile.Write(mtBytes)
		if err != nil {
			return errors.New("error writting sst file")
		}
	} else {
		_, err = dataFile.Seek(0, 2)
		if err != nil {
			return errors.New("error reading sst file")
		}

		_, err = dataFile.Write(bfBytes)
		if err != nil {
			return errors.New("error writting sst file")
		}

		mtOffset, err := dataFile.Seek(0, 1)
		if err != nil {
			return errors.New("error reading sst file")
		}

		_, err = dataFile.Seek(4*OFFSETSIZE, 0)
		if err != nil {
			return errors.New("error reading sst file")
		}

		mtOffsetBytes := make([]byte, OFFSETSIZE)
		binary.LittleEndian.PutUint64(mtOffsetBytes, uint64(mtOffset))

		_, err = dataFile.Write(mtOffsetBytes)
		if err != nil {
			return errors.New("error writting sst file")
		}

		_, err = dataFile.Seek(0, 2)
		if err != nil {
			return errors.New("error reading sst file")
		}

		_, err = dataFile.Write(mtBytes)
		if err != nil {
			return errors.New("error writting sst file")
		}

	}

	return nil

}

func (sst *SSTable) checkBf(key string, dirPath string) (*record.Record, error) {
	var err error
	var filterBytes []byte
	files, err := readTOC(dirPath)
	if err != nil {
		return nil, err
	}

	if len(files) > 1 {
		path := dirPath + BLOOMNAME

		filterBytes, err = os.ReadFile(path)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

	} else {
		path := dirPath + SINGLEFILENAME
		file, err := os.Open(path)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		defer file.Close()

		headerBytes := make([]byte, 5*OFFSETSIZE)
		_, err = file.Read(headerBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		header := []uint64{
			binary.LittleEndian.Uint64(headerBytes[:OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[OFFSETSIZE : 2*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[2*OFFSETSIZE : 3*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[3*OFFSETSIZE : 4*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[4*OFFSETSIZE:]),
		}

		_, err = file.Seek(int64(header[3]), 0)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		filterBytes = make([]byte, int64(header[4])-int64(header[3]))

		_, err = file.Read(filterBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
	}

	bf, err := bloomFilter.BytesToBloomFilter(filterBytes)
	if err != nil {
		return nil, err
	}

	// not found
	if !bf.IsPresent([]byte(key)) {
		return nil, nil
	} else {
		// continue search in Summary
		return sst.checkSummary(key, dirPath)
	}
}

func (sst *SSTable) checkSummary(key string, dirPath string) (*record.Record, error) {
	var err error
	var file *os.File
	var header []uint64
	var eof int64
	files, err := readTOC(dirPath)
	if err != nil {
		return nil, err
	}

	if len(files) > 1 {
		path := dirPath + SUMMARYNAME
		file, err = os.Open(path)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		defer file.Close()

		eof, err = file.Seek(0, 2)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		_, err = file.Seek(0, 0)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

	} else {
		path := dirPath + SINGLEFILENAME
		file, err = os.Open(path)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		defer file.Close()

		headerBytes := make([]byte, 5*OFFSETSIZE)
		_, err = file.Read(headerBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
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
			return nil, errors.New("error reading sst file")
		}

		eof = int64(header[3])
	}

	// reading low-key size
	keySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return nil, errors.New("error reading sst file")
	}

	keySize := binary.LittleEndian.Uint64(keySizeBytes)

	// reading low-key
	lowKey := make([]byte, keySize)
	_, err = file.Read(lowKey)
	if err != nil {
		return nil, errors.New("error reading sst file")
	}

	// readinf high-key size
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return nil, errors.New("error reading sst file")
	}
	keySize = binary.LittleEndian.Uint64(keySizeBytes)

	// reading high-key
	highKey := make([]byte, keySize)
	_, err = file.Read(highKey)
	if err != nil {
		return nil, errors.New("error reading sst file")
	}

	// if out of range
	if key < string(lowKey) || key > string(highKey) {
		return nil, nil
	}

	lastOffset := uint64(0)

	for {
		// reading key size
		position, err := file.Seek(0, 1)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		if position == eof {
			return sst.checkIndex(key, dirPath, lastOffset)
		}

		_, err = file.Read(keySizeBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		keySize = binary.LittleEndian.Uint64(keySizeBytes)

		// reading key
		readKey := make([]byte, keySize)
		_, err = file.Read(readKey)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		// reading offset
		offsetBytes := make([]byte, OFFSETSIZE)
		_, err = file.Read(offsetBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		offset := binary.LittleEndian.Uint64(offsetBytes)

		// checking range
		if string(readKey) > key {
			return sst.checkIndex(key, dirPath, lastOffset)
		} else {
			// updating lastOffset
			lastOffset = offset
		}

	}
}

func (sst *SSTable) checkIndex(key string, dirPath string, offset uint64) (*record.Record, error) {
	var err error
	var file *os.File
	var header []uint64
	var eof int64
	files, err := readTOC(dirPath)
	if err != nil {
		return nil, err
	}

	if len(files) > 1 {
		path := dirPath + INDEXNAME
		file, err = os.Open(path)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		defer file.Close()

		eof, err = file.Seek(0, 2)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		_, err = file.Seek(0, 0)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

	} else {
		path := dirPath + SINGLEFILENAME
		file, err = os.Open(path)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		defer file.Close()

		headerBytes := make([]byte, 5*OFFSETSIZE)
		_, err = file.Read(headerBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		header = []uint64{
			binary.LittleEndian.Uint64(headerBytes[:OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[OFFSETSIZE : 2*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[2*OFFSETSIZE : 3*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[3*OFFSETSIZE : 4*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[4*OFFSETSIZE:]),
		}

		_, err = file.Seek(int64(header[1]), 0)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		eof = int64(header[2])
	}

	// seeking to position
	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		return nil, errors.New("error reading sst file")
	}

	// looping through all entries in one range of index
	for i := 0; i < sst.summaryFactor; i++ {
		position, err := file.Seek(0, 1)
		if position == eof {
			break
		}
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		// reading key size
		keySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
		_, err = file.Read(keySizeBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		keySize := binary.LittleEndian.Uint64(keySizeBytes)

		// reading key
		readKey := make([]byte, keySize)
		_, err = file.Read(readKey)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		// reading offset
		offsetBytes := make([]byte, OFFSETSIZE)
		_, err = file.Read(offsetBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		offsetData := binary.LittleEndian.Uint64(offsetBytes)

		// checking range
		if string(readKey) > key {
			return nil, nil
		} else if string(readKey) == key {
			// continue search in Data
			return sst.checkData(offsetData, dirPath)
		}

	}
	return nil, nil
}

func (sst *SSTable) checkData(offset uint64, dirPath string) (*record.Record, error) {
	var err error
	var file *os.File
	var header []uint64
	files, err := readTOC(dirPath)
	if err != nil {
		return nil, err
	}

	if len(files) > 1 {
		path := dirPath + DATANAME
		file, err = os.Open(path)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		defer file.Close()
	} else {
		path := dirPath + SINGLEFILENAME
		file, err = os.Open(path)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		defer file.Close()

		headerBytes := make([]byte, 5*OFFSETSIZE)
		_, err = file.Read(headerBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		header = []uint64{
			binary.LittleEndian.Uint64(headerBytes[:OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[OFFSETSIZE : 2*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[2*OFFSETSIZE : 3*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[3*OFFSETSIZE : 4*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[4*OFFSETSIZE:]),
		}

		_, err = file.Seek(int64(header[0]), 0)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
	}

	// seeking to position
	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		return nil, errors.New("error reading sst file")
	}

	var recBytes []byte
	var ret *record.Record
	if sst.compression {
		var bufSize [binary.MaxVarintLen64]byte

		// Read the SSTEntry size from the file into the buffer
		_, err = file.Read(bufSize[:])
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		// Decode the SSTEntry size from the buffer
		entrySize, bytesRead := binary.Uvarint(bufSize[:])

		_, err = file.Seek(-int64(binary.MaxVarintLen64-bytesRead), 1)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		entryBytes := make([]byte, entrySize)

		_, err = file.Read(entryBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		globalDictData, err := os.ReadFile(dirPath + string(os.PathSeparator) + GLOBALDICTNAME)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}
		globalDict := make(map[string]int)
		err = json.Unmarshal(globalDictData, &globalDict)
		if err != nil {
			return nil, errors.New("error converting json file")
		}

		ret, err = record.SSTBytesToRecord(entryBytes, &globalDict)
		if err != nil {
			return nil, err
		}

		recBytes = entryBytes
	} else {
		// reading header without value-size
		headerBytes := make([]byte, record.RECORD_HEADER_SIZE)
		_, err = file.Read(headerBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		// getting key size
		keySize := binary.LittleEndian.Uint64(headerBytes[record.KEY_SIZE_START:record.VALUE_SIZE_START])
		valSize := binary.LittleEndian.Uint64(headerBytes[record.VALUE_SIZE_START:record.KEY_START])

		// reading rest of the bytes
		secondPartBytes := make([]byte, keySize+valSize)
		_, err = file.Read(secondPartBytes)
		if err != nil {
			return nil, errors.New("error reading sst file")
		}

		recBytes = append(headerBytes, secondPartBytes...)

		ret = record.BytesToRecord(recBytes)
	}

	valid, err := sst.checkMerkle(recBytes, dirPath)
	if err != nil {
		return nil, err
	}

	if !valid {
		return nil, errors.New("error value not valid")
	}

	return ret, nil
}

func (sst *SSTable) checkMerkle(data []byte, dirPath string) (bool, error) {
	var err error
	var file *os.File
	var header []uint64
	var eof int64
	files, err := readTOC(dirPath)
	if err != nil {
		return false, err
	}

	if len(files) > 1 {
		path := dirPath + MERKLENAME
		file, err = os.Open(path)
		if err != nil {
			return false, errors.New("error reading sst file")
		}
		defer file.Close()

		eof, err = file.Seek(0, 2)
		if err != nil {
			return false, errors.New("error reading sst file")
		}

		_, err = file.Seek(0, 0)
		if err != nil {
			return false, errors.New("error reading sst file")
		}
	} else {
		path := dirPath + SINGLEFILENAME
		file, err = os.Open(path)
		if err != nil {
			return false, errors.New("error reading sst file")
		}
		defer file.Close()

		headerBytes := make([]byte, 5*OFFSETSIZE)
		_, err = file.Read(headerBytes)
		if err != nil {
			return false, errors.New("error reading sst file")
		}

		header = []uint64{
			binary.LittleEndian.Uint64(headerBytes[:OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[OFFSETSIZE : 2*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[2*OFFSETSIZE : 3*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[3*OFFSETSIZE : 4*OFFSETSIZE]),
			binary.LittleEndian.Uint64(headerBytes[4*OFFSETSIZE:]),
		}

		eof, err = file.Seek(0, 2)
		if err != nil {
			return false, errors.New("error reading sst file")
		}

		_, err = file.Seek(int64(header[4]), 0)
		if err != nil {
			return false, errors.New("error reading sst file")
		}
	}

	pos, err := file.Seek(0, 1)

	mtBytes := make([]byte, eof-pos)
	_, err = file.Read(mtBytes)
	if err != nil {
		return false, errors.New("error reading sst file")
	}

	mt, err := merkleTree.BytesToMerkleTree(mtBytes)
	if err != nil {
		return false, err
	}

	valid, _ := mt.CheckValidityOfNode(data)

	return valid, nil
}
