package sstable

import (
	"encoding/binary"
	"fmt"
	"key-value-engine/structs/bloomFilter"
	"key-value-engine/structs/merkleTree"
	"key-value-engine/structs/record"
	"os"
)

func (sst *SSTable) makeSingleFile(data []*record.Record, dirPath string) error {
	file, err := os.Create(dirPath + string(os.PathSeparator) + SINGLEFILENAME)
	if err != nil {
		return err
	}
	defer file.Close()

	err = sst.makeTOC(dirPath, false)
	if err != nil {
		return err
	}

	bf := bloomFilter.MakeBloomFilter(uint64(len(data)), sst.filterProbability)
	dataSize := 0
	indexSize := 0
	summarySize := 2*record.KEY_SIZE_SIZE + data[0].GetKeySize() + data[len(data)-1].GetKeySize()
	merkleData := make([][]byte, len(data))
	for i, rec := range data {
		sstEntry := rec.SSTRecordToBytes()
		dataSize += len(sstEntry)

		indexSize += OFFSETSIZE + int(rec.GetKeySize()) + record.KEY_SIZE_SIZE

		if i%sst.summaryFactor == 0 {
			summarySize += OFFSETSIZE + rec.GetKeySize() + record.KEY_SIZE_SIZE
		}

		bf.Add([]byte(rec.GetKey()))

		// filling merkleData
		merkleData[i] = sstEntry
	}

	bfBytes := bf.BloomFilterToBytes()
	filterSize := len(bfBytes)

	dataOffset := make([]byte, OFFSETSIZE)
	binary.LittleEndian.PutUint64(dataOffset, uint64(5*OFFSETSIZE))

	indexOffset := make([]byte, OFFSETSIZE)
	binary.LittleEndian.PutUint64(indexOffset, uint64(5*OFFSETSIZE+dataSize))
	offsetHeader := append(dataOffset, indexOffset...)

	summaryOffset := make([]byte, OFFSETSIZE)
	binary.LittleEndian.PutUint64(summaryOffset, uint64(5*OFFSETSIZE+dataSize+indexSize))
	offsetHeader = append(offsetHeader, summaryOffset...)

	filterOffset := make([]byte, OFFSETSIZE)
	binary.LittleEndian.PutUint64(filterOffset, uint64(5*OFFSETSIZE+dataSize+indexSize+int(summarySize)))
	offsetHeader = append(offsetHeader, filterOffset...)

	merkleOffset := make([]byte, OFFSETSIZE)
	binary.LittleEndian.PutUint64(merkleOffset, uint64(5*OFFSETSIZE+dataSize+indexSize+int(summarySize)+filterSize))
	offsetHeader = append(offsetHeader, merkleOffset...)

	_, err = file.Write(offsetHeader)
	if err != nil {
		return fmt.Errorf("error writing file header: %s\n", err)
	}

	// Writting data
	for _, rec := range data {
		_, err := file.Write(rec.SSTRecordToBytes())
		if err != nil {
			return fmt.Errorf("error writing record: %s\n", err)
		}
	}

	// Writting Index
	offsetIndex := 0
	for _, rec := range data {
		result := sst.indexFormatToBytes(rec, offsetIndex)

		_, err := file.Write(result)
		if err != nil {
			return fmt.Errorf("error writing Index entry: %s\n", err)
		}

		// updating offset
		offsetIndex += len(rec.SSTRecordToBytes())
	}

	// Writting Sumarry

	// getting low and high key range
	low := data[0]
	high := data[len(data)-1]

	// serializing low-key
	lowKeySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(lowKeySizeBytes, low.GetKeySize())

	lowKeyBytes := []byte(low.GetKey())

	header := append(lowKeySizeBytes, lowKeyBytes...)

	// serializing high-key
	highKeySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(highKeySizeBytes, high.GetKeySize())

	header = append(header, highKeySizeBytes...)

	highKeyBytes := []byte(high.GetKey())

	header = append(header, highKeyBytes...)

	// writting header
	_, err = file.Write(header)
	if err != nil {
		return fmt.Errorf("error writing header: %s\n", err)
	}

	offsetSummary := 0
	for i, rec := range data {
		// looping by summaryFactor
		if i%sst.summaryFactor == 0 {
			result := sst.indexFormatToBytes(rec, offsetSummary)
			_, err := file.Write(result)
			if err != nil {
				return fmt.Errorf("error writing Index entry: %s\n", err)
			}
		}

		// updating offset
		offsetSummary += OFFSETSIZE + record.KEY_SIZE_SIZE + int(rec.GetKeySize())
	}

	// writting bloom filter
	_, err = file.Write(bfBytes)
	if err != nil {
		return fmt.Errorf("error writing bloom filter to Filter: %s\n", err)
	}

	// making merkle
	mt := merkleTree.MakeMerkleTree(merkleData)
	mtBytes, _ := merkleTree.SerializeMerkleTree(mt)
	if err != nil {
		return fmt.Errorf("error serializing Merkle: %s\n", err)
	}
	// writting merkle
	_, err = file.Write(mtBytes)
	if err != nil {
		return fmt.Errorf("error writing Merkle: %s\n", err)
	}

	return nil

}

func (sst *SSTable) checkSingle(key string, dirPath string) (*record.Record, error) {
	path := dirPath + SINGLEFILENAME
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error reading Summary: %s\n", err)
	}
	defer file.Close()

	headerBytes := make([]byte, 5*OFFSETSIZE)
	_, err = file.Read(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("error reading header: %s\n", err)
	}

	header := []uint64{
		binary.LittleEndian.Uint64(headerBytes[:OFFSETSIZE]),
		binary.LittleEndian.Uint64(headerBytes[OFFSETSIZE : 2*OFFSETSIZE]),
		binary.LittleEndian.Uint64(headerBytes[2*OFFSETSIZE : 3*OFFSETSIZE]),
		binary.LittleEndian.Uint64(headerBytes[3*OFFSETSIZE : 4*OFFSETSIZE]),
		binary.LittleEndian.Uint64(headerBytes[4*OFFSETSIZE:]),
	}

	return sst.checkFilterSingle(key, file, header)
}

func (sst *SSTable) checkFilterSingle(key string, file *os.File, header []uint64) (*record.Record, error) {
	_, err := file.Seek(int64(header[3]), 0)
	if err != nil {
		return nil, fmt.Errorf("error seeking: %s\n", err)
	}
	filterBytes := make([]byte, int64(header[4])-int64(header[3]))

	_, err = file.Read(filterBytes)
	if err != nil {
		return nil, fmt.Errorf("error reading filter: %s\n", err)
	}

	bf, err := bloomFilter.BytesToBloomFilter(filterBytes)
	if err != nil {
		return nil, fmt.Errorf("error reading filter: %s\n", err)
	}

	// not found
	if !bf.IsPresent([]byte(key)) {
		return nil, nil
	} else {
		return sst.checkSummarySingle(key, file, header)
	}

}

func (sst *SSTable) checkSummarySingle(key string, file *os.File, header []uint64) (*record.Record, error) {
	_, err := file.Seek(int64(header[2]), 0)
	if err != nil {
		return nil, fmt.Errorf("error seeking: %s\n", err)
	}

	// reading low-key size
	keySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return nil, fmt.Errorf("error reading low key size: %s\n", err)
	}

	keySize := binary.LittleEndian.Uint64(keySizeBytes)

	// reading low-key
	lowKey := make([]byte, keySize)
	_, err = file.Read(lowKey)
	if err != nil {
		return nil, fmt.Errorf("error reading low key: %s\n", err)
	}

	// readinf high-key size
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return nil, fmt.Errorf("error reading high key size: %s\n", err)
	}
	keySize = binary.LittleEndian.Uint64(keySizeBytes)

	// reading high-key
	highKey := make([]byte, keySize)
	_, err = file.Read(highKey)
	if err != nil {
		return nil, fmt.Errorf("error reading high key: %s\n", err)
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
			return nil, fmt.Errorf("error reading key size: %s\n", err)
		}
		if position == int64(header[3]) {
			return sst.checkIndexSingle(key, lastOffset, file, header)
		}

		_, err = file.Read(keySizeBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading key size: %s\n", err)
		}

		keySize = binary.LittleEndian.Uint64(keySizeBytes)

		// reading key
		readKey := make([]byte, keySize)
		_, err = file.Read(readKey)
		if err != nil {
			return nil, fmt.Errorf("error reading key: %s\n", err)
		}

		// reading offset
		offsetBytes := make([]byte, OFFSETSIZE)
		_, err = file.Read(offsetBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading offset: %s\n", err)
		}
		offset := binary.LittleEndian.Uint64(offsetBytes)

		// checking range
		if string(readKey) > key {
			return sst.checkIndexSingle(key, lastOffset, file, header)
		} else {
			// updating lastOffset
			lastOffset = offset
		}

	}
}

func (sst *SSTable) checkIndexSingle(key string, offset uint64, file *os.File, header []uint64) (*record.Record, error) {
	_, err := file.Seek(int64(header[1])+int64(offset), 0)
	if err != nil {
		return nil, fmt.Errorf("error seeking: %s\n", err)
	}

	// looping through all entries in one range of index
	for i := 0; i < sst.summaryFactor; i++ {
		position, err := file.Seek(0, 1)
		if err != nil {
			return nil, fmt.Errorf("error reading key size: %s\n", err)
		}
		if position == int64(header[2]) {
			break
		}

		// reading key size
		keySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
		_, err = file.Read(keySizeBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading key size: %s\n", err)
		}
		keySize := binary.LittleEndian.Uint64(keySizeBytes)

		// reading key
		readKey := make([]byte, keySize)
		_, err = file.Read(readKey)
		if err != nil {
			return nil, fmt.Errorf("error reading key: %s\n", err)
		}

		// reading offset
		offsetBytes := make([]byte, OFFSETSIZE)
		_, err = file.Read(offsetBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading offset: %s\n", err)
		}
		offset := binary.LittleEndian.Uint64(offsetBytes)

		// checking range
		if string(readKey) > key {
			return nil, nil
		} else if string(readKey) == key {
			// continue search in Data
			return sst.checkDataSingle(offset, file, header)
		}

	}
	return nil, nil
}

func (sst *SSTable) checkDataSingle(offset uint64, file *os.File, header []uint64) (*record.Record, error) {
	// seeking to position
	_, err := file.Seek(int64(header[0])+int64(offset), 0)
	if err != nil {
		return nil, fmt.Errorf("error seekign in Data: %s\n", err)
	}

	// reading header without value-size
	headerBytes := make([]byte, record.RECORD_HEADER_SIZE-record.VALUE_SIZE_SIZE)
	_, err = file.Read(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("error reading header: %s\n", err)
	}

	// two cases if tombstone or not
	var recBytes []byte
	if headerBytes[record.TOMBSTONE_START] == 1 {
		// reading key size
		keySize := binary.LittleEndian.Uint64(headerBytes[record.KEY_SIZE_START:record.VALUE_SIZE_START])
		secondPartBytes := make([]byte, keySize)
		_, err := file.Read(secondPartBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading secong part of record: %s\n", err)
		}

		recBytes = append(headerBytes, secondPartBytes...)
	} else {
		// getting key size
		keySize := binary.LittleEndian.Uint64(headerBytes[record.KEY_SIZE_START:record.VALUE_SIZE_START])

		// reading value size
		valSizeBytes := make([]byte, record.VALUE_SIZE_SIZE)
		_, err := file.Read(valSizeBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading value size: %s\n", err)
		}
		valSize := binary.LittleEndian.Uint64(valSizeBytes)

		// reading rest of the bytes
		secondPartBytes := make([]byte, keySize+valSize)
		_, err = file.Read(secondPartBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading second part of record: %s\n", err)
		}

		recBytes = append(headerBytes, valSizeBytes...)
		recBytes = append(recBytes, secondPartBytes...)
	}

	valid, err := sst.checkMerkleSingle(recBytes, file, header)
	if err != nil {
		return nil, fmt.Errorf("error checking merkle: %s\n", err)
	}

	if !valid {
		return nil, fmt.Errorf("Value not valid!\n")
	}

	return record.SSTBytesToRecord(recBytes), nil
}

func (sst *SSTable) checkMerkleSingle(bytes []byte, file *os.File, header []uint64) (bool, error) {
	_, err := file.Seek(int64(header[4]), 0)
	if err != nil {
		return false, fmt.Errorf("error seekign in Merkle: %s\n", err)
	}

	// Get file information
	fileInfo, statErr := file.Stat()
	if statErr != nil {
		fmt.Println("error getting file information:", statErr)
		return false, nil
	}

	sizeToEnd := fileInfo.Size() - int64(header[4])

	mtBytes := make([]byte, sizeToEnd)
	_, err = file.Read(mtBytes)
	if err != nil {
		return false, fmt.Errorf("error reading Merkle: %s\n", err)
	}

	mt, err := merkleTree.DeserializeMerkleTree(mtBytes)
	if err != nil {
		return false, fmt.Errorf("error deserializing Merkle: %s\n", err)
	}

	valid, _ := mt.CheckValidityOfNode(bytes)

	return valid, nil
}
