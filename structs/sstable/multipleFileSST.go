package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"key-value-engine/structs/bloomFilter"
	"key-value-engine/structs/merkleTree"
	"key-value-engine/structs/record"
	"os"
)

func (sst *SSTable) makeMultipleFiles(data []*record.Record, dirPath string) error {
	fileData, err := os.Create(dirPath + string(os.PathSeparator) + DATANAME)
	if err != nil {
		return err
	}
	defer fileData.Close()

	fileIndex, err := os.Create(dirPath + string(os.PathSeparator) + INDEXNAME)
	if err != nil {
		return fmt.Errorf("error creating Index: %s\n", err)
	}
	defer fileIndex.Close()

	fileSummary, err := os.Create(dirPath + string(os.PathSeparator) + SUMMARYNAME)
	if err != nil {
		return fmt.Errorf("error creating Summary: %s\n", err)
	}
	defer fileSummary.Close()

	fileFilter, err := os.Create(dirPath + string(os.PathSeparator) + BLOOMNAME)
	if err != nil {
		return fmt.Errorf("error creating Filter: %s\n", err)
	}
	defer fileFilter.Close()

	fileMerkle, err := os.Create(dirPath + string(os.PathSeparator) + MERKLENAME)
	if err != nil {
		return fmt.Errorf("error creating Merkle: %s\n", err)
	}
	defer fileFilter.Close()

	err = sst.makeTOC(dirPath, true)
	if err != nil {
		return err
	}

	// getting low and high key range
	low := data[0]
	high := data[len(data)-1]

	// serializing low-key
	lowKeySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(lowKeySizeBytes, low.GetKeySize())

	lowKeyBytes := []byte(low.GetKey())

	summaryHeader := append(lowKeySizeBytes, lowKeyBytes...)

	// serializing high-key
	highKeySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(highKeySizeBytes, high.GetKeySize())

	summaryHeader = append(summaryHeader, highKeySizeBytes...)

	highKeyBytes := []byte(high.GetKey())

	summaryHeader = append(summaryHeader, highKeyBytes...)

	// writting header
	_, err = fileSummary.Write(summaryHeader)
	if err != nil {
		return fmt.Errorf("error writing header: %s\n", err)
	}

	bf := bloomFilter.MakeBloomFilter(uint64(len(data)), sst.filterProbability)
	offsetIndex := 0
	offsetSummary := 0
	merkleData := make([][]byte, len(data))
	for i, rec := range data {
		// Making Data
		sstEntry := rec.SSTRecordToBytes()
		_, err := fileData.Write(sstEntry)
		if err != nil {
			return fmt.Errorf("error writing record: %s\n", err)
		}

		// Making Index
		indexEntry := sst.indexFormatToBytes(rec, offsetIndex)

		_, err = fileIndex.Write(indexEntry)
		if err != nil {
			return fmt.Errorf("error writing Index entry: %s\n", err)
		}

		// updating offset
		offsetIndex += len(sstEntry)

		// Making Summary
		if i%sst.summaryFactor == 0 {
			summaryEntry := sst.indexFormatToBytes(rec, offsetSummary)
			_, err := fileSummary.Write(summaryEntry)
			if err != nil {
				return fmt.Errorf("error writing Index entry: %s\n", err)
			}
		}

		// updating offset
		offsetSummary += OFFSETSIZE + record.KEY_SIZE_SIZE + int(rec.GetKeySize())

		// populating bloom filter
		bf.Add([]byte(rec.GetKey()))

		// filling merkleData
		merkleData[i] = sstEntry
	}

	// writting bloom filter
	_, err = fileFilter.Write(bf.BloomFilterToBytes())
	if err != nil {
		return fmt.Errorf("error writing bloom filter to Filter: %s\n", err)
	}

	// making merkle
	merkleT := merkleTree.MakeMerkleTree(merkleData)
	merkleTBytes, err := merkleTree.SerializeMerkleTree(merkleT)
	if err != nil {
		return fmt.Errorf("error serializing merkle tree: %s\n", err)
	}

	// writting merkle
	_, err = fileMerkle.Write(merkleTBytes)
	if err != nil {
		return fmt.Errorf("error writing bloom filter to Filter: %s\n", err)
	}

	return nil
}

func (sst *SSTable) checkMultiple(key string, dirPath string) (*record.Record, error) {
	return sst.checkFilter(key, dirPath)
}

func (sst *SSTable) checkFilter(key string, subdirPath string) (*record.Record, error) {
	path := subdirPath + BLOOMNAME

	bloomBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading Filter: %s\n", err)
	}

	bf, err := bloomFilter.BytesToBloomFilter(bloomBytes)
	if err != nil {
		return nil, fmt.Errorf("error deserializing Filter: %s\n", err)
	}

	// not found
	if !bf.IsPresent([]byte(key)) {
		return nil, nil
	} else {
		// continue search in Summary
		return sst.checkSummary(key, subdirPath)
	}
}

func (sst *SSTable) checkSummary(key string, subdirPath string) (*record.Record, error) {
	path := subdirPath + SUMMARYNAME
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error reading Summary: %s\n", err)
	}
	defer file.Close()

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
	// looping through entries
	for {
		// reading key size
		_, err := file.Read(keySizeBytes)
		if err == io.EOF {
			return sst.checkIndex(key, lastOffset, subdirPath)
		}
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
			return sst.checkIndex(key, lastOffset, subdirPath)
		} else {
			// updating lastOffset
			lastOffset = offset
		}

	}

}

func (sst *SSTable) checkIndex(key string, offset uint64, subdirPath string) (*record.Record, error) {
	path := subdirPath + INDEXNAME
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error reading Index: %s\n", err)
	}
	defer file.Close()

	// seeking to position
	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		return nil, fmt.Errorf("error seekig in Index: %s\n", err)
	}

	// looping through all entries in one range of index
	for i := 0; i < sst.summaryFactor; i++ {
		// reading key size
		keySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
		_, err := file.Read(keySizeBytes)
		if err == io.EOF {
			break
		}
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
			return sst.checkData(offset, subdirPath)
		}

	}

	return nil, nil
}

func (sst *SSTable) checkData(offset uint64, subdirPath string) (*record.Record, error) {
	path := subdirPath + DATANAME
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error reading Data: %s\n", err)
	}
	defer file.Close()

	// seeking to position
	_, err = file.Seek(int64(offset), 0)
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

	valid, err := sst.checkMerkleMultiple(recBytes, subdirPath)
	if err != nil {
		return nil, fmt.Errorf("error reading second part of record: %s\n", err)
	}

	if !valid {
		return nil, fmt.Errorf("Value not valid!\n")
	}

	return record.SSTBytesToRecord(recBytes), nil

}

func (sst *SSTable) checkMerkleMultiple(bytes []byte, subdirPath string) (bool, error) {
	path := subdirPath + MERKLENAME

	mtBytes, err := os.ReadFile(path)
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
