package sstable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"key-value-engine/structs/bloomFilter"
	"key-value-engine/structs/merkleTree"
	"key-value-engine/structs/record"
	"os"
)

func (sst *SSTable) makeSingleFileComp(data []*record.Record, dirPath string) error {
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
	merkleData := make([][]byte, len(data))

	dataPos, err := file.Seek(5*OFFSETSIZE, 0)
	if err != nil {
		return err
	}

	// Writting data
	globalDict := make(map[string]int)
	dictIndex := 1
	for i, rec := range data {
		_, exists := globalDict[rec.GetKey()]
		if !exists {
			globalDict[rec.GetKey()] = dictIndex
			dictIndex++
		}

		recSer := rec.SSTRecordToBytes(dictIndex)
		recSerSizeBytes := make([]byte, binary.MaxVarintLen64)
		recSizeLen := binary.PutUvarint(recSerSizeBytes, uint64(len(recSer)))

		_, err := file.Write(recSerSizeBytes[:recSizeLen])
		if err != nil {
			return fmt.Errorf("error writing record: %s\n", err)
		}
		_, err = file.Write(recSer)
		if err != nil {
			return fmt.Errorf("error writing record: %s\n", err)
		}

		// populating Filter
		bf.Add([]byte(rec.GetKey()))

		// filling merkleData
		merkleData[i] = recSer
	}

	// Write Index
	indexPos, err := file.Seek(0, 1)
	if err != nil {
		return err
	}

	offsetIndex := 0
	for _, rec := range data {
		result := sst.indexFormatToBytes(rec, offsetIndex)

		_, err := file.Write(result)
		if err != nil {
			return fmt.Errorf("error writing Index entry: %s\n", err)
		}

		// updating offset
		recSer := rec.SSTRecordToBytes(globalDict[rec.GetKey()])
		recSerSizeBytes := make([]byte, binary.MaxVarintLen64)
		recSizeLen := binary.PutUvarint(recSerSizeBytes, uint64(len(recSer)))
		offsetIndex += len(recSer) + recSizeLen
	}

	// Write Summary
	summaryPos, err := file.Seek(0, 1)
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
	_, err = file.Write(summaryHeader)
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

	filterPos, err := file.Seek(0, 1)
	if err != nil {
		return err
	}

	bfBytes := bf.BloomFilterToBytes()

	_, err = file.Write(bfBytes)
	if err != nil {
		return fmt.Errorf("error writing Index entry: %s\n", err)
	}

	merklePos, err := file.Seek(0, 1)
	if err != nil {
		return err
	}

	mt := merkleTree.MakeMerkleTree(merkleData)

	mtBytes, err := merkleTree.SerializeMerkleTree(mt)
	if err != nil {
		return err
	}

	_, err = file.Write(mtBytes)
	if err != nil {
		return fmt.Errorf("error writing Index entry: %s\n", err)
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}

	header := []uint64{uint64(dataPos), uint64(indexPos), uint64(summaryPos), uint64(filterPos), uint64(merklePos)}

	for _, pos := range header {
		offsetBytes := make([]byte, OFFSETSIZE)
		binary.LittleEndian.PutUint64(offsetBytes, pos)

		_, err = file.Write(offsetBytes)
		if err != nil {
			return err
		}
	}

	marshalled, err := json.MarshalIndent(globalDict, "", "  ")
	if err != nil {
		return fmt.Errorf("error converting hashmap to json: %s", err)

	}

	// Write the JSON data to the file
	err = os.WriteFile(dirPath+string(os.PathSeparator)+GLOBALDICTNAME, marshalled, 0644)
	if err != nil {
		return fmt.Errorf("error writing hashmap to file: %s", err)
	}

	return nil
}

func (sst *SSTable) checkSingleComp(key string, dirPath string) (*record.Record, error) {
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

	return sst.checkFilterSingleComp(key, file, header, dirPath)
}

func (sst *SSTable) checkFilterSingleComp(key string, file *os.File, header []uint64, dirPath string) (*record.Record, error) {
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
		return nil, err
	}

	// not found
	if !bf.IsPresent([]byte(key)) {
		return nil, nil
	} else {
		return sst.checkSummarySingleComp(key, file, header, dirPath)
	}

}

func (sst *SSTable) checkSummarySingleComp(key string, file *os.File, header []uint64, dirPath string) (*record.Record, error) {
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
			return sst.checkIndexSingleComp(key, lastOffset, file, header, dirPath)
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
			return sst.checkIndexSingleComp(key, lastOffset, file, header, dirPath)
		} else {
			// updating lastOffset
			lastOffset = offset
		}

	}
}

func (sst *SSTable) checkIndexSingleComp(key string, offset uint64, file *os.File, header []uint64, dirPath string) (*record.Record, error) {
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
			return sst.checkDataSingleComp(offset, file, header, dirPath)
		}

	}
	return nil, nil
}

func (sst *SSTable) checkDataSingleComp(offset uint64, file *os.File, header []uint64, dirPath string) (*record.Record, error) {
	// seeking to position
	_, err := file.Seek(int64(header[0])+int64(offset), 0)
	if err != nil {
		return nil, fmt.Errorf("error seekign in Data: %s\n", err)
	}

	var bufSize [binary.MaxVarintLen64]byte

	// Read the SSTEntry size from the file into the buffer
	_, err = file.Read(bufSize[:])
	if err != nil {
		return nil, err
	}

	// Decode the SSTEntry size from the buffer
	entrySize, bytesRead := binary.Uvarint(bufSize[:])
	if bytesRead <= 0 {
		return nil, fmt.Errorf("failed to read varUint from file")
	}

	_, err = file.Seek(-int64(binary.MaxVarintLen64-bytesRead), 1)
	if err != nil {
		return nil, err
	}

	entryBytes := make([]byte, entrySize)

	_, err = file.Read(entryBytes)
	if err != nil {
		return nil, fmt.Errorf("error reading filter: %s\n", err)
	}

	valid, err := sst.checkMerkleSingleComp(entryBytes, file, header)
	if err != nil {
		return nil, fmt.Errorf("error checking merkle: %s\n", err)
	}

	if !valid {
		return nil, fmt.Errorf("Value not valid!\n")
	}

	globalDictData, err := os.ReadFile(dirPath + string(os.PathSeparator) + GLOBALDICTNAME)
	if err != nil {
		return nil, err
	}
	globalDict := make(map[string]int)
	err = json.Unmarshal(globalDictData, &globalDict)
	if err != nil {
		return nil, err
	}

	rec, err := record.SSTBytesToRecord(entryBytes, &globalDict)
	if err != nil {
		return nil, err
	}

	return rec, nil
}

func (sst *SSTable) checkMerkleSingleComp(bytes []byte, file *os.File, header []uint64) (bool, error) {
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
