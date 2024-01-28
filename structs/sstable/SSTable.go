package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"key-value-engine/structs/bloomFilter"
	"key-value-engine/structs/record"
	"os"
	"strings"
)

const (
	DIRECTORY      = "data" + string(os.PathSeparator) + "sstable"
	SUBDIR         = DIRECTORY + string(os.PathSeparator)
	DATANAME       = "SST_Data.db"
	INDEXNAME      = "SST_Index.db"
	SUMMARYNAME    = "SST_Summary.db"
	BLOOMNAME      = "SST_Filter.db"
	TOCNAME        = "TOC.csv"
	MERKLENAME     = "SST_Merkle.db"
	SINGLEFILENAME = "SST.db"
	OFFSETSIZE     = 8
)

type SSTable struct {
	nextIndex         int
	summaryFactor     int
	multipleFiles     bool
	filterProbability float64
}

func MakeSSTable(summaryFactor int, multipleFiles bool, filterProbability float64) (*SSTable, error) {
	if _, err := os.Stat(DIRECTORY); os.IsNotExist(err) {
		if err := os.MkdirAll(DIRECTORY, 0755); err != nil {
			return nil, fmt.Errorf("error creating sstable directory: %s", err)
		}
	}

	subdirs, err := getSubdirs(DIRECTORY)
	if err != nil {
		return nil, fmt.Errorf("error getting SST directories: %s\n", err)
	}

	count := len(subdirs) + 1

	return &SSTable{
		nextIndex:         count,
		summaryFactor:     summaryFactor,
		multipleFiles:     multipleFiles,
		filterProbability: filterProbability,
	}, nil
}

func (sst *SSTable) Get(key string) (*record.Record, error) {
	subdirs, err := getSubdirs(DIRECTORY)
	if err != nil {
		return nil, fmt.Errorf("error getting SST directories: %s\n", err)
	}

	// looping backwards
	for i := len(subdirs) - 1; i >= 0; i-- {
		subdir := subdirs[i]
		subdirPath := DIRECTORY + string(os.PathSeparator) + subdir + string(os.PathSeparator)

		files, _ := readTOC(DIRECTORY + string(os.PathSeparator) + subdir)

		if len(files) > 1 {
			// checking bloom filter
			found, err := sst.checkMultiple(key, subdirPath)
			if err != nil {
				return nil, fmt.Errorf("error finding key: %s\n", err)
			}

			// if found return, otherwise continue search in next SST
			if found != nil {
				return found, nil
			}

		} else {
			// checking bloom filter
			found, err := sst.checkSingle(key, subdirPath)
			if err != nil {
				return nil, fmt.Errorf("error finding key: %s\n", err)
			}

			// if found return, otherwise continue search in next SST
			if found != nil {
				return found, nil
			}
		}

	}

	return nil, nil

}

func (sst *SSTable) Flush(data []*record.Record) error {
	// making directory for SSTable
	dirPath := SUBDIR + "SST_" + fmt.Sprintf("%d", sst.nextIndex)
	err := os.Mkdir(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error making SST direcory: %s\n", err)
	}
	sst.nextIndex++

	if sst.multipleFiles {
		// make Multiple files
		err = sst.makeMultipleFiles(data, dirPath)
	} else {
		// make Single file
		err = sst.makeSingleFile(data, dirPath)
	}

	return nil
}

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
	for i, rec := range data {
		// Making Data
		_, err := fileData.Write(rec.SSTRecordToBytes())
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
		offsetIndex += len(rec.SSTRecordToBytes())

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
	}

	// writting bloom filter
	_, err = fileFilter.Write(bf.BloomFilterToBytes())
	if err != nil {
		return fmt.Errorf("error writing bloom filter to Filter: %s\n", err)
	}

	return nil
}

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
	for i, rec := range data {
		dataSize += len(rec.SSTRecordToBytes())

		indexSize += OFFSETSIZE + int(rec.GetKeySize()) + record.KEY_SIZE_SIZE

		if i%sst.summaryFactor == 0 {
			summarySize += OFFSETSIZE + rec.GetKeySize() + record.KEY_SIZE_SIZE
		}

		bf.Add([]byte(rec.GetKey()))
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

	return nil

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
	if headerBytes[record.TOMBSTONE_START] == 1 {
		// reading key size
		keySize := binary.LittleEndian.Uint64(headerBytes[record.KEY_SIZE_START:record.VALUE_SIZE_START])
		secondPartBytes := make([]byte, keySize)
		_, err := file.Read(secondPartBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading secong part of record: %s\n", err)
		}

		recBytes := append(headerBytes, secondPartBytes...)

		return record.SSTBytesToRecord(recBytes), nil
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

		recBytes := append(headerBytes, valSizeBytes...)
		recBytes = append(recBytes, secondPartBytes...)

		return record.SSTBytesToRecord(recBytes), nil
	}

}

func (sst *SSTable) indexFormatToBytes(rec *record.Record, offset int) []byte {
	// serializing key size
	keySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(keySizeBytes, rec.GetKeySize())

	// serializing key
	keyBytes := []byte(rec.GetKey())

	// serilizing offset
	offsetBytes := make([]byte, OFFSETSIZE)
	binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))

	result := append(keySizeBytes, keyBytes...)
	result = append(result, offsetBytes...)

	return result
}

func (sst *SSTable) checkMultiple(key string, dirPath string) (*record.Record, error) {
	return sst.checkFilter(key, dirPath)
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

func (sst *SSTable) makeTOC(dirPath string, multipleFiles bool) error {
	file, err := os.Create(dirPath + string(os.PathSeparator) + TOCNAME)
	if err != nil {
		return err
	}
	defer file.Close()

	csvData := ""
	if multipleFiles {
		csvData = fmt.Sprintf("%s,%s,%s,%s,%s", DATANAME, INDEXNAME, SUMMARYNAME, BLOOMNAME, MERKLENAME)

	} else {
		csvData = fmt.Sprintf("%s", SINGLEFILENAME)
	}

	_, err = file.WriteString(csvData)
	if err != nil {
		return err
	}
	return nil
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
	if headerBytes[record.TOMBSTONE_START] == 1 {
		// reading key size
		keySize := binary.LittleEndian.Uint64(headerBytes[record.KEY_SIZE_START:record.VALUE_SIZE_START])
		secondPartBytes := make([]byte, keySize)
		_, err := file.Read(secondPartBytes)
		if err != nil {
			return nil, fmt.Errorf("error reading secong part of record: %s\n", err)
		}

		recBytes := append(headerBytes, secondPartBytes...)

		return record.SSTBytesToRecord(recBytes), nil
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

		recBytes := append(headerBytes, valSizeBytes...)
		recBytes = append(recBytes, secondPartBytes...)

		return record.SSTBytesToRecord(recBytes), nil
	}
}

func readTOC(dirPath string) ([]string, error) {
	content, err := os.ReadFile(dirPath + string(os.PathSeparator) + TOCNAME)
	if err != nil {
		return nil, err
	}

	line := string(content)

	return strings.Split(line, ","), nil
}

func getSubdirs(directory string) ([]string, error) {
	// opening direcotry
	dir, err := os.Open(directory)
	if err != nil {
		return nil, fmt.Errorf("error opening sstable direcotry: %s\n", err)
	}
	defer dir.Close()

	// reading content of direcotry
	entries, err := dir.Readdir(0)
	if err != nil {
		return nil, fmt.Errorf("error reading directories: %s\n", err)
	}

	var subdirs []string

	// adding subdirecories
	for _, entry := range entries {
		if entry.IsDir() {
			subdirs = append(subdirs, entry.Name())
		}
	}

	return subdirs, nil
}
