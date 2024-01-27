package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"key-value-engine/structs/bloomFilter"
	"key-value-engine/structs/record"
	"os"
)

const (
	DIRECTORY   = "data" + string(os.PathSeparator) + "sstable"
	SUBDIR      = DIRECTORY + string(os.PathSeparator)
	DATANAME    = "SST_Data.db"
	INDEXNAME   = "SST_Index.db"
	SUMMARYNAME = "SST_Summary.db"
	BLOOMNAME   = "SST_Filter.db"
	OFFSETSIZE  = 8
)

type SSTable struct {
	nextIndex     int
	summaryFactor int
}

/*
MakeSSTable creates a new SSTable instance with the specified summary factor.

Parameters:
  - summaryFactor: An integer indicating the desired summary factor for the SSTable Summary.

Returns:
  - *SSTable: Pointer to the newly created SSTable instance.
  - error: An error, if any, encountered during the process.
*/
func MakeSSTable(summaryFactor int) (*SSTable, error) {
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
		nextIndex:     count,
		summaryFactor: summaryFactor,
	}, nil
}

/*
Get retrieves a Record associated with the specified key from the SSTable.

Parameters:
  - key: A string representing the key to search for.

Returns:
  - *record.Record: Pointer to the found Record, or nil if the key is not found.
  - error: An error, if any, encountered during the process.
*/
func (sst *SSTable) Get(key string) (*record.Record, error) {
	subdirs, err := getSubdirs(DIRECTORY)
	if err != nil {
		return nil, fmt.Errorf("error getting SST directories: %s\n", err)
	}

	// looping backwards
	for i := len(subdirs) - 1; i >= 0; i-- {
		subdir := subdirs[i]
		subdirPath := DIRECTORY + string(os.PathSeparator) + subdir + string(os.PathSeparator)

		// checking bloom filter
		found, err := sst.checkFilter(key, subdirPath)
		if err != nil {
			return nil, fmt.Errorf("error finding key: %s\n", err)
		}

		// if found return, otherwise continue search in next SST
		if found != nil {
			return found, nil
		}

	}

	return nil, nil

}

/*
Flush writes the provided data to a new SSTable directory, including data, index, summary, and filter files.

Parameters:
  - data: A slice of Records to be flushed to the SSTable.

Returns:
  - error: An error, if any, encountered during the flushing process.
*/
func (sst *SSTable) Flush(data []*record.Record) error {
	// making directory for SSTable
	dirPath := SUBDIR + "SST_" + fmt.Sprintf("%d", sst.nextIndex)
	err := os.Mkdir(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error making SST direcory: %s\n", err)
	}
	sst.nextIndex++

	// making Data
	err = sst.makeData(data, dirPath)
	if err != nil {
		return fmt.Errorf("error making Data: %s\n", err)
	}

	// making Index
	err = sst.makeIndex(data, dirPath)
	if err != nil {
		return fmt.Errorf("error making Index: %s\n", err)
	}

	// making Summary
	err = sst.makeSummary(data, dirPath)
	if err != nil {
		return fmt.Errorf("error making Summary: %s\n", err)
	}

	// making Filter
	err = sst.makeFilter(data, dirPath)
	if err != nil {
		return fmt.Errorf("error making Filter: %s\n", err)
	}

	return nil
}

/*
checkFilter checks if the specified key is likely present in the SSTable using a Bloom filter and if yes searching
in next structures in hierarchy.

Parameters:
  - key: A string representing the key to check.
  - subdirPath: The path to the SSTable subdirectory containing the Bloom filter.

Returns:
  - *record.Record: Pointer to the Record if the key is present, or nil if not found.
  - error: An error, if any, encountered during the checking process.
*/
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

/*
checkSummary retrieves a Record from the SSTable by searching within the summary file, and next files in hierarchy.

Parameters:
  - key: A string representing the key to search for.
  - subdirPath: The path to the SSTable subdirectory containing the summary file.

Returns:
  - *record.Record: Pointer to the found Record, or nil if not found.
  - error: An error, if any, encountered during the checking process.
*/
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

/*
checkIndex retrieves a Record from the SSTable by searching within the index file and next file in chierarchy.

Parameters:
  - key: A string representing the key to search for.
  - offset: The offset in the index file where the search should begin.
  - subdirPath: The path to the SSTable subdirectory containing the index file.

Returns:
  - *record.Record: Pointer to the found Record, or nil if not found.
  - error: An error, if any, encountered during the checking process.
*/
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

/*
checkData retrieves a Record from the SSTable data file based on the provided offset.

Parameters:
  - offset: The offset in the data file where the Record is located.
  - subdirPath: The path to the SSTable subdirectory containing the data file.

Returns:
  - *record.Record: Pointer to the found Record.
  - error: An error, if any, encountered during the checking process.
*/
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

/*
makeData creates a data file in the specified directory and writes serialized Records to it.

Parameters:
  - data: A slice of *record.Record representing the Records to be written to the data file.
  - dirPath: The directory path where the data file should be created.

Returns:
  - error: An error, if any, encountered during the file creation or writing process.
*/
func (sst *SSTable) makeData(data []*record.Record, dirPath string) error {
	file, err := os.Create(dirPath + string(os.PathSeparator) + DATANAME)
	if err != nil {
		return err
	}

	defer file.Close()

	for _, rec := range data {
		_, err := file.Write(rec.SSTRecordToBytes())
		if err != nil {
			return fmt.Errorf("error writing record: %s\n", err)
		}
	}
	return nil
}

/*
makeIndex creates an index file in the specified directory and writes formatted index entries to it.

Parameters:
  - data: A slice of *record.Record representing the Records for which index entries will be created.
  - dirPath: The directory path where the index file should be created.

Returns:
  - error: An error, if any, encountered during the file creation or writing process.
*/
func (sst *SSTable) makeIndex(data []*record.Record, dirPath string) error {
	file, err := os.Create(dirPath + string(os.PathSeparator) + INDEXNAME)
	if err != nil {
		return fmt.Errorf("error creating Index: %s\n", err)
	}

	defer file.Close()

	offset := 0
	for _, rec := range data {
		result := sst.indexFormatToBytes(rec, offset)

		_, err := file.Write(result)
		if err != nil {
			return fmt.Errorf("error writing Index entry: %s\n", err)
		}

		// updating offset
		offset += len(rec.SSTRecordToBytes())
	}
	return nil
}

/*
makeSummary creates a summary file in the specified directory and writes formatted summary entries to it.

Parameters:
  - data: A slice of *record.Record representing the Records for which summary entries will be created.
  - dirPath: The directory path where the summary file should be created.

Returns:
  - error: An error, if any, encountered during the file creation or writing process.
*/
func (sst *SSTable) makeSummary(data []*record.Record, dirPath string) error {
	file, err := os.Create(dirPath + string(os.PathSeparator) + SUMMARYNAME)
	if err != nil {
		return fmt.Errorf("error creating Summary: %s\n", err)
	}

	defer file.Close()

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

	offset := 0
	for i, rec := range data {
		// looping by summaryFactor
		if i%sst.summaryFactor == 0 {
			result := sst.indexFormatToBytes(rec, offset)
			_, err := file.Write(result)
			if err != nil {
				return fmt.Errorf("error writing Index entry: %s\n", err)
			}
		}

		// updating offset
		offset += OFFSETSIZE + record.KEY_SIZE_SIZE + int(rec.GetKeySize())
	}

	return nil
}

/*
makeFilter creates a Bloom filter file in the specified directory and writes the serialized Bloom filter to it.

Parameters:
  - data: A slice of *record.Record representing the Records used to build the Bloom filter.
  - dirPath: The directory path where the Bloom filter file should be created.

Returns:
  - error: An error, if any, encountered during the file creation or writing process.
*/
func (sst *SSTable) makeFilter(data []*record.Record, dirPath string) error {
	file, err := os.Create(dirPath + string(os.PathSeparator) + BLOOMNAME)
	if err != nil {
		return fmt.Errorf("error creating Filter: %s\n", err)
	}

	defer file.Close()

	bf := bloomFilter.MakeBloomFilter(uint64(len(data)), 0.1)

	// populating bloom filter
	for _, rec := range data {
		bf.Add([]byte(rec.GetKey()))
	}

	// writting bloom filter
	_, err = file.Write(bf.BloomFilterToBytes())
	if err != nil {
		return fmt.Errorf("error writing bloom filter to Filter: %s\n", err)
	}

	return nil
}

/*
indexFormatToBytes formats a Record into a byte slice representing an index entry.

Parameters:
  - rec: A *record.Record instance to be formatted into an index entry.
  - offset: An integer representing the offset associated with the Record in the SSTable.

Returns:
  - []byte: A byte slice containing the formatted index entry.
*/
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

/*
getSubdirs returns a slice of subdirectory names within the specified directory.

Parameters:
  - directory: The path to the directory for which subdirectories are to be retrieved.

Returns:
  - []string: A slice containing the names of subdirectories.
  - error: An error, if any, encountered during the process.
*/
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
