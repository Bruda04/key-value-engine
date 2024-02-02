package sstable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"key-value-engine/structs/record"
	"os"
)

type TableFile struct {
	file          *os.File
	isMultiFile   bool
	currentOffset int
	lastOffset    int
	dirPath       string
}

func (sst *SSTable) Compress() error {
	if sst.compressionTypeLSM == "size-tiered" {
		err := sst.compressSizeTier()
		if err != nil {
			return err
		}
	} else {
		sst.compressLeveled()
	}
	return nil
}
func (sst *SSTable) compressSizeTier() error {
	dirnamesByTier, _ := sst.getDirsByTier()
	for i, tier := range dirnamesByTier {
		if len(tier) >= sst.tablesToCompress {
			compressionLevel := i + 1
			compressionTables := tier[:sst.tablesToCompress]

			err := sst.extractDataSizeTier(compressionTables, compressionLevel)
			if err != nil {
				return err
			}
			for _, dirname := range compressionTables {
				err = os.RemoveAll(dirname)
			}
			return sst.compressSizeTier()
		}

	}
	return nil
}

func (sst *SSTable) extractDataSizeTier(tablesPaths []string, level int) error {
	var dataFiles []*TableFile
	dirPath := SUBDIR + fmt.Sprintf("C%d_SST_%d", level+1, sst.nextIndex)
	err := os.Mkdir(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error making SST direcory: %s\n", err)
	}
	sst.nextIndex++

	if sst.compression {
		globalDict := make(map[string]int)
		marshalled, err := json.MarshalIndent(globalDict, "", "  ")
		if err != nil {
			return err

		}

		err = os.WriteFile(dirPath+string(os.PathSeparator)+GLOBALDICTNAME, marshalled, 0644)
		if err != nil {
			return err
		}
	}

	for _, tablePath := range tablesPaths {
		files, _ := readTOC(tablePath)
		if len(files) > 1 {
			file, err := os.Open(tablePath + DATANAME)
			seek, _ := file.Seek(0, 2)
			tableFile := makeTableFile(file, true, 0, int(seek), tablePath)
			if err != nil {
				return fmt.Errorf("error reading data: %s\n", err)
			}
			defer file.Close()
			dataFiles = append(dataFiles, tableFile)
		} else {
			file, err := os.Open(tablePath + SINGLEFILENAME)
			headerBytes := make([]byte, 2*OFFSETSIZE)
			_, err = file.Read(headerBytes)
			if err != nil {
				return fmt.Errorf("error reading header: %s\n", err)
			}
			endOfData := binary.LittleEndian.Uint64(headerBytes[OFFSETSIZE : 2*OFFSETSIZE])
			tableFile := makeTableFile(file, false, 5*OFFSETSIZE, int(endOfData), tablePath)

			if err != nil {
				return fmt.Errorf("error reading data: %s\n", err)
			}
			defer file.Close()
			dataFiles = append(dataFiles, tableFile)
		}
	}
	err = sst.makeTOC(dirPath, sst.multipleFiles)
	if err != nil {
		return err
	}

	var i = 0
	var comparableRecords map[*TableFile]*record.Record
	comparableRecords = make(map[*TableFile]*record.Record)
	for _, f := range dataFiles {
		comparableRecords[f] = nil
	}
	for {
		if i == 0 {
			for _, file := range dataFiles {
				comparableRecords[file], _ = sst.readRecordFromFile(file)
			}

		}

		if len(comparableRecords) == 0 {
			break
		}

		var minimalRecord *record.Record
		var minimalFile *TableFile

		for file, rec := range comparableRecords {
			if minimalRecord == nil {
				minimalRecord = rec
				minimalFile = file
			} else if rec.GetKey() < minimalRecord.GetKey() {
				minimalRecord = rec
				minimalFile = file
			} else if rec.GetKey() == minimalRecord.GetKey() {
				if rec.GetTimestamp() > minimalRecord.GetTimestamp() {
					comparableRecords[minimalFile], _ = sst.readRecordFromFile(minimalFile)
					if comparableRecords[minimalFile] == nil {
						delete(comparableRecords, minimalFile)
					}

					minimalRecord = rec
					minimalFile = file
				} else {
					comparableRecords[file], _ = sst.readRecordFromFile(file)
					if comparableRecords[file] == nil {
						delete(comparableRecords, file)
					}
				}
			}
		}

		i += 1
		comparableRecords[minimalFile], _ = sst.readRecordFromFile(minimalFile)
		if comparableRecords[minimalFile] == nil {
			delete(comparableRecords, minimalFile)
		}
		err = sst.putData(minimalRecord, dirPath)
		if err != nil {
			return err
		}
	}

	err = sst.formIndex(dirPath)
	if err != nil {
		return err
	}
	err = sst.formSummary(dirPath)
	if err != nil {
		return err
	}
	err = sst.formBfMt(dirPath, i)
	if err != nil {
		return err
	}

	return nil
}

func (sst *SSTable) readRecordFromFile(table *TableFile) (*record.Record, error) {
	if table.currentOffset >= table.lastOffset {
		return nil, nil
	}
	_, err := table.file.Seek(int64(table.currentOffset), 0)
	if err != nil {
		return nil, err
	}
	var ret *record.Record
	if sst.compression {

		var bufSize [binary.MaxVarintLen64]byte

		// Read the SSTEntry size from the file into the buffer
		_, err = table.file.Read(bufSize[:])
		if err != nil {
			return nil, err
		}

		// Decode the SSTEntry size from the buffer
		entrySize, bytesRead := binary.Uvarint(bufSize[:])

		_, err = table.file.Seek(-int64(binary.MaxVarintLen64-bytesRead), 1)
		if err != nil {
			return nil, err
		}

		entryBytes := make([]byte, entrySize)

		_, err = table.file.Read(entryBytes)
		if err != nil {
			return nil, err
		}

		globalDictData, err := os.ReadFile(table.dirPath + string(os.PathSeparator) + GLOBALDICTNAME)
		if err != nil {
			return nil, err
		}
		globalDict := make(map[string]int)
		err = json.Unmarshal(globalDictData, &globalDict)
		if err != nil {
			return nil, err
		}

		ret, err = record.SSTBytesToRecord(entryBytes, &globalDict)
		if err != nil {
			return nil, err
		}
		table.currentOffset += len(entryBytes) + bytesRead

	} else {
		headerBytes := make([]byte, record.RECORD_HEADER_SIZE)
		_, err = table.file.Read(headerBytes)
		if err != nil {
			return nil, err
		}

		keySize := binary.LittleEndian.Uint64(headerBytes[record.KEY_SIZE_START:record.VALUE_SIZE_START])
		valSize := binary.LittleEndian.Uint64(headerBytes[record.VALUE_SIZE_START:record.KEY_START])

		secondPartBytes := make([]byte, keySize+valSize)
		_, err = table.file.Read(secondPartBytes)
		if err != nil {
			return nil, err
		}
		recBytes := append(headerBytes, secondPartBytes...)
		table.currentOffset += len(recBytes)

		ret = record.BytesToRecord(recBytes)

	}

	return ret, nil
}

func makeTableFile(file *os.File, isMultifile bool, currentOffset int, lastOffset int, dirPath string) *TableFile {
	return &TableFile{
		file:          file,
		isMultiFile:   isMultifile,
		currentOffset: currentOffset,
		lastOffset:    lastOffset,
		dirPath:       dirPath,
	}
}

func (sst *SSTable) compressLeveled() {
	// to be done
}
