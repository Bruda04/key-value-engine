package sstable

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"key-value-engine/structs/record"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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
		err := sst.compressLeveled()
		if err != nil {
			return err
		}
	}
	return nil
}
func (sst *SSTable) compressSizeTier() error {
	dirnamesByTier, _ := sst.getDirsByTier()
	re := regexp.MustCompile(`C(\d+)_`)
	for _, tier := range dirnamesByTier {
		match := re.FindStringSubmatch(tier[0])
		lvl, _ := strconv.Atoi(match[1])

		if len(tier) >= sst.tablesToCompress && lvl < sst.maxLSMLevels {
			compressionLevel := lvl + 1
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
	dirPath := SUBDIR + fmt.Sprintf("C%d_SST_%d", level, sst.nextIndex)
	err := os.Mkdir(dirPath, os.ModePerm)
	if err != nil {
		return errors.New("error making SST direcory")
	}
	sst.nextIndex++

	if sst.compression {
		globalDict := make(map[string]int)
		marshalled, err := json.MarshalIndent(globalDict, "", "  ")
		if err != nil {
			return errors.New("error converting json")

		}

		err = os.WriteFile(dirPath+string(os.PathSeparator)+GLOBALDICTNAME, marshalled, 0644)
		if err != nil {
			return errors.New("error writting json")
		}
	}

	for _, tablePath := range tablesPaths {
		files, _ := readTOC(tablePath)
		if len(files) > 1 {
			file, err := os.Open(tablePath + DATANAME)
			seek, _ := file.Seek(0, 2)
			tableFile := makeTableFile(file, true, 0, int(seek), tablePath)
			if err != nil {
				return errors.New("error making table file")
			}
			defer file.Close()
			dataFiles = append(dataFiles, tableFile)
		} else {
			file, err := os.Open(tablePath + SINGLEFILENAME)
			headerBytes := make([]byte, 2*OFFSETSIZE)
			_, err = file.Read(headerBytes)
			if err != nil {
				return errors.New("error reading header")
			}
			endOfData := binary.LittleEndian.Uint64(headerBytes[OFFSETSIZE : 2*OFFSETSIZE])
			tableFile := makeTableFile(file, false, 5*OFFSETSIZE, int(endOfData), tablePath)

			if err != nil {
				return errors.New("error reading data")
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
		return nil, errors.New("error reading file")
	}
	var ret *record.Record
	if sst.compression {

		var bufSize [binary.MaxVarintLen64]byte

		// Read the SSTEntry size from the file into the buffer
		_, err = table.file.Read(bufSize[:])
		if err != nil {
			return nil, errors.New("error reading file")
		}

		// Decode the SSTEntry size from the buffer
		entrySize, bytesRead := binary.Uvarint(bufSize[:])

		_, err = table.file.Seek(-int64(binary.MaxVarintLen64-bytesRead), 1)
		if err != nil {
			return nil, errors.New("error reading file")
		}

		entryBytes := make([]byte, entrySize)

		_, err = table.file.Read(entryBytes)
		if err != nil {
			return nil, errors.New("error reading file")
		}

		globalDictData, err := os.ReadFile(table.dirPath + string(os.PathSeparator) + GLOBALDICTNAME)
		if err != nil {
			return nil, errors.New("error reading file")
		}
		globalDict := make(map[string]int)
		err = json.Unmarshal(globalDictData, &globalDict)
		if err != nil {
			return nil, errors.New("error reading json")
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
			return nil, errors.New("error reading file")
		}

		keySize := binary.LittleEndian.Uint64(headerBytes[record.KEY_SIZE_START:record.VALUE_SIZE_START])
		valSize := binary.LittleEndian.Uint64(headerBytes[record.VALUE_SIZE_START:record.KEY_START])

		secondPartBytes := make([]byte, keySize+valSize)
		_, err = table.file.Read(secondPartBytes)
		if err != nil {
			return nil, errors.New("error reading file")
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

func (sst *SSTable) compressLeveled() error {
	dirnamesByTier, _ := sst.getDirsByTier()
	re := regexp.MustCompile(`C(\d+)_`)
	for id, tier := range dirnamesByTier {
		match := re.FindStringSubmatch(tier[0])
		lvl, _ := strconv.Atoi(match[1])

		if lvl == sst.maxLSMLevels {
			return nil
		}

		//calculating level size
		sizeOfTier := 0
		for _, filePath := range tier {
			sizeOfFile, err := getDirectorySize(filePath)
			if err == nil {
				sizeOfTier += int(sizeOfFile)
			}
		}

		for sizeOfTier > int(float64(sst.firstLeveledSize)*math.Pow(float64(sst.leveledInc), float64(lvl-1))) {
			freedMemory, _ := getDirectorySize(tier[0])
			sizeOfTier -= freedMemory
			if len(dirnamesByTier) == lvl { //this is the current final level
				err := sst.extractDataSizeTier([]string{tier[0]}, lvl+1)
				err = os.RemoveAll(tier[0])
				if err != nil {
					return err
				}
				return sst.compressLeveled()
			}

			lowK, highK, err := sst.findHighLowKey(tier[0])
			if err != nil {
				return err
			}
			compressionTables := []string{tier[0]}
			compressionLevel := lvl + 1

			for _, nextLvlFile := range dirnamesByTier[id+1] {
				low, high, err := sst.findHighLowKey(nextLvlFile)
				if err != nil {
					return err
				}
				if !(lowK > high || highK < low) {
					compressionTables = append(compressionTables, nextLvlFile)
				}
			}

			err = sst.extractDataSizeTier(compressionTables, compressionLevel)
			if err != nil {
				return err
			}
			for _, dirname := range compressionTables {
				err = os.RemoveAll(dirname)
			}
			return sst.compressLeveled()
		}

	}
	return nil
}

func getDirectorySize(dirPath string) (int, error) {
	var size int64

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})

	return int(size), err
}

func (sst *SSTable) findHighLowKey(dirPath string) (string, string, error) {
	var err error
	var file *os.File
	var header []uint64
	files, err := readTOC(dirPath)
	if err != nil {
		return "", "", err
	}

	if len(files) > 1 {
		path := dirPath + SUMMARYNAME
		file, err = os.Open(path)
		if err != nil {
			return "", "", errors.New("error reading sst file")
		}
		defer file.Close()

		_, err = file.Seek(0, 2)
		if err != nil {
			return "", "", errors.New("error reading sst file")
		}

		_, err = file.Seek(0, 0)
		if err != nil {
			return "", "", errors.New("error reading sst file")
		}

	} else {
		path := dirPath + SINGLEFILENAME
		file, err = os.Open(path)
		if err != nil {
			return "", "", errors.New("error reading sst file")
		}
		defer file.Close()

		headerBytes := make([]byte, 5*OFFSETSIZE)
		_, err = file.Read(headerBytes)
		if err != nil {
			return "", "", errors.New("error reading sst file")
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
			return "", "", errors.New("error reading sst file")
		}

	}

	// reading low-key size
	keySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return "", "", errors.New("error reading sst file")
	}

	keySize := binary.LittleEndian.Uint64(keySizeBytes)

	// reading low-key
	lowKey := make([]byte, keySize)
	_, err = file.Read(lowKey)
	if err != nil {
		return "", "", errors.New("error reading sst file")
	}

	// readinf high-key size
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return "", "", errors.New("error reading sst file")
	}
	keySize = binary.LittleEndian.Uint64(keySizeBytes)

	// reading high-key
	highKey := make([]byte, keySize)
	_, err = file.Read(highKey)
	if err != nil {
		return "", "", errors.New("error reading sst file")
	}

	return string(lowKey), string(highKey), nil
}
