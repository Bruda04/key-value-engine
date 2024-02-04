package wputils

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"key-value-engine/structs/memtable"
	"key-value-engine/structs/record"
	"key-value-engine/structs/wal"
	"os"
	"strconv"
	"strings"
)

func AddRecord(manager *memtable.MemManager, walInstance *wal.WAL, restoreEndOffset int64, rec *record.Record) (int64, error) {
	filePath := "data" + string(os.PathSeparator) + "memwal.csv"
	var file *os.File
	var err error
	file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, errors.New("error reading wal file")
	}

	var addOffset int64
	addOffset = restoreEndOffset
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, errors.New("error reading wal file")
	}

	if fileInfo.Size() == 0 {
		filename := walInstance.SegmentFiles[len(walInstance.SegmentFiles)-1]
		offset := strconv.FormatInt(addOffset, 10)
		_, err := file.Seek(0, 2)
		if err != nil {
			return 0, errors.New("error reading wal file")
		}
		_, err = file.WriteString(filename + "," + offset + "\n")
		if err != nil {
			return 0, errors.New("error writting wal file")
		}
	}

	recToBytes := rec.RecordToBytes()

	err = walInstance.AddRecord(rec)
	if err != nil {
		return 0, err
	}

	addOffset += int64(len(recToBytes))
	if addOffset > walInstance.SegmentSize {
		addOffset %= walInstance.SegmentSize
		addOffset += 8
	}
	isSwitch, isFlush, err := manager.PutMem(rec)
	if err != nil {
		return 0, err
	}
	if isSwitch {
		filename := walInstance.SegmentFiles[len(walInstance.SegmentFiles)-1]
		offset := strconv.FormatInt(addOffset, 10)
		_, err := file.Seek(0, 2)
		if err != nil {
			return 0, errors.New("error reading wal file")
		}
		_, err = file.WriteString(filename + "," + offset + "\n")
		if err != nil {
			return 0, errors.New("error reading wal file")
		}
		if isFlush {
			_, err := file.Seek(0, 0)
			if err != nil {
				return 0, errors.New("error reading wal file")
			}

			scanner := bufio.NewScanner(file)
			if scanner.Scan() && scanner.Scan() {
				line := scanner.Text()
				parts := strings.Split(line, ",")
				if len(parts) != 2 {
					return 0, errors.New("invalid line format in CSV file")
				}
				lowWaterMarkFile := parts[0]
				lowWaterMark, err := extractFileIndex(lowWaterMarkFile)
				if err != nil {
					return 0, err
				}

				// Delete WAL files up to lowWaterMarkFile
				err = walInstance.DeleteLWM(uint64(lowWaterMark - 1))
				if err != nil {
					return 0, err
				}

				// Update CSV file by deleting the first line and subtracting lowWaterMark from the index
				if err := updateCSVFile(filePath, lowWaterMark-1); err != nil {
					return 0, err
				}

			} else {
				return 0, errors.New("error csv file is empty")
			}
		}
	}
	return addOffset, nil
}

func Restore(manager *memtable.MemManager, walInstance *wal.WAL) (int64, error) {
	var retOffset int64
	file, err := os.OpenFile("data"+string(os.PathSeparator)+"memwal.csv", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var lines [][]string

	reader := csv.NewReader(file)

	lines, err = reader.ReadAll()
	if err != nil {
		return 0, errors.New("error reading csv file")
	}

	if len(lines) == 0 {
		return 8, nil
	}
	var data [][]string
	for _, line := range lines {
		if len(line) < 2 {
			return 0, errors.New("error reading csv file")
		}
		dataLine := []string{line[0], line[1]}

		// Append the record to the data slice
		data = append(data, dataLine)
	}

	offset, err := strconv.ParseInt(data[0][1], 10, 64)
	if err != nil {
		return 0, errors.New("error reading csv file")
	}
	// Iterate through the records and restore

	currentOffset := offset

	for {
		rec, newOffset, err := walInstance.RestoreRecord(currentOffset)
		if rec == nil && err == nil {
			retOffset = currentOffset
			break
		}
		if err != nil {
			return 0, err
		}
		currentOffset = newOffset

		_, _, err = manager.PutMem(rec)
		if err != nil {
			return 0, err
		}
	}

	return retOffset, nil
}

func extractFileIndex(fileName string) (int, error) {
	parts := strings.Split(fileName, "_")
	if len(parts) != 2 {
		return 0, errors.New("invalid file name format")
	}
	indexParts := strings.Split(parts[1], ".")
	index, err := strconv.Atoi(indexParts[0])
	if err != nil {
		return 0, errors.New("error reading csv file")
	}

	return index, nil
}

func updateCSVFile(filePath string, lowWaterMark int) error {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return errors.New("error reading csv file")
	}
	defer file.Close()

	lines, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return errors.New("error reading csv file")
	}

	updatedLines := make([][]string, len(lines)-1)
	for i, line := range lines[1:] {
		if len(line) < 2 {
			return errors.New("error reading csv file")
		}
		updatedIndex, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(line[0], "data"+string(os.PathSeparator)+"wal"+string(os.PathSeparator)+"wal_"), ".log"))
		if err != nil {
			return errors.New("error reading csv file")
		}
		updatedIndex -= lowWaterMark
		line[0] = fmt.Sprintf("data"+string(os.PathSeparator)+"wal"+string(os.PathSeparator)+"wal_%d.log", updatedIndex)
		updatedLines[i] = line
	}

	// Truncate the file and write updated lines
	err = file.Truncate(0)
	if err != nil {
		return errors.New("error formating csv file")
	}
	_, err = file.Seek(0, 0)
	if err != nil {
		return errors.New("error reading csv file")
	}

	writer := csv.NewWriter(file)
	err = writer.WriteAll(updatedLines)
	if err != nil {
		return errors.New("error writting csv file")
	}

	return nil
}
