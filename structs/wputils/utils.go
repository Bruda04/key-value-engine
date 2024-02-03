package wputils

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"key-value-engine/structs/memtable"
	"key-value-engine/structs/record"
	"key-value-engine/structs/wal"
	"log"
	"os"
	"strconv"
	"strings"
)

func AddRecord(manager *memtable.MemManager, walInstance *wal.WAL, restoreEndOffset int64, rec *record.Record) (int64, error) {
	filePath := "data" + string(os.PathSeparator) + "memwal.csv"
	var file *os.File
	var err error
	file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)

	var addOffset int64
	addOffset = restoreEndOffset
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, err
	}

	if fileInfo.Size() == 0 {
		filename := walInstance.SegmentFiles[len(walInstance.SegmentFiles)-1]
		offset := strconv.FormatInt(addOffset, 10)
		seekEnd, err := file.Seek(0, 2)
		fmt.Println(seekEnd)
		if err != nil {
			return 0, err
		}
		_, err = file.WriteString(filename + "," + offset + "\n")
		if err != nil {
			return 0, err
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
	isSwitch, isFlush := manager.PutMem(rec)
	if isSwitch {
		filename := walInstance.SegmentFiles[len(walInstance.SegmentFiles)-1]
		offset := strconv.FormatInt(addOffset, 10)
		seekEnd, err := file.Seek(0, 2)
		fmt.Println(seekEnd)
		if err != nil {
			return 0, err
		}
		_, err = file.WriteString(filename + "," + offset + "\n")
		if err != nil {
			return 0, err
		}
		if isFlush {
			file.Seek(0, 0)

			scanner := bufio.NewScanner(file)
			if scanner.Scan() && scanner.Scan() {
				line := scanner.Text()
				parts := strings.Split(line, ",")
				if len(parts) != 2 {
					return 0, fmt.Errorf("invalid line format in CSV file: %s", line)
				}
				lowWaterMarkFile := parts[0]
				lowWaterMark, err := extractFileIndex(lowWaterMarkFile)

				// Delete WAL files up to lowWaterMarkFile
				walInstance.DeleteLWM(uint64(lowWaterMark - 1))
				if err != nil {
					return 0, err
				}
				// Update CSV file by deleting the first line and subtracting lowWaterMark from the index
				if err := updateCSVFile(filePath, lowWaterMark-1); err != nil {
					return 0, err
				}

			} else {
				return 0, fmt.Errorf("CSV file is empty")
			}
		}
	}
	return addOffset, nil
}

func Restore(manager *memtable.MemManager, walInstance *wal.WAL) (int64, error) {
	var retOffset int64
	file, err := os.Open("data" + string(os.PathSeparator) + "memwal.csv")
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var lines [][]string

	reader := csv.NewReader(file)

	lines, err = reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	if len(lines) == 0 {
		return 8, nil
	}
	var data [][]string
	for _, line := range lines {
		if len(line) < 2 {
			log.Printf("Invalid record: %v\n", line)
			continue
		}
		dataLine := []string{line[0], line[1]}

		// Append the record to the data slice
		data = append(data, dataLine)
	}

	offset, err := strconv.ParseInt(data[0][1], 10, 64)
	if err != nil {
		return 0, err
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
		manager.PutMem(rec)
	}

	return retOffset, nil
}

func extractFileIndex(fileName string) (int, error) {
	parts := strings.Split(fileName, "_")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid file name format: %s", fileName)
	}
	indexParts := strings.Split(parts[1], ".")
	index, err := strconv.Atoi(indexParts[0])
	if err != nil {
		return 0, err
	}

	return index, nil
}

func updateCSVFile(filePath string, lowWaterMark int) error {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	lines, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return err
	}

	updatedLines := make([][]string, len(lines)-1)
	for i, line := range lines[1:] {
		if len(line) < 2 {
			return fmt.Errorf("Invalid record: %v", line)
		}
		updatedIndex, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(line[0], "data"+string(os.PathSeparator)+"wal"+string(os.PathSeparator)+"wal_"), ".log"))
		if err != nil {
			return err
		}
		updatedIndex -= lowWaterMark
		line[0] = fmt.Sprintf("data"+string(os.PathSeparator)+"wal"+string(os.PathSeparator)+"wal_%d.log", updatedIndex)
		updatedLines[i] = line
	}

	// Truncate the file and write updated lines
	file.Truncate(0)
	file.Seek(0, 0)

	writer := csv.NewWriter(file)
	err = writer.WriteAll(updatedLines)
	if err != nil {
		return err
	}

	return nil
}
