package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"key-value-engine/structs/record"
	"log"
	"os"
)

const (
	DIRECTORY = "data" + string(os.PathSeparator) + "wal"
	FILEPATH  = DIRECTORY + string(os.PathSeparator) + "wal"
	EXT       = ".log"
)

/*
Structure:
- SegmentSize: Size of each WAL segment file.
- SegmentFiles: List of filenames representing WAL segment files.
- RepairFileIndex: Index of the current WAL segment file being repaired.
- RepairOffset: Offset within the current WAL segment file during repair operations.
*/

type WAL struct {
	SegmentSize     int64
	SegmentFiles    []string
	RepairFileIndex int64
	RepairOffset    int64
}

/*
MakeWAL initializes and returns a new WAL instance.

Parameters:
- segmentSize: Size of each WAL segment file.

Returns:
- *WAL: Pointer to the created WAL instance.
- error: Error, if any, during the initialization process.
*/
func MakeWAL(segmentSize int64) (*WAL, error) {
	if err := os.MkdirAll(DIRECTORY, 0755); err != nil {
		return nil, errors.New("error creating wal data directory")
	}

	initialSegmentFile := FILEPATH + "_1" + EXT

	var filenames []string

	if _, err := os.Stat(initialSegmentFile); err == nil {
		dir, _ := os.Open(DIRECTORY)
		files, _ := dir.Readdir(0)
		for _, file := range files {
			filenames = append(filenames, DIRECTORY+string(os.PathSeparator)+file.Name())
		}
	} else {
		filenames = append(filenames, initialSegmentFile)
		if err := createInitialSegmentFile(initialSegmentFile); err != nil {
			return nil, errors.New("error creating initial segment file")
		}
	}

	wal := &WAL{
		SegmentSize:     segmentSize,
		SegmentFiles:    filenames,
		RepairFileIndex: 0,
		RepairOffset:    8,
	}

	return wal, nil
}

/*
createInitialSegmentFile creates the initial WAL segment file with an overflow record part.

Parameters:
- filename: Name of the segment file.

Returns:
- error: Error, if any, during file creation.
*/
func createInitialSegmentFile(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)

	overflowBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(overflowBytes, 0)
	_, err = f.Write(overflowBytes)

	return err
}

/*
AddRecord appends a new record to the WAL, handling record overflow by creating new segments.

Parameters:
- key: Key for the new record.
- value: Value associated with the key.
- deleted: Flag indicating if the record is marked as deleted.

Returns:
- error: Error, if any, during the record addition process.
*/
func (wal *WAL) AddRecord(rec *record.Record) error {
	recordBytes := rec.RecordToBytes()

	filePath := wal.SegmentFiles[len(wal.SegmentFiles)-1]

	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return errors.New("error opening segment file for writing")
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)

	mmappedData, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return errors.New("error mmaping file")
	}
	defer func(mmappedData *mmap.MMap) {
		err := mmappedData.Unmap()
		if err != nil {

		}
	}(&mmappedData)

	fileSize := int64(len(mmappedData))

	// In case of record overflowing
	if fileSize+int64(len(recordBytes)) > wal.SegmentSize {

		bytesToFit := wal.SegmentSize - fileSize
		firstRecordPart := recordBytes[:bytesToFit]
		secondRecordPart := recordBytes[bytesToFit:]

		err := f.Truncate(fileSize + int64(len(firstRecordPart)))
		if err != nil {
			return err
		}
		mmapedDataExpanded, err := mmap.Map(f, mmap.RDWR, 0)
		if err != nil {
			log.Fatal(err)
		}
		defer func(mmapedDataExpanded *mmap.MMap) {
			err := mmapedDataExpanded.Unmap()
			if err != nil {

			}
		}(&mmapedDataExpanded)
		copy(mmapedDataExpanded[fileSize:], firstRecordPart)

		err = wal.makeSegment()
		if err != nil {
			return err
		}

		secondFile, err := os.OpenFile(wal.SegmentFiles[len(wal.SegmentFiles)-1], os.O_RDWR, 0644)
		if err != nil {
			return errors.New("error opening new segment file for writing")
		}
		defer func(secondFile *os.File) {
			err := secondFile.Close()
			if err != nil {

			}
		}(secondFile)

		mmapedDataSecondFile, err := mmap.Map(secondFile, mmap.RDWR, 0)
		if err != nil {
			return errors.New("error mmaping new file")
		}
		defer func(mmapedDataSecondFile *mmap.MMap) {
			err := mmapedDataSecondFile.Unmap()
			if err != nil {

			}
		}(&mmapedDataSecondFile)

		secondRecordPartSize := uint64(len(secondRecordPart))
		secondPartSizeBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(secondPartSizeBytes, secondRecordPartSize)

		err = secondFile.Truncate(8 + int64(secondRecordPartSize))
		if err != nil {
			return err
		}
		mmappedDataSecondFileExplanded, err := mmap.Map(secondFile, mmap.RDWR, 0)
		if err != nil {
			log.Fatal(err)
		}
		defer func(mmappedDataSecondFileExplanded *mmap.MMap) {
			err := mmappedDataSecondFileExplanded.Unmap()
			if err != nil {

			}
		}(&mmappedDataSecondFileExplanded)
		copy(mmappedDataSecondFileExplanded[:8], secondPartSizeBytes)
		copy(mmappedDataSecondFileExplanded[8:], secondRecordPart)

	} else {
		err := f.Truncate(fileSize + int64(len(recordBytes)))
		if err != nil {
			return err
		}
		mmapedDataExpanded, err := mmap.Map(f, mmap.RDWR, 0)
		if err != nil {
			log.Fatal(err)
		}
		defer func(mmapedDataExpanded *mmap.MMap) {
			err := mmapedDataExpanded.Unmap()
			if err != nil {

			}
		}(&mmapedDataExpanded)
		copy(mmapedDataExpanded[fileSize:], recordBytes)
	}

	return nil
}

/*
makeSegment creates a new WAL segment file.

Returns:
- error: Error, if any, during segment creation.
*/
func (wal *WAL) makeSegment() error {
	newSegmentFile := FILEPATH + fmt.Sprintf("_%d.log", len(wal.SegmentFiles)+1)
	wal.SegmentFiles = append(wal.SegmentFiles, newSegmentFile)

	// Create the new segment file
	file, err := os.Create(newSegmentFile)
	if err != nil {
		return errors.New("error creating new segment file")
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			return
		}
	}(file)

	leftover := make([]byte, 8)
	binary.LittleEndian.PutUint64(leftover, 0)
	_, err = file.Write(leftover)
	if err != nil {
		return errors.New("error writing overflow record part length to file")
	}
	return nil
}

/*
RestoreRecord retrieves and restores a record from the WAL based on the provided offset.

Parameters:
- offset: Offset within the WAL to start the restoration process.

Returns:
- *record.Record: Restored record.
- error: Error, if any, during the restoration process.
*/
func (wal *WAL) RestoreRecord(offset int64) (*record.Record, int64, error) {
	if offset != -1 {
		wal.RepairOffset = offset
	}

	f, err := os.OpenFile(wal.SegmentFiles[wal.RepairFileIndex], os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, errors.New("error opening new segment file for writing")
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)

	mmappedData, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, 0, errors.New("error mmaping new file")
	}
	defer func(mmappedData *mmap.MMap) {
		err := mmappedData.Unmap()
		if err != nil {

		}
	}(&mmappedData)
	currentSeek, _ := f.Seek(0, 1)
	eofSeek := len(mmappedData)
	_, _ = f.Seek(currentSeek, 0)
	if int(offset) >= eofSeek {
		return nil, 0, nil
	}

	leftoverSpace := wal.SegmentSize - offset

	if leftoverSpace < record.RECORD_HEADER_SIZE {
		recordFirstPartBytes := make([]byte, leftoverSpace)
		copy(recordFirstPartBytes, mmappedData[wal.RepairOffset:])

		if wal.RepairFileIndex == int64(len(wal.SegmentFiles)-1) {
			return nil, 0, errors.New("not enough segment files")
		}

		wal.RepairFileIndex++
		wal.RepairOffset = 0

		secondFile, err := os.OpenFile(wal.SegmentFiles[wal.RepairFileIndex], os.O_RDWR, 0644)
		if err != nil {
			return nil, 0, errors.New("error opening new segment file for writing")
		}
		defer func(secondFile *os.File) {
			err := secondFile.Close()
			if err != nil {

			}
		}(secondFile)

		mmapedDataSecondFile, err := mmap.Map(secondFile, mmap.RDWR, 0)
		if err != nil {
			return nil, 0, errors.New("error mmaping new file")
		}
		defer func(mmapedDataSecondFile *mmap.MMap) {
			err := mmapedDataSecondFile.Unmap()
			if err != nil {

			}
		}(&mmapedDataSecondFile)

		leftoverRecSizeBytes := make([]byte, 8)
		copy(leftoverRecSizeBytes, mmapedDataSecondFile[:8])
		leftoverRecSize := binary.LittleEndian.Uint64(leftoverRecSizeBytes)

		wal.RepairOffset += 8

		recSecondPartBytes := make([]byte, leftoverRecSize)
		copy(recSecondPartBytes, mmapedDataSecondFile[8:8+leftoverRecSize])

		wal.RepairOffset += int64(leftoverRecSize)

		recBytes := append(recordFirstPartBytes, recSecondPartBytes...)
		rec := record.BytesToRecord(recBytes)

		if record.CrcHash(rec.GetValue()) != rec.GetCrc() {
			return nil, 0, errors.New("crc does not match the hashed value")
		}

		return rec, wal.RepairOffset, nil

	}

	recHeader := make([]byte, record.RECORD_HEADER_SIZE)
	copy(recHeader, mmappedData[wal.RepairOffset:wal.RepairOffset+record.RECORD_HEADER_SIZE])

	recSize := record.Size(recHeader)

	if int64(recSize) > leftoverSpace {
		recordFirstPartBytes := make([]byte, leftoverSpace)
		copy(recordFirstPartBytes, mmappedData[wal.RepairOffset:])

		if wal.RepairFileIndex == int64(len(wal.SegmentFiles)-1) {
			return nil, 0, errors.New("not enough segment files")
		}

		wal.RepairFileIndex++
		wal.RepairOffset = 0

		secondFile, err := os.OpenFile(wal.SegmentFiles[wal.RepairFileIndex], os.O_RDWR, 0644)
		if err != nil {
			return nil, 0, errors.New("error opening new segment file for writing")
		}
		defer func(secondFile *os.File) {
			err := secondFile.Close()
			if err != nil {

			}
		}(secondFile)

		mmapedDataSecondFile, err := mmap.Map(secondFile, mmap.RDWR, 0)
		if err != nil {
			return nil, 0, errors.New("error mmaping new file")
		}
		defer func(mmapedDataSecondFile *mmap.MMap) {
			err := mmapedDataSecondFile.Unmap()
			if err != nil {

			}
		}(&mmapedDataSecondFile)

		leftoverRecSizeBytes := make([]byte, 8)
		copy(leftoverRecSizeBytes, mmapedDataSecondFile[:8])
		leftoverRecSize := binary.LittleEndian.Uint64(leftoverRecSizeBytes)

		wal.RepairOffset += 8

		recSecondPartBytes := make([]byte, leftoverRecSize)
		copy(recSecondPartBytes, mmapedDataSecondFile[8:8+leftoverRecSize])

		wal.RepairOffset += int64(leftoverRecSize)

		recBytes := append(recordFirstPartBytes, recSecondPartBytes...)
		rec := record.BytesToRecord(recBytes)

		if record.CrcHash(rec.GetValue()) != rec.GetCrc() {
			return nil, 0, errors.New("crc does not match the hashed value")
		}

		return rec, wal.RepairOffset, nil
	}

	recBytes := make([]byte, recSize)
	copy(recBytes, mmappedData[wal.RepairOffset:wal.RepairOffset+int64(recSize)])

	wal.RepairOffset += int64(recSize)

	rec := record.BytesToRecord(recBytes)

	if record.CrcHash(rec.GetValue()) != rec.GetCrc() {
		return nil, 0, errors.New("error opening new segment file for writing")
	}

	return rec, wal.RepairOffset, nil
}

/*
DeleteLWM deletes WAL segment files up to the specified Low Watermark (LWM).

Parameters:
- lwm: Low Watermark specifying the index up to which segments should be deleted.
*/
func (wal *WAL) DeleteLWM(lwm uint64) error {
	toDelte := make([]string, lwm)
	copy(toDelte, wal.SegmentFiles[:lwm])

	wal.SegmentFiles = wal.SegmentFiles[lwm:]

	for i := 0; i < len(toDelte); i++ {
		err := os.Remove(toDelte[i])

		if err != nil {
			return errors.New("error deleting file")
		}
	}

	err := wal.renameSegments()
	if err != nil {
		return err
	}

	return nil

}

/*
renameSegments renames WAL segment files to maintain sequential order.
*/
func (wal *WAL) renameSegments() error {

	for i := 0; i < len(wal.SegmentFiles); i++ {
		oldName := wal.SegmentFiles[i]
		newName := fmt.Sprintf("%s_%d.log", FILEPATH, i+1)

		err := os.Rename(oldName, newName)
		wal.SegmentFiles[i] = newName

		if err != nil {
			return errors.New("error renaming file")
		}

	}
	return nil

}
