package wal

import (
	"errors"
	"fmt"
	"io"
	"key-value-engine/structs/record"
	"os"
)

const (
	// Temporary data path
	DATA_PATH   = ".." + string(os.PathSeparator) + ".." + string(os.PathSeparator) + "data" + string(os.PathSeparator) + "wal"
	PATH_PREFIX = DATA_PATH + string(os.PathSeparator) + "wal_"
)

type WAL struct {
	SegmentSize   uint64           // User defined segment size in bytes (ex. 100)
	LatestSegment []*record.Record // List of pointers to records
	SegmentFiles  []string         // Segment filenames
}

/*
MakeWAL initializes a new Write-Ahead Log (WAL) with the provided segment size.
Parameters:

	segmentSize: uint64 - The user-defined length in bytes for each segment of the WAL.

Returns:

	*WAL: Pointer to the initialized WAL.
	error: An error if any occurred during the initialization process.
*/
func MakeWAL(segmentSize uint64) (*WAL, error) {
	if _, err := os.Stat(DATA_PATH); os.IsNotExist(err) {
		if err := os.MkdirAll(DATA_PATH, 0755); err != nil {
			return nil, fmt.Errorf("error creating data directory: %s", err)
		}
	}

	initialSegmentFile := PATH_PREFIX + "_1.log"

	if _, err := os.Stat(initialSegmentFile); err == nil {
		if err := os.Remove(initialSegmentFile); err != nil {
			return nil, fmt.Errorf("error removing existing initial segment file: %s", err)
		}
	}

	_, err := os.Create(initialSegmentFile)
	if err != nil {
		return nil, fmt.Errorf("error creating initial segment file: %s", err)
	}

	wal := &WAL{
		SegmentSize:   segmentSize,
		SegmentFiles:  []string{initialSegmentFile},
		LatestSegment: make([]*record.Record, 0),
	}

	return wal, nil
}

/*
AddRecord adds a new record to the Write-Ahead Log (WAL).
Parameters:

	key: string - The key associated with the record.
	value: []byte - The value of the record.
	deleted: bool - Whether the record is marked as deleted or not.

Returns:

	error: An error if any occurred while adding the record.
*/
func (wal *WAL) AddRecord(key string, value []byte, deleted bool) error {
	rec := record.MakeRecord(key, value, deleted)
	recordSize := uint64(record.RECORD_HEADER_SIZE + rec.GetKeySize() + rec.GetValueSize())

	totalSize := recordSize
	for _, r := range wal.LatestSegment {
		totalSize += uint64(record.RECORD_HEADER_SIZE + r.GetKeySize() + r.GetValueSize())
	}

	wal.LatestSegment = append(wal.LatestSegment, rec)

	if totalSize > wal.SegmentSize {
		if err := wal.serializeSegment(); err != nil {
			return fmt.Errorf("error writing segment: %s", err)
		}
	}
	return nil
}

/*
serializeSegment writes the records of the segment into a new segment file.
Returns:

	error: An error if any occurred during the serialization process.
*/
func (wal *WAL) serializeSegment() error {
	// Ensure there is an active file
	if len(wal.SegmentFiles) == 0 {
		return errors.New("no active segment file")
	}

	// Iterate over all records in the segment
	for _, rec := range wal.LatestSegment {
		recordBytes := rec.RecordToBytes()

		// Open the current segment file for writing
		file, err := os.OpenFile(wal.SegmentFiles[len(wal.SegmentFiles)-1], os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("error opening segment file for writing: %s", err)
		}

		// Check if adding the current record exceeds the segment size
		fileSize, err := file.Seek(0, io.SeekEnd)
		if err != nil {
			file.Close()
			return fmt.Errorf("error getting segment file size: %s", err)
		}

		// If adding the current record exceeds the segment size, split the record
		if uint64(fileSize)+uint64(len(recordBytes)) > wal.SegmentSize {

			// Calculate how much of the record can fit in the current segment
			bytesToFit := wal.SegmentSize - uint64(fileSize)
			firstPart := recordBytes[:bytesToFit]
			secondPart := recordBytes[bytesToFit:]

			if _, err := file.Write(firstPart); err != nil {
				file.Close()
				return fmt.Errorf("error writing first part of record to segment file: %s", err)
			}
			file.Close()

			// Create a new segment for the second part
			wal.makeSegment()

			// Open the new segment file for writing
			file, err = os.OpenFile(wal.SegmentFiles[len(wal.SegmentFiles)-1], os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("error opening new segment file for writing: %s", err)
			}

			// Write the second part to the new segment file
			if _, err := file.Write(secondPart); err != nil {
				file.Close()
				return fmt.Errorf("error writing second part of record to new segment file: %s", err)
			}
			file.Close()
		} else {
			// If the record fits in the current segment, simply write it to the file
			if _, err := file.Write(recordBytes); err != nil {
				file.Close()
				return fmt.Errorf("error writing record to segment file: %s", err)
			}
			file.Close()
		}
	}

	return nil
}

/*
makeSegment creates a new segment and appends it to the WAL's segments.
*/
func (wal *WAL) makeSegment() {
	// Create a new segment file
	newSegmentFile := PATH_PREFIX + fmt.Sprintf("_%d.log", len(wal.SegmentFiles)+1)
	wal.SegmentFiles = append(wal.SegmentFiles, newSegmentFile)

	// Update the LatestSegment to an empty slice for the new segment
	wal.LatestSegment = make([]*record.Record, 0)

	// Create the new segment file
	file, err := os.Create(newSegmentFile)
	if err != nil {
		fmt.Printf("Error creating new segment file: %s\n", err)
		return
	}
	defer file.Close()
}

// Potential deserialization implementation (doesn't work)
//func (wal *WAL) deserializeSegment(segmentFile string) ([]*Record, error) {
//	file, err := os.Open(segmentFile)
//	if err != nil {
//		return nil, fmt.Errorf("error opening segment file for reading: %s", err)
//	}
//	defer file.Close()
//
//	var records []*Record
//
//	for {
//		recordBytes, err := readBytesFromFile(file, RECORD_HEADER_SIZE)
//		if err != nil {
//			if err == io.EOF {
//				break // End of file
//			}
//			return nil, fmt.Errorf("error reading record header: %s", err)
//		}
//
//		record := BytesToRecord(recordBytes)
//		// Read key and value for the record
//		keyAndValueBytes, err := readBytesFromFile(file, uint64(record.GetKeySize()+uint64(len(record.GetValue()))))
//		if err != nil {
//			return nil, fmt.Errorf("error reading key and value for record: %s", err)
//		}
//
//		// Update key and value for the record
//		record.key = string(keyAndValueBytes[:record.GetKeySize()])
//		record.value = keyAndValueBytes[record.GetKeySize():]
//
//		records = append(records, record)
//	}
//
//	return records, nil
//}
//
//func (wal *WAL) loadSegments() error {
//	for _, segmentFile := range wal.SegmentFiles {
//		records, err := wal.deserializeSegment(segmentFile)
//		if err != nil {
//			return fmt.Errorf("error loading segment %s: %s", segmentFile, err)
//		}
//
//		wal.LatestSegment = append(wal.LatestSegment, records...)
//	}
//
//	return nil
//}
//
//func readBytesFromFile(file *os.File, size uint64) ([]byte, error) {
//	data := make([]byte, size)
//	n, err := file.Read(data)
//	if err != nil {
//		return nil, fmt.Errorf("error reading from file: %s", err)
//	}
//	if uint64(n) != size {
//		return nil, fmt.Errorf("unexpected number of bytes read from file")
//	}
//	return data, nil
//}
