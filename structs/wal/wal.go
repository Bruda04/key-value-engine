package wal

import (
	"fmt"
	"key-value-engine/structs/record"
	"os"
)

const (
	// Temporary data path
	DATA_PATH   = ".." + string(os.PathSeparator) + ".." + string(os.PathSeparator) + "data" + string(os.PathSeparator) + "wal"
	PATH_PREFIX = DATA_PATH + string(os.PathSeparator) + "wal_"
)

type Segment struct {
	records            []*record.Record // Slice of records in the current segment
	currentSegmentSize uint64           // Stores the length in bytes of the current segment
}

type WAL struct {
	SegmentSize  uint64     // User defined length in bytes
	Segments     []*Segment // Slice of WAL's segments
	SegmentFiles []string   // List of current segment filenames
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

	wal := &WAL{
		SegmentSize:  segmentSize,
		Segments:     make([]*Segment, 0),
		SegmentFiles: make([]string, 0),
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

	if len(wal.Segments) == 0 {
		wal.makeSegment()
	}

	newestSegment := wal.Segments[len(wal.Segments)-1]

	if newestSegment.currentSegmentSize+recordSize > wal.SegmentSize {
		if err := wal.serializeSegment(); err != nil {
			return fmt.Errorf("error creating new segment: %s", err)
		}
		newestSegment = wal.Segments[len(wal.Segments)-1]
	}
	newestSegment.addRecordToSegment(rec, recordSize)
	//RECORD ADDED TO THE SEGMENT, NOTIFY MEMTABLE TO PROCEED WITH ADDING THE RECORD
	return nil
}

/*
addRecordToSegment appends a record to the segment and updates the segment size.
Parameters:

	rec: *record.Record - The record to be added.
	recordSize: uint64 - Size of the record in bytes.
*/
func (segment *Segment) addRecordToSegment(rec *record.Record, recordSize uint64) {
	segment.records = append(segment.records, rec)
	segment.currentSegmentSize += recordSize
}

/*
serializeSegment writes the records of the segment into a new segment file.
Returns:

	error: An error if any occurred during the serialization process.
*/
func (wal *WAL) serializeSegment() error {
	newSegmentFile := PATH_PREFIX + fmt.Sprintf("_%d.log", len(wal.SegmentFiles)+1)

	_, err := os.Create(newSegmentFile)
	if err != nil {
		return fmt.Errorf("error creating new segment file: %s", err)
	}
	wal.SegmentFiles = append(wal.SegmentFiles, newSegmentFile)

	activeFile, err := os.OpenFile(newSegmentFile, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %s", err)
	}
	segment := wal.Segments[len(wal.Segments)-1]

	for _, rec := range segment.records {
		recordBytes := rec.RecordToBytes()

		_, err := activeFile.Write(recordBytes)
		if err != nil {
			return fmt.Errorf("error writing record to segment file: %s", err)
		}
	}

	if activeFile != nil {
		if err := activeFile.Close(); err != nil {
			return fmt.Errorf("error closing previous segment file: %s", err)
		}
	}

	wal.makeSegment()

	return nil
}

/*
makeSegment creates a new segment and appends it to the WAL's segments.
*/
func (wal *WAL) makeSegment() {
	segment := &Segment{
		records:            make([]*record.Record, 0),
		currentSegmentSize: 0,
	}
	wal.Segments = append(wal.Segments, segment)
}
