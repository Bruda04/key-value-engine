package wal

import (
	"fmt"
	"hash/crc32"
	"os"
	"time"
)

// Segment size defined in bytes by user
// Neccessary check for integrity when loading in from disc
// Can only load in one wal writing in memory at the time, not the whole segment
// Cant delete a wal segment untill its pushed onto SSTable

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE

	RECORD_HEADER_SIZE = CRC_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE

	// Placeholder path
	FILE_ID   = "0001"
	DATA_PATH = ".." + string(os.PathSeparator) + ".." + string(os.PathSeparator) + "data" + string(os.PathSeparator) + "wal"
	PATH      = DATA_PATH + string(os.PathSeparator) + "wal_"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type Record struct {
	CRC       uint32
	Timestamp uint64
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

type WAL struct {
	SegmentSize        uint64    // User defined length in bytes
	WALRecords         []*Record // List of records
	SegmentFiles       []string  // List of current WAL's  segments
	currentSegmentSize uint64    // Stores the length in bytes of the current segment
}

func MakeWAL(segmentSize uint64) (*WAL, error) {
	if _, err := os.Stat(DATA_PATH); os.IsNotExist(err) {
		if err := os.MkdirAll(DATA_PATH, 0755); err != nil {
			return nil, fmt.Errorf("error creating data directory: %s", err)
		}
	}

	segmentFiles := []string{PATH + FILE_ID + ".log"}

	filePath := segmentFiles[0]

	var file *os.File
	var err error

	if _, statErr := os.Stat(filePath); os.IsNotExist(statErr) {
		file, err = os.Create(filePath)
		if err != nil {
			return nil, fmt.Errorf("error creating WAL file: %s", err)
		}
	} else {
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("error opening WAL file: %s", err)
		}
	}

	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			fmt.Printf("error closing WAL file: %s\n", closeErr)
		}
	}()

	wal := &WAL{
		SegmentSize:        segmentSize,
		WALRecords:         make([]*Record, 0),
		SegmentFiles:       segmentFiles,
		currentSegmentSize: 0,
	}

	return wal, nil
}

func (wal *WAL) Add(key string, value string, deleted bool) error {
	rec := newRecord(key, value, deleted)
	recordSize := uint64(RECORD_HEADER_SIZE + len(rec.Key) + len(rec.Value))

	if wal.currentSegmentSize+recordSize > wal.SegmentSize {
		if err := wal.createSegment(); err != nil {
			return fmt.Errorf("error creating new segment: %s", err)
		}
	}

	wal.WALRecords = append(wal.WALRecords, rec)
	wal.currentSegmentSize += recordSize

	return nil
}

func (wal *WAL) createSegment() error {
	newSegmentFile := PATH + FILE_ID + fmt.Sprintf("_%d.log", len(wal.SegmentFiles)+1)

	_, err := os.Create(newSegmentFile)
	if err != nil {
		return fmt.Errorf("error creating new segment file: %s", err)
	}

	activeFile, err := os.OpenFile(wal.SegmentFiles[len(wal.SegmentFiles)-1], os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %s", err)
	}

	//TO BE IMPLEMENTED - SERIALIZATION OF SEGMENTS
	//err = wal.writeSegment(activeFile)
	//if err != nil {
	//	return fmt.Errorf("error writing segment: %s", err)
	//}

	if activeFile != nil {
		if err := activeFile.Close(); err != nil {
			return fmt.Errorf("error closing previous segment file: %s", err)
		}
	}

	wal.SegmentFiles = append(wal.SegmentFiles, newSegmentFile)
	wal.currentSegmentSize = 0

	return nil
}

func newRecord(key string, value string, deleted bool) *Record {
	return &Record{
		CRC:       CRC32([]byte(value)),
		Timestamp: uint64(time.Now().Unix()),
		Tombstone: deleted,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     []byte(value),
	}
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// USAGE EXAMPLE
//func main() {
//	wal, err := MakeWAL(1024) // Segment size of 1024 bytes
//	if err != nil {
//		fmt.Println("Error creating WAL:", err)
//		return
//	}
//
//	// Adding records to the WAL...
//	for i := 0; i < 1000; i++ {
//		err = wal.Add(fmt.Sprintf("exampleKey%d", i), fmt.Sprintf("exampleValue%d", i), false)
//		if err != nil {
//			fmt.Println("Error adding record to WAL:", err)
//			return
//		}
//	}
//}
