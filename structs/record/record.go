package record

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

type Record struct {
	crc       uint32
	timestamp uint64
	tombstone bool
	keySize   uint64
	valueSize uint64
	key       string
	value     []byte
}

/*
MakeRecord creates a Record instance with the specified key, value, and tombstone status.

Parameters:
  - key: A string representing the key for the Record.
  - value: A byte slice representing the value for the Record.
  - deleted: A boolean indicating whether the Record is marked as deleted (tombstone).

Returns:
  - Pointer to a Record instance initialized with the provided parameters.
*/
func MakeRecord(key string, value []byte, deleted bool) *Record {
	return &Record{
		crc:       CrcHash(value),
		timestamp: uint64(time.Now().Unix()),
		tombstone: deleted,
		keySize:   uint64(len([]byte(key))),
		valueSize: uint64(len(value)),
		key:       key,
		value:     value,
	}
}

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
)

/*
CrcHash calculates the CRC32 hash for the given byte slice.

Parameters:
  - data: A byte slice for which the CRC32 hash is to be calculated.

Returns:
  - uint32: The CRC32 hash value.
*/
func CrcHash(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func (r *Record) GetCrc() uint32 {
	return r.crc
}

func (r *Record) GetTimestamp() uint64 {
	return r.timestamp
}

func (r *Record) IsTombstone() bool {
	return r.tombstone
}

func (r *Record) GetKeySize() uint64 {
	return r.keySize
}

func (r *Record) GetValueSize() uint64 {
	return r.valueSize
}

func (r *Record) GetKey() string {
	return r.key
}

func (r *Record) GetValue() []byte {
	return r.value
}

/*
RecordToBytes converts the Record to a byte slice.

Returns:
  - []byte: A byte slice representing the serialized form of the Record.
*/
func (r *Record) RecordToBytes() []byte {
	crcBytes := make([]byte, CRC_SIZE)
	binary.LittleEndian.PutUint32(crcBytes, r.crc)

	timestampBytes := make([]byte, TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(timestampBytes, r.timestamp)

	tombstoneBytes := []byte{0}
	if r.tombstone {
		tombstoneBytes[0] = 1
	}

	keySizeBytes := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(keySizeBytes, r.keySize)

	valueSizeBytes := make([]byte, VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(valueSizeBytes, r.valueSize)

	keyBytes := []byte(r.key)
	valueBytes := r.value

	result := append(crcBytes, timestampBytes...)
	result = append(result, tombstoneBytes...)
	result = append(result, keySizeBytes...)
	result = append(result, valueSizeBytes...)
	result = append(result, keyBytes...)
	result = append(result, valueBytes...)

	return result
}

/*
BytesToRecord converts a byte slice to a Record instance.

Parameters:
  - bytes: A byte slice representing the serialized form of the Record.

Returns:
  - *Record: Pointer to a Record instance initialized with the data from the byte slice.
*/
func BytesToRecord(bytes []byte) *Record {
	r := Record{}

	r.crc = binary.LittleEndian.Uint32(bytes[CRC_START:TIMESTAMP_START])

	r.timestamp = binary.LittleEndian.Uint64(bytes[TIMESTAMP_START:TOMBSTONE_START])

	r.tombstone = bytes[TOMBSTONE_START] == 1

	r.keySize = binary.LittleEndian.Uint64(bytes[KEY_SIZE_START:VALUE_SIZE_START])

	r.valueSize = binary.LittleEndian.Uint64(bytes[VALUE_SIZE_START:RECORD_HEADER_SIZE])

	r.key = string(bytes[KEY_START : KEY_START+r.keySize])
	r.value = bytes[KEY_START+r.keySize : KEY_START+r.keySize+r.valueSize]

	return &r
}

/*
Size calculates the Record size in bytes.

Returns:
  - int: Record size in bytes.
*/
func (r *Record) Size() int {
	return int(CRC_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + r.keySize + r.valueSize)
}

/*
Size calculates the total size of a record based on the provided header.

Parameters:
  - header: A byte slice representing the header of the record.

Returns:
  - int: The total size of the record calculated based on the information in the header.
*/
func Size(header []byte) int {
	keySize := binary.LittleEndian.Uint64(header[KEY_SIZE_START:VALUE_SIZE_START])
	valueSize := binary.LittleEndian.Uint64(header[VALUE_SIZE_START:RECORD_HEADER_SIZE])
	return int(CRC_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + keySize + valueSize)

}

/*
PrintRecord prints the fields of a Record struct in a formatted way.
*/
func (r *Record) PrintRecord() {
	fmt.Printf("CRC: %d\n", r.crc)
	fmt.Printf("Timestamp: %d\n", r.timestamp)
	fmt.Printf("Tombstone: %t\n", r.tombstone)
	fmt.Printf("Key Size: %d\n", r.keySize)
	fmt.Printf("Value Size: %d\n", r.valueSize)
	fmt.Printf("Key: %s\n", r.key)
	fmt.Printf("Value: %v\n", r.value)
}

/*
SSTRecordToBytes converts a Record instance to a byte slice for an SST (Sorted String Table) file.

Returns:
  - []byte: A byte slice representing the serialized form of the Record.

The function distinguishes between regular records and tombstone records.
If the record is not a tombstone, it delegates the conversion to the RecordToBytes function.
*/
func (r *Record) SSTRecordToBytes() []byte {
	if !r.tombstone {
		return r.RecordToBytes()
	}

	crcBytes := make([]byte, CRC_SIZE)
	binary.LittleEndian.PutUint32(crcBytes, r.crc)

	timestampBytes := make([]byte, TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(timestampBytes, r.timestamp)

	tombstoneBytes := []byte{0}
	if r.tombstone {
		tombstoneBytes[0] = 1
	}

	keySizeBytes := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(keySizeBytes, r.keySize)

	keyBytes := []byte(r.key)

	result := append(crcBytes, timestampBytes...)
	result = append(result, tombstoneBytes...)
	result = append(result, keySizeBytes...)
	result = append(result, keyBytes...)

	return result
}

/*
SSTBytesToRecord converts a byte slice to a Record instance for an SST file.

Parameters:
  - bytes: A byte slice representing the serialized form of the Record.

Returns:
  - *Record: Pointer to a Record instance initialized with the data from the byte slice.

The function distinguishes between regular records and tombstone records by checking
the value at the TOMBSTONE_START position in the byte slice. If it's 1, indicating a tombstone,
it creates a Record instance with tombstone information, otherwise, it delegates the conversion
to the BytesToRecord function.
*/
func SSTBytesToRecord(bytes []byte) *Record {
	tombstone := bytes[TOMBSTONE_START] == 1

	if !tombstone {
		return BytesToRecord(bytes)
	}

	r := Record{}

	r.tombstone = tombstone

	r.crc = binary.LittleEndian.Uint32(bytes[CRC_START:TIMESTAMP_START])

	r.timestamp = binary.LittleEndian.Uint64(bytes[TIMESTAMP_START:TOMBSTONE_START])

	r.keySize = binary.LittleEndian.Uint64(bytes[KEY_SIZE_START:VALUE_SIZE_START])

	r.valueSize = 0

	r.key = string(bytes[VALUE_SIZE_START : VALUE_SIZE_START+r.keySize])

	r.value = make([]byte, 0)

	return &r
}
