package bloomFilter

import (
	"math"
)

type BloomFilter struct {
	elNum         uint64
	hashNum       uint64
	size          uint64
	hashFunctions []HashWithSeed //number of hash functions
	data          []byte         //automatically all elements are 0

}

const (
	EL_NUM_SIZE    = 8
	HASH_NUM_SIZE  = 8
	SIZE_SIZE      = 8
	SEED_SIZE_SIZE = 8

	EL_NUM_START    = 0
	HASH_NUM_START  = EL_NUM_START + EL_NUM_SIZE
	SIZE_START      = HASH_NUM_START + HASH_NUM_SIZE
	HASH_FUNC_START = SIZE_START + SIZE_SIZE
	DATA_START      = HASH_FUNC_START + SEED_SIZE_SIZE

	BLOOM_FILTER_HEADER_SIZE = EL_NUM_SIZE + HASH_NUM_SIZE + SIZE_SIZE
)

/*
Initialize Bloom Filter

	-accepts number of elements we assume we'll have (1000000)
	-probability (0.1), % of false positives we accept

falsePositives - Bloom filter is never wrong, but might sometimes tell an element exists when it doesn't
*/
func MakeBloomFilter(expectedEl uint64, probability float64) *BloomFilter {
	size := getSize(expectedEl, probability)
	hashNum := getNumHash(size, expectedEl)
	hashFunctions := CreateHashFunctions(uint(hashNum))
	return &BloomFilter{uint64(expectedEl),
		uint64(hashNum),
		size,
		hashFunctions,
		make([]byte, size)}
}

// returns number of hash functions necessary
func getNumHash(size, numEl uint64) uint64 {
	return uint64(float64(size) / float64(numEl) * math.Log(2))
}

func getSize(numEl uint64, probability float64) uint64 {
	return uint64(-float64(numEl) * math.Log(probability) / math.Pow(math.Log(2), 2))
}

// adding new elements
func (b *BloomFilter) Add(element []byte) {
	for i := 0; i < int(b.hashNum); i++ {
		position := b.hashFunctions[i].Hash(element) % uint64(b.size)
		b.data[position] = 1
	}
}

func (b *BloomFilter) IsPresent(element []byte) bool {
	for i := 0; i < int(b.hashNum); i++ {
		position := b.hashFunctions[i].Hash(element) % uint64(b.size)
		if b.data[position] == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) BloomFilterToBytes() []byte {
	var result []byte

	// Encode element number using variable-length encoding
	elNumBytes := encodeVarUint64(bf.elNum)
	result = append(result, elNumBytes...)

	// Encode hash number using variable-length encoding
	hashNumBytes := encodeVarUint64(bf.hashNum)
	result = append(result, hashNumBytes...)

	// Encode size using variable-length encoding
	sizeBytes := encodeVarUint64(bf.size)
	result = append(result, sizeBytes...)

	// Encode hash functions
	for _, hf := range bf.hashFunctions {
		// Encode seed size using variable-length encoding
		seedSizeBytes := encodeVarUint64(uint64(len(hf.Seed)))
		result = append(result, seedSizeBytes...)

		// Append seed
		result = append(result, hf.Seed...)
	}

	// Append data
	result = append(result, bf.data...)

	return result
}

// Helper function to encode uint64 as variable-length bytes
func encodeVarUint64(value uint64) []byte {
	var encoded []byte
	for value >= 0x80 {
		encoded = append(encoded, byte(value)|0x80)
		value >>= 7
	}
	encoded = append(encoded, byte(value))
	return encoded
}

func BytesToBloomFilter(data []byte) (*BloomFilter, error) {
	bf := &BloomFilter{}

	// Decode element number
	elNum, bytesRead := decodeVarUint64(data)
	bf.elNum = elNum
	data = data[bytesRead:]

	// Decode hash number
	hashNum, bytesRead := decodeVarUint64(data)
	bf.hashNum = hashNum
	data = data[bytesRead:]

	// Decode size
	size, bytesRead := decodeVarUint64(data)
	bf.size = size
	data = data[bytesRead:]

	// Decode hash functions
	var hashFunctions []HashWithSeed
	for i := 0; i < int(hashNum); i++ {
		// Decode seed size
		seedSize, bytesRead := decodeVarUint64(data)
		data = data[bytesRead:]

		// Extract seed
		seed := data[:seedSize]
		data = data[seedSize:]

		hashFunctions = append(hashFunctions, HashWithSeed{Seed: seed})
	}
	bf.hashFunctions = hashFunctions

	// The remaining data is the Bloom filter data
	bf.data = data

	return bf, nil
}

// Helper function to decode variable-length uint64 from byte slice
func decodeVarUint64(data []byte) (uint64, int) {
	var result uint64
	var bytesRead int
	for shift := 0; shift < 64; shift += 7 {
		if len(data) == 0 {
			break
		}
		b := data[0]
		data = data[1:]
		result |= (uint64(b) & 0x7F) << shift
		bytesRead++
		if b&0x80 == 0 {
			break
		}
	}
	return result, bytesRead
}
