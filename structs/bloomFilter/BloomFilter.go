package bloomFilter

import (
	"encoding/binary"
	"errors"
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
	elNumBytes := make([]byte, EL_NUM_SIZE)
	binary.LittleEndian.PutUint64(elNumBytes, bf.elNum)

	hashNumBytes := make([]byte, HASH_NUM_SIZE)
	binary.LittleEndian.PutUint64(hashNumBytes, bf.hashNum)

	sizeBytes := make([]byte, SIZE_SIZE)
	binary.LittleEndian.PutUint64(sizeBytes, bf.size)

	var hashFuncBytes []byte
	for _, hf := range bf.hashFunctions {
		seedSizeBytes := make([]byte, SEED_SIZE_SIZE)
		binary.LittleEndian.PutUint64(seedSizeBytes, uint64(len(hf.Seed)))
		hashFuncBytes = append(hashFuncBytes, seedSizeBytes...)
		hashFuncBytes = append(hashFuncBytes, hf.Seed...)
	}

	result := append(elNumBytes, hashNumBytes...)
	result = append(result, sizeBytes...)
	result = append(result, hashFuncBytes...)
	result = append(result, bf.data...)

	return result
}

func BytesToBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < BLOOM_FILTER_HEADER_SIZE {
		return nil, errors.New("insufficient data for BloomFilter header")
	}

	elNum := binary.LittleEndian.Uint64(data[EL_NUM_START:HASH_NUM_START])
	hashNum := binary.LittleEndian.Uint64(data[HASH_NUM_START:SIZE_START])
	size := binary.LittleEndian.Uint64(data[SIZE_START:HASH_FUNC_START])

	if len(data) < int(BLOOM_FILTER_HEADER_SIZE+size) {
		return nil, errors.New("insufficient data for BloomFilter payload")
	}

	var hashFunctions []HashWithSeed
	index := HASH_FUNC_START
	for i := 0; i < int(hashNum); i++ {
		seedSize := binary.LittleEndian.Uint64(data[index : index+SEED_SIZE_SIZE])
		index += SEED_SIZE_SIZE
		seed := data[index : index+int(seedSize)]
		index += int(seedSize)

		hashFunctions = append(hashFunctions, HashWithSeed{Seed: seed})
	}

	bf := &BloomFilter{
		elNum:         elNum,
		hashNum:       hashNum,
		size:          size,
		hashFunctions: hashFunctions,
		data:          data[index:],
	}

	return bf, nil
}
