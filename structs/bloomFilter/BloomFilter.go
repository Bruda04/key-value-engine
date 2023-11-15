package bloomFilter

import "math"

type BloomFilter struct {
	elNum         uint
	data          []byte //automatically all elements are 0
	hashNum       uint
	hashFunctions []HashWithSeed //number of hash functions
	size          int
}

/*
Initialize Bloom Filter

	-accepts number of elements we assume we'll have (1000000)
	-probability (0.1), % of false positives we accept

falsePositives - Bloom filter is never wrong, but might sometimes tell an element exists when it doesn't
*/
func MakeBloomFilter(expectedEl int, probability float64) *BloomFilter {
	size := getSize(expectedEl, probability)
	hashNum := getNumHash(size, expectedEl)
	hashFunctions := CreateHashFunctions(uint(hashNum))
	return &BloomFilter{uint(expectedEl),
		make([]byte, size),
		uint(hashNum),
		hashFunctions,
		size}
}

// returns number of hash functions necessary
func getNumHash(size, numEl int) uint {
	return uint(float64(size) / float64(numEl) * math.Log(2))
}

func getSize(numEl int, probability float64) int {
	return int(-float64(numEl) * math.Log(probability) / math.Pow(math.Log(2), 2))
}

// adding new elements, uses murmur hash
func (b *BloomFilter) Add(element []byte) {
	for i := 0; i < int(b.hashNum); i++ {
		position := b.hashFunctions[i].Hash(element) % uint64(b.size)
		b.data[position] = 1
	}
}

// currently accepts strings
func (b *BloomFilter) IsPresent(element []byte) bool {
	for i := 0; i < int(b.hashNum); i++ {
		position := b.hashFunctions[i].Hash(element) % uint64(b.size)
		if b.data[position] == 0 {
			return false
		}
	}
	return true
}
