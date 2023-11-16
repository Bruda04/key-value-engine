package cms

import (
	"crypto/md5"
	"encoding/binary"
	"time"
)

/*
HashWithSeed is a struct representing a hash function with a specified seed.

Fields:
  - Seed: Byte slice representing the seed used in the hash function.

Purpose:
  - Provides a structure for creating hash functions with specific seeds, allowing controlled randomness in hashing.
*/
type hashWithSeed struct {
	Seed []byte
}

/*
Hash calculates the hash value for a given byte slice using the MD5 hash function with a specified seed.

Parameters:
  - h: HashWithSeed instance representing the hash function with a specific seed.
  - data: Byte slice to be hashed.

Returns:
  - 64-bit unsigned integer representing the hash value.
*/
func (h hashWithSeed) hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

/*
CreateHashFunctions generates a slice of HashWithSeed instances, each with a unique seed, to be used as hash functions in a Count-Min Sketch (CMS).

Parameters:
  - k: Number of hash functions to generate.

Returns:
  - Slice of HashWithSeed instances, each with a unique seed.
*/
func createHashFunctions(k uint) []hashWithSeed {
	h := make([]hashWithSeed, k)
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := hashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}
