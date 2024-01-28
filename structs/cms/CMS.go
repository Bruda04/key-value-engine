package cms

import (
	"encoding/binary"
	"errors"
	"math"
)

type CMS struct {
	k, m          uint64
	data          [][]uint64
	hashFunctions []hashWithSeed
}

const (
	K_SIZE         = 8
	M_SIZE         = 8
	HASH_FUNC_SIZE = 8
	SEED_SIZE      = 8

	K_START         = 0
	M_START         = K_START + K_SIZE
	HASH_FUNC_START = M_START + M_SIZE
	DATA_START_CMS  = HASH_FUNC_START + HASH_FUNC_SIZE
	CMS_HEADER_SIZE = K_SIZE + M_SIZE + HASH_FUNC_SIZE
)

/*
MakeCMS creates a Count-Min Sketch (CMS) data structure with specified parameters.

Parameters:
  - epsilon: A floating-point value influencing the accuracy of the sketch. Smaller values result in more accurate estimations.
  - delta: A floating-point value controlling the failure probability of the algorithm. Smaller delta reduces the likelihood of overestimation.

Returns:
  - Pointer to a CMS instance configured with the specified parameters and initialized data structures.
*/
func MakeCMS(epsilon float64, delta float64) *CMS {
	m := calculateM(epsilon)
	k := calculateK(delta)
	data := make([][]uint64, k)
	for i := range data {
		data[i] = make([]uint64, m)
	}
	hfs := createHashFunctions(k)

	return &CMS{
		k:             uint64(k),
		m:             uint64(m),
		data:          data,
		hashFunctions: hfs,
	}
}

/*
Add increments the counters in the Count-Min Sketch (CMS) data structure for a given element.

Parameters:
  - cms: Pointer to the CMS instance to which the element's count should be incremented.
  - element: Byte slice representing the element whose count needs to be increased.
*/
func (cms *CMS) Add(element []byte) {
	for i, hf := range cms.hashFunctions {
		index := hf.hash(element) % uint64(cms.m)
		cms.data[i][index]++
	}
}

/*
Estimate returns the approximate count of a given element in the Count-Min Sketch (CMS) data structure.

Parameters:
  - cms: Pointer to the CMS instance from which the element's count should be estimated.
  - element: Byte slice representing the element whose count needs to be estimated.

Returns:
  - Approximate count of the element based on the minimum value across hash functions.

Note: The Estimate function is used to retrieve approximate counts of elements stored in the CMS.
*/
func (cms *CMS) Estimate(element []byte) uint64 {
	minVal := uint64(math.MaxUint64)
	for i, hf := range cms.hashFunctions {
		index := hf.hash(element) % uint64(cms.m)
		tmpVal := cms.data[i][index]
		if tmpVal < minVal {
			minVal = tmpVal
		}
	}

	return minVal
}

func (cms *CMS) CMSToBytes() []byte {
	kBytes := make([]byte, K_SIZE)
	binary.LittleEndian.PutUint64(kBytes, cms.k)

	mBytes := make([]byte, M_SIZE)
	binary.LittleEndian.PutUint64(mBytes, cms.m)

	var hashFuncBytes []byte
	for _, hf := range cms.hashFunctions {
		seedSizeBytes := make([]byte, SEED_SIZE)
		binary.LittleEndian.PutUint64(seedSizeBytes, uint64(len(hf.Seed)))
		hashFuncBytes = append(hashFuncBytes, seedSizeBytes...)
		hashFuncBytes = append(hashFuncBytes, hf.Seed...)
	}

	var dataBytes []byte
	for _, row := range cms.data {
		for _, value := range row {
			valueBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(valueBytes, value)
			dataBytes = append(dataBytes, valueBytes...)
		}
	}

	result := append(kBytes, mBytes...)
	result = append(result, hashFuncBytes...)
	result = append(result, dataBytes...)

	return result
}

func BytesToCMS(data []byte) (*CMS, error) {
	if len(data) < CMS_HEADER_SIZE {
		return nil, errors.New("insufficient data for CMS header")
	}

	k := binary.LittleEndian.Uint64(data[K_START:M_START])
	m := binary.LittleEndian.Uint64(data[M_START:HASH_FUNC_START])

	if len(data) < int(CMS_HEADER_SIZE) {
		return nil, errors.New("insufficient data for CMS payload")
	}

	var hashFunctions []hashWithSeed
	index := HASH_FUNC_START
	for i := 0; i < int(k); i++ {
		seedSize := binary.LittleEndian.Uint64(data[index : index+SEED_SIZE])
		index += SEED_SIZE
		seed := data[index : index+int(seedSize)]
		index += int(seedSize)

		hashFunctions = append(hashFunctions, hashWithSeed{Seed: seed})
	}

	// Initialize the 2D data slice
	dataSlice := make([][]uint64, k)
	for i := range dataSlice {
		dataSlice[i] = make([]uint64, m)
	}

	// Populate the 2D data slice with values
	for i := 0; i < int(k); i++ {
		for j := 0; j < int(m); j++ {
			value := binary.LittleEndian.Uint64(data[index : index+8])
			index += 8
			dataSlice[i][j] = value
		}
	}

	cms := &CMS{
		k:             k,
		m:             m,
		hashFunctions: hashFunctions,
		data:          dataSlice,
	}

	return cms, nil
}
