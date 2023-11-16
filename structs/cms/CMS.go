package cms

import "math"

type CMS struct {
	k, m          uint
	data          [][]uint64
	hashFunctions []hashWithSeed
}

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
		k:             k,
		m:             m,
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
