package hll

import (
	"errors"
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

type HyperLogLog struct {
	m   uint64
	p   uint8
	reg []uint8
}

/*
MakeHLL function creates and initializes a new HyperLogLog structure with the specified precision (p).
If the precision provided is outside the valid range it returns an error
Parameters:
- p: Precision value (uint8) representing the desired precision level for the HyperLogLog.
Returns:
- *HyperLogLog: A pointer to the newly created HyperLogLog structure with initialized fields.
- error: An error indicating any issues encountered during the creation process.
*/
func MakeHLL(p uint8) (*HyperLogLog, error) {
	if p < HLL_MIN_PRECISION || p > HLL_MAX_PRECISION {
		return nil, errors.New("Illegal precision")
	}

	m := uint64(math.Pow(2, float64(p)))

	newHLL := &HyperLogLog{
		m:   m,
		p:   p,
		reg: make([]uint8, m),
	}
	return newHLL, nil
}

/*
Add method incorporates a new data element into the HyperLogLog structure.
Parameters:

- data: Input data (byte slice) to be added to the HyperLogLog structure.

Usage:
hllInstance := &HyperLogLog{} // Create an instance of HyperLogLog
hllInstance.Add(someData)    // Incorporate 'someData' into 'hllInstance'
// 'hllInstance' now represents the updated HyperLogLog structure after adding 'someData'.
*/

func (hll *HyperLogLog) Add(data []byte) {
	hashedData := createHash(data)
	k := firstKbits(hashedData, uint64(hll.p))
	r := trailingZeroBits(hashedData)
	if uint8(r+1) > hll.reg[k] {
		hll.reg[k] = uint8(r + 1)
	}
}

/*
Estimate method calculates and returns an estimation of the cardinality (number of unique elements)
represented by the HyperLogLog structure.

Returns:
- float64: Estimated cardinality of the set represented by the hll
*/
func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

// Hash function used for adding an element to a hll
func createHash(stream []byte) uint64 {
	h := fnv.New64()
	h.Write(stream)
	sum := h.Sum64()
	h.Reset()
	return sum
}
