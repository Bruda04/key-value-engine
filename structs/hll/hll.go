package hll

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
	HLL_M_SIZE        = 8
	HLL_P_SIZE        = 1
	HLL_HEADER_SIZE   = HLL_P_SIZE + HLL_M_SIZE
)

func firstKbits(value, k uint64) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) int {
	return bits.TrailingZeros64(value)
}

type HyperLogLog struct {
	M   uint64  // M is the number of registers (2^p)
	P   uint8   // P is the precision
	Reg []uint8 // Reg is the array of registers
}

/*
MakeHLL function creates and initializes a new HyperLogLog structure with the specified precision (P).
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
		M:   m,
		P:   p,
		Reg: make([]uint8, m),
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

func (hll *HyperLogLog) Add(data []byte) error {
	hashedData, err := createHash(data)
	if err != nil {
		return err
	}
	k := firstKbits(hashedData, uint64(hll.P))
	r := trailingZeroBits(hashedData)
	if uint8(r+1) > hll.Reg[k] {
		hll.Reg[k] = uint8(r + 1)
	}
	return nil
}

/*
Estimate method calculates and returns an estimation of the cardinality (number of unique elements)
represented by the HyperLogLog structure.

Returns:
- float64: Estimated cardinality of the set represented by the hll
*/
func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

// Hash function used for adding an element to a hll
func createHash(stream []byte) (uint64, error) {
	h := fnv.New64()
	_, err := h.Write(stream)
	if err != nil {
		return 0, errors.New("couldn't write data to hash function")
	}
	sum := h.Sum64()
	h.Reset()
	return sum, nil
}

/*
HLLToBytes serializes the HyperLogLog structure into a byte slice.
Returns:
- []byte: Serialized representation of the HyperLogLog structure.
- error: An error indicating any issues encountered during serialization.
*/
func (hll *HyperLogLog) HLLToBytes() ([]byte, error) {
	// Check if precision is within valid range
	if hll.P < HLL_MIN_PRECISION || hll.P > HLL_MAX_PRECISION {
		return nil, errors.New("illegal precision")
	}

	// Serialize M, P, and Reg into a byte slice
	data := make([]byte, HLL_HEADER_SIZE+len(hll.Reg))
	binary.LittleEndian.PutUint64(data[:HLL_M_SIZE], hll.M)
	data[HLL_M_SIZE] = hll.P
	copy(data[HLL_HEADER_SIZE:], hll.Reg)
	return data, nil
}

/*
BytesToHLL deserializes a byte slice into a HyperLogLog structure.
Parameters:
- data: Byte slice containing the serialized HyperLogLog structure.
Returns:
- error: An error indicating any issues encountered during deserialization.
*/
func (hll *HyperLogLog) BytesToHLL(data []byte) error {
	// Check if there is enough data to deserialize
	if len(data) < HLL_HEADER_SIZE {
		return errors.New("insufficient data for HyperLogLog structure")
	}

	// Deserialize M, P, and Reg from the byte slice
	hll.M = binary.LittleEndian.Uint64(data[:HLL_M_SIZE])
	hll.P = data[HLL_M_SIZE]
	hll.Reg = data[HLL_HEADER_SIZE:]

	return nil
}
