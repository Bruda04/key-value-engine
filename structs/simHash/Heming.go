package simHash

/*
	HammingDistance calculates the Hamming distance between two uint values.

Parameters:
  - a: The first uint value.
  - b: The second uint value.

Returns:
  - uint: The Hamming distance between the two input uint values.
*/
func HemingDistance(a uint, b uint) uint {
	or := a ^ b
	count := uint(0)

	for or > 0 {
		count += or & 1
		or >>= 1
	}

	return count
}
