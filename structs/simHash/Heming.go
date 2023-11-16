package simHash

func HemingDistance(prvi uint, drugi uint) uint {
	or := prvi ^ drugi
	count := uint(0)

	for or > 0 {
		count += or & 1
		or >>= 1
	}

	return count
}
