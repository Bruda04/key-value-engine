package simHash

import (
	"crypto/md5"
	"fmt"
)

/*
getHashAsString calculates the MD5 hash of the input data and returns it as a string of binary digits.

Parameters:
  - data: Input byte slice to be hashed.

Returns:
  - string: The MD5 hash value represented as a string of binary digits.
*/
func getHashAsString(data []byte) string {
	hash := md5.Sum(data)
	res := ""
	for _, b := range hash {
		res += fmt.Sprintf("%08b", b)
	}
	return res
}
