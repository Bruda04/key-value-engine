package simHash

import (
	"crypto/md5"
	"fmt"
)

func getHashAsString(data []byte) string {
	hash := md5.Sum(data)
	res := ""
	for _, b := range hash {
		res += fmt.Sprintf("%08b", b)
	}
	return res
}
