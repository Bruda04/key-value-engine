package simHash

import (
	"math"
	"regexp"
)

const HASHLEN = 128

var STOPPERWORDS = []string{
	"a", "an", "the", "and", "but", "or", "for", "nor", "on", "at", "to", "from", "by", "with",
	"about", "above", "across", "after", "against", "along", "amid", "among", "around", "as",
	"before", "behind", "below", "beneath", "beside", "between", "beyond", "but", "concerning",
	"considering", "despite", "down", "during", "except", "for", "from", "in", "inside", "into",
	"like", "near", "of", "off", "on", "onto", "out", "over", "past", "regarding", "round", "since",
	"through", "to", "toward", "under", "underneath", "until", "unto", "up", "upon", "with", "within",
	"without",
}

func isStopperWord(word string) bool {
	for _, sw := range STOPPERWORDS {
		if sw == word {
			return true
		}
	}

	return false
}

func getRepeatingMap(words []string) map[string]int {
	ret := make(map[string]int)

	for _, word := range words {
		if !isStopperWord(word) {
			ret[word]++
		}
	}

	return ret
}

func getWordsHashs(words []string) map[string]string {
	ret := make(map[string]string)

	for _, w := range words {
		if !isStopperWord(w) {
			ret[w] = getHashAsString([]byte(w))
		}

	}

	return ret
}

func getColumnSum(wordCount map[string]int, wordHashs map[string]string) []int {
	var ret []int
	for i := 0; i < HASHLEN; i++ {
		indexSum := 0
		for k, v := range wordHashs {
			indexVal := v[i]
			if indexVal == '1' {
				indexSum += wordCount[k] * 1
			} else if indexVal == '0' {
				indexSum += wordCount[k] * -1
			}
		}
		ret = append(ret, indexSum)
	}

	return ret
}

func getTextFingerprint(columnSum []int) uint {
	var ret uint

	for i := range columnSum {
		if columnSum[i] >= 0 {
			ret += uint(math.Pow(2, float64(HASHLEN-i)))
		}
	}

	return ret
}

func SimHash(text []byte) uint {
	textStr := string(text)

	re := regexp.MustCompile(`\b\w+\b`)
	words := re.FindAllString(textStr, -1)

	wordCount := getRepeatingMap(words)
	wordHashs := getWordsHashs(words)

	columnSum := getColumnSum(wordCount, wordHashs)

	ret := getTextFingerprint(columnSum)

	return uint(ret)
}
