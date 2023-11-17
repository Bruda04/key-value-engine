package simHash

import (
	"math"
	"regexp"
)

// HASHLEN is length of hash function output in bits
const HASHLEN = 128

// STOPPERWORDS is a predefined list of common stopper words.
var STOPPERWORDS = []string{
	"a", "an", "the", "and", "but", "or", "for", "nor", "on", "at", "to", "from", "by", "with",
	"about", "above", "across", "after", "against", "along", "amid", "among", "around", "as",
	"before", "behind", "below", "beneath", "beside", "between", "beyond", "but", "concerning",
	"considering", "despite", "down", "during", "except", "for", "from", "in", "inside", "into",
	"like", "near", "of", "off", "on", "onto", "out", "over", "past", "regarding", "round", "since",
	"through", "to", "toward", "under", "underneath", "until", "unto", "up", "upon", "with", "within",
	"without",
}

/*
	isStopperWord checks if the given word is a stopper word by comparing it to a predefined list.

Parameters:
- word: The word to be checked.

Returns:
- bool: True if the word is a stopper word, otherwise false.
*/
func isStopperWord(word string) bool {
	for _, sw := range STOPPERWORDS {
		if sw == word {
			return true
		}
	}

	return false
}

/*
	getRepeatingMap counts the occurrences of each non-stopper word in the input slice.

Parameters:
  - words: Slice of strings representing the words in the text.

Returns:
  - map[string]int: A map where keys are words, and values are their respective counts.
*/
func getRepeatingMap(words []string) map[string]int {
	ret := make(map[string]int)

	for _, word := range words {
		if !isStopperWord(word) {
			ret[word]++
		}
	}

	return ret
}

/*
	getWordsHashs generates hash values for each word in the input slice, excluding stopper words.

Parameters:
  - words: Slice of strings representing the words in the text.

Returns:
  - map[string]string: A map where keys are words, and values are their corresponding hash values as strings.
*/
func getWordsHashs(words []string) map[string]string {
	ret := make(map[string]string)

	for _, w := range words {
		if !isStopperWord(w) {
			ret[w] = getHashAsString([]byte(w))
		}

	}

	return ret
}

/*
	getColumnSum calculates the column sums based on the provided word count and word hash values.

Parameters:
  - wordCount: A map containing the count of occurrences for each word in the text.
  - wordHashs: A map containing hash values for each word.

Returns:
  - []int: Slice of integers representing the calculated column sums.
*/
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

/*
	getTextFingerprint generates the text fingerprint based on the provided column sums.

Parameters:
  - columnSum: Slice of integers representing the column sums.

Returns:
  - uint: Text fingerprint value.
*/
func getTextFingerprint(columnSum []int) uint {
	var ret uint

	for i := range columnSum {
		if columnSum[i] >= 0 {
			ret += uint(math.Pow(2, float64(HASHLEN-i)))
		}
	}

	return ret
}

/*
	SimHash calculates the SimHash value of the input text

Parameters:
  - text: Input byte slice representing the text to calculate the SimHash for.

Returns:
  - uint: SimHash value for the input text.

Note:
  - SimHash removes every stopper word.
*/
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
