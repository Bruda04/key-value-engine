package Engine

import (
	"fmt"
	"key-value-engine/structs/scan"
	"regexp"
	"strings"
)

func (e *Engine) prefixIterator(call string) {
	parts := strings.Split(call, " ")
	prefix := parts[1]

	iter := scan.MakePrefixIterate(prefix, e.memMan, e.sst)

	nextRegex := regexp.MustCompile(NEXTREGEX)
	stopRegex := regexp.MustCompile(STOPREGEX)
	systemNameRegex := regexp.MustCompile(SYSTEMKEY)

	for {
		input := getInput()

		if stopRegex.MatchString(input) {
			return
		} else if nextRegex.MatchString(input) {
			next := iter.Next()

			for next != nil && systemNameRegex.MatchString(next.GetKey()) {
				next = iter.Next()
			}

			if next != nil {
				fmt.Printf("key: %s\tvalue: %s\n", next.GetKey(), next.GetValue())
			} else {
				fmt.Println("END")
				return
			}
		} else {
			showValidOptions()
			pauseTerminal()
		}
	}
}

func (e *Engine) rangeIterator(call string) {
	parts := strings.Split(call, " ")
	ranges := strings.Split(parts[1], "-")

	iter := scan.MakeRangeIterate(ranges[0], ranges[1], e.memMan, e.sst)

	nextRegex := regexp.MustCompile(NEXTREGEX)
	stopRegex := regexp.MustCompile(STOPREGEX)
	systemNameRegex := regexp.MustCompile(SYSTEMKEY)
	for {
		input := getInput()
		if stopRegex.MatchString(input) {
			return
		} else if nextRegex.MatchString(input) {
			next := iter.Next()

			for next != nil && systemNameRegex.MatchString(next.GetKey()) {
				next = iter.Next()
			}

			if next != nil {
				fmt.Printf("key: %s\t value:%s\n", next.GetKey(), next.GetValue())
			} else {
				fmt.Println("END")
				return
			}

		} else {
			showValidOptions()
			pauseTerminal()
		}
	}
}
