package Engine

import (
	"fmt"
	"key-value-engine/structs/scan"
	"regexp"
	"strconv"
	"strings"
)

func (e *Engine) prefixScan(call string) {
	parts := strings.Split(call, " ")
	prefix := parts[1]
	pageNumSTR := parts[2]
	pageSizeSTR := parts[3]

	pageNum, _ := strconv.ParseInt(pageNumSTR, 10, 64)
	pageSize, _ := strconv.ParseInt(pageSizeSTR, 10, 64)

	systemNameRegex := regexp.MustCompile(SYSTEMKEY)

	res := scan.PrefixScan(prefix, int(pageNum), int(pageSize), e.memMan, e.sst)

	for i, rec := range res {
		if systemNameRegex.MatchString(rec.GetKey()) {
			continue
		}
		fmt.Printf("%d. key: %s\tvalue: %s\n", i+1, rec.GetKey(), rec.GetValue())
	}
}

func (e *Engine) rangeScan(call string) {
	parts := strings.Split(call, " ")
	ranges := strings.Split(parts[1], "-")
	pageNumSTR := parts[2]
	pageSizeSTR := parts[3]

	pageNum, _ := strconv.ParseInt(pageNumSTR, 10, 64)
	pageSize, _ := strconv.ParseInt(pageSizeSTR, 10, 64)

	systemNameRegex := regexp.MustCompile(SYSTEMKEY)

	res := scan.RangeScan(ranges[0], ranges[1], int(pageNum), int(pageSize), e.memMan, e.sst)

	for i, rec := range res {
		if systemNameRegex.MatchString(rec.GetKey()) {
			continue
		}
		fmt.Printf("%d. key: %s\tvalue: %s\n", i+1, rec.GetKey(), rec.GetValue())
	}
}
