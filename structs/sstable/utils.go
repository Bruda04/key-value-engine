package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"key-value-engine/structs/record"
	"os"
	"sort"
	"strconv"
	"strings"
)

func getSubdirs(directory string) ([]string, error) {
	// opening direcotry
	dir, err := os.Open(directory)
	if err != nil {
		return nil, errors.New("error opening sstable direcotry")
	}
	defer dir.Close()

	// reading content of direcotry
	entries, err := dir.Readdir(0)
	if err != nil {
		return nil, errors.New("error reading directories")
	}

	var subdirs []string

	// adding subdirecories
	for _, entry := range entries {
		if entry.IsDir() {
			subdirs = append(subdirs, entry.Name())
		}
	}

	return subdirs, nil
}

func (sst *SSTable) makeTOC(dirPath string, multipleFiles bool) error {
	file, err := os.Create(dirPath + string(os.PathSeparator) + TOCNAME)
	if err != nil {
		return errors.New("error opening sstable direcotry")
	}
	defer file.Close()

	csvData := ""
	if multipleFiles {
		csvData = fmt.Sprintf("%s,%s,%s,%s,%s", DATANAME, INDEXNAME, SUMMARYNAME, BLOOMNAME, MERKLENAME)

	} else {
		csvData = fmt.Sprintf("%s", SINGLEFILENAME)
	}

	_, err = file.WriteString(csvData)
	if err != nil {
		return errors.New("error writting to sst file")
	}
	return nil
}

func readTOC(dirPath string) ([]string, error) {
	content, err := os.ReadFile(dirPath + string(os.PathSeparator) + TOCNAME)
	if err != nil {
		return nil, errors.New("error reading sstable file")
	}

	line := string(content)

	return strings.Split(line, ","), nil
}

func (sst *SSTable) indexFormatToBytes(rec *record.Record, offset int) []byte {
	// serializing key size
	keySizeBytes := make([]byte, record.KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(keySizeBytes, rec.GetKeySize())

	// serializing key
	keyBytes := []byte(rec.GetKey())

	// serilizing offset
	offsetBytes := make([]byte, OFFSETSIZE)
	binary.LittleEndian.PutUint64(offsetBytes, uint64(offset))

	result := append(keySizeBytes, keyBytes...)
	result = append(result, offsetBytes...)

	return result
}

func (sst *SSTable) getDirsByTier() ([][]string, error) {
	subdirs, err := getSubdirs(DIRECTORY)
	if err != nil {
		return nil, errors.New("error opening sstable direcotry")
	}

	// Map to store directories grouped by their tier number
	dirnamesByTierMap := make(map[int][]string)

	for i := 0; i < len(subdirs); i++ {
		subdir := subdirs[i] // C1_SST1 for example
		var strIndex []byte

		for j := 1; j < len(subdir) && subdir[j] != '_'; j++ {
			strIndex = append(strIndex, subdir[j])
		}

		resultString := string(strIndex)

		// Convert the string to an integer
		tierNumber, _ := strconv.Atoi(resultString)

		// Check if the tier number is greater than zero
		if tierNumber > 0 {
			// Append the current subdir to the appropriate tier in the map
			dirnamesByTierMap[tierNumber] = append(dirnamesByTierMap[tierNumber], DIRECTORY+string(os.PathSeparator)+subdir+string(os.PathSeparator))
		}
	}

	// Convert the map to a sorted slice of slices
	var dirnamesByTier [][]string
	var sortedTiers []int
	for tier := range dirnamesByTierMap {
		sortedTiers = append(sortedTiers, tier)
	}
	sort.Ints(sortedTiers)

	for _, tier := range sortedTiers {
		dirnamesByTier = append(dirnamesByTier, dirnamesByTierMap[tier])
	}
	return dirnamesByTier, nil
}
