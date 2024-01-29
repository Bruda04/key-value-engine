package sstable

import (
	"encoding/binary"
	"fmt"
	"key-value-engine/structs/record"
	"os"
	"strings"
)

func getSubdirs(directory string) ([]string, error) {
	// opening direcotry
	dir, err := os.Open(directory)
	if err != nil {
		return nil, fmt.Errorf("error opening sstable direcotry: %s\n", err)
	}
	defer dir.Close()

	// reading content of direcotry
	entries, err := dir.Readdir(0)
	if err != nil {
		return nil, fmt.Errorf("error reading directories: %s\n", err)
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
		return err
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
		return err
	}
	return nil
}

func readTOC(dirPath string) ([]string, error) {
	content, err := os.ReadFile(dirPath + string(os.PathSeparator) + TOCNAME)
	if err != nil {
		return nil, err
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
