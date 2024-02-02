package sstable

import (
	"encoding/json"
	"fmt"
	"key-value-engine/structs/record"
	"os"
)

const (
	DIRECTORY      = "data" + string(os.PathSeparator) + "sstable"
	SUBDIR         = DIRECTORY + string(os.PathSeparator)
	DATANAME       = "SST_Data.db"
	INDEXNAME      = "SST_Index.db"
	SUMMARYNAME    = "SST_Summary.db"
	BLOOMNAME      = "SST_Filter.db"
	TOCNAME        = "TOC.csv"
	MERKLENAME     = "SST_Merkle.db"
	GLOBALDICTNAME = "SST_Dict.json"
	SINGLEFILENAME = "SST.db"
	OFFSETSIZE     = 8
	HEADERSIZE     = 5 * OFFSETSIZE
)

type SSTable struct {
	nextIndex         int
	summaryFactor     int
	multipleFiles     bool
	compression       bool
	filterProbability float64
}

func MakeSSTable(summaryFactor int, multipleFiles bool, filterProbability float64, compress bool) (*SSTable, error) {
	if _, err := os.Stat(DIRECTORY); os.IsNotExist(err) {
		if err := os.MkdirAll(DIRECTORY, 0755); err != nil {
			return nil, fmt.Errorf("error creating sstable directory: %s", err)
		}
	}

	subdirs, err := getSubdirs(DIRECTORY)
	if err != nil {
		return nil, fmt.Errorf("error getting SST directories: %s\n", err)
	}

	count := len(subdirs) + 1

	return &SSTable{
		nextIndex:         count,
		summaryFactor:     summaryFactor,
		multipleFiles:     multipleFiles,
		filterProbability: filterProbability,
		compression:       compress,
	}, nil
}

func (sst *SSTable) Get(key string) (*record.Record, error) {
	subdirs, err := getSubdirs(DIRECTORY)
	if err != nil {
		return nil, err
	}

	// looping backwards
	for i := len(subdirs) - 1; i >= 0; i-- {
		subdir := subdirs[i]
		subdirPath := DIRECTORY + string(os.PathSeparator) + subdir + string(os.PathSeparator)

		found, err := sst.checkBf(key, subdirPath)
		if err != nil {
			return nil, err
		}

		if found != nil {
			return found, nil
		}

	}

	return nil, nil

}

func (sst *SSTable) Flush(data []*record.Record) error {
	// making directory for SSTable
	dirPath := SUBDIR + "SST_" + fmt.Sprintf("%d", sst.nextIndex)
	err := os.Mkdir(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error making SST direcory: %s\n", err)
	}
	sst.nextIndex++

	err = sst.makeTOC(dirPath, sst.multipleFiles)
	if err != nil {
		return err
	}

	if sst.compression {
		globalDict := make(map[string]int)
		marshalled, err := json.MarshalIndent(globalDict, "", "  ")
		if err != nil {
			return err

		}

		err = os.WriteFile(dirPath+string(os.PathSeparator)+GLOBALDICTNAME, marshalled, 0644)
		if err != nil {
			return err
		}
	}

	for _, rec := range data {
		err = sst.putData(rec, dirPath)
		if err != nil {
			return err
		}
	}

	err = sst.formIndex(dirPath)
	if err != nil {
		return err
	}
	err = sst.formSummary(dirPath)
	if err != nil {
		return err
	}
	err = sst.formBfMt(dirPath, len(data))
	if err != nil {
		return err
	}

	return nil
}
