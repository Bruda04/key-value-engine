package sstable

import (
	"encoding/json"
	"errors"
	"fmt"
	"key-value-engine/structs/iterator"
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
	nextIndex          int
	summaryFactor      int
	multipleFiles      bool
	compression        bool
	filterProbability  float64
	maxLSMLevels       int
	tablesToCompress   int    // when there's n sstables on the same level, compress them
	compressionTypeLSM string // size-tiered or leveled
	firstLeveledSize   uint64
	leveledInc         uint64
}

func MakeSSTable(summaryFactor int, multipleFiles bool, filterProbability float64, compress bool, maxLSMLevels int, tablesToCompress int, compressionType string, firstLeveledSize uint64, leveledInc uint64) (*SSTable, error) {
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
		nextIndex:          count,
		summaryFactor:      summaryFactor,
		multipleFiles:      multipleFiles,
		filterProbability:  filterProbability,
		compression:        compress,
		maxLSMLevels:       maxLSMLevels,
		tablesToCompress:   tablesToCompress,
		compressionTypeLSM: compressionType,
		firstLeveledSize:   firstLeveledSize,
		leveledInc:         leveledInc,
	}, nil
}

func (sst *SSTable) Get(key string) (*record.Record, error) {
	tiers, err := sst.getDirsByTier()
	if err != nil {
		return nil, err
	}

	// looping backwards
	for i := 0; i < len(tiers); i++ {
		for j := len(tiers[i]) - 1; j >= 0; j-- {
			subdir := tiers[i][j]

			found, err := sst.checkBf(key, subdir)
			if err != nil {
				return nil, err
			}

			if found != nil {
				return found, nil
			}
		}
	}

	return nil, nil
}

func (sst *SSTable) Flush(data []*record.Record) error {
	// making directory for SSTable
	dirPath := SUBDIR + "C1_SST_" + fmt.Sprintf("%d", sst.nextIndex)
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
			return errors.New("error converting to json")

		}

		err = os.WriteFile(dirPath+string(os.PathSeparator)+GLOBALDICTNAME, marshalled, 0644)
		if err != nil {
			return errors.New("error writting to json")
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

	err = sst.Compress()
	if err != nil {
		return err
	}
	return nil
}

// --------------------------FOR ITERATORS
func (sst *SSTable) GetSSTRangeIterators(minRange, maxRange string) []iterator.Iterator {
	var sstIterators []iterator.Iterator

	for _, path := range sst.getIteratorDirs() {
		sstIterators = append(sstIterators, sst.NewSSTRangeIterator(minRange, maxRange, path))
	}

	return sstIterators
}

func (sst *SSTable) GetSSTPrefixIterators(prefix string) []iterator.Iterator {
	var sstIterators []iterator.Iterator

	for _, path := range sst.getIteratorDirs() {
		sstIterators = append(sstIterators, sst.NewSSTPrefixIterator(prefix, path))
	}

	return sstIterators
}

func (sst *SSTable) getIteratorDirs() []string {
	var singleSSTPath []string

	tiers, err := sst.getDirsByTier()
	if err != nil {
		return nil
	}

	// looping backwards
	for i := 0; i < len(tiers); i++ {
		for j := len(tiers[i]) - 1; j >= 0; j-- {
			subdir := tiers[i][j]

			singleSSTPath = append(singleSSTPath, subdir)

		}
	}

	return singleSSTPath
}
