package sstable

import (
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
)

type SSTable struct {
	nextIndex         int
	summaryFactor     int
	multipleFiles     bool
	filterProbability float64
	compression       bool
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
		return nil, fmt.Errorf("error getting SST directories: %s\n", err)
	}

	// looping backwards
	for i := len(subdirs) - 1; i >= 0; i-- {
		subdir := subdirs[i]
		subdirPath := DIRECTORY + string(os.PathSeparator) + subdir + string(os.PathSeparator)

		files, _ := readTOC(DIRECTORY + string(os.PathSeparator) + subdir)

		if len(files) > 1 {
			if sst.compression {
				found, err := sst.checkMultipleComp(key, subdirPath)
				if err != nil {
					return nil, fmt.Errorf("error finding key: %s\n", err)
				}
				// if found return, otherwise continue search in next SST
				if found != nil {
					return found, nil
				}

			} else {
				found, err := sst.checkMultiple(key, subdirPath)
				if err != nil {
					return nil, fmt.Errorf("error finding key: %s\n", err)
				}
				// if found return, otherwise continue search in next SST
				if found != nil {
					return found, nil
				}
			}

		} else {
			if sst.compression {
				found, err := sst.checkSingleComp(key, subdirPath)
				if err != nil {
					return nil, fmt.Errorf("error finding key: %s\n", err)
				}

				// if found return, otherwise continue search in next SST
				if found != nil {
					return found, nil
				}
			} else {
				found, err := sst.checkSingle(key, subdirPath)
				if err != nil {
					return nil, fmt.Errorf("error finding key: %s\n", err)
				}

				// if found return, otherwise continue search in next SST
				if found != nil {
					return found, nil
				}
			}
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

	if sst.multipleFiles {
		// make Multiple files
		if sst.compression {
			err = sst.makeMultipleFilesComp(data, dirPath)
		} else {
			err = sst.makeMultipleFiles(data, dirPath)
		}
	} else {
		// make Single file
		if sst.compression {
			err = sst.makeSingleFileComp(data, dirPath)
		} else {
			err = sst.makeSingleFile(data, dirPath)
		}
	}

	return nil
}
