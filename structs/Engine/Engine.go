package Engine

import (
	"key-value-engine/structs/config"
	cache "key-value-engine/structs/lruCache"
	"key-value-engine/structs/memtable"
	"key-value-engine/structs/sstable"
	"key-value-engine/structs/tokenBucket"
	"key-value-engine/structs/wal"
	"key-value-engine/structs/wputils"
)

type Engine struct {
	config           *config.Config
	tokenBucket      *tokenBucket.TokenBucket
	commitLog        *wal.WAL
	sst              *sstable.SSTable
	lruCache         *cache.LRUCache
	memMan           *memtable.MemManager
	walRestoreOffset int64
}

func MakeEngine() *Engine {
	cfg, err := config.MakeConfig()
	if err != nil {
		displayError(err)
	}

	tb := tokenBucket.MakeTokenBucket(int64(cfg.TokenCapacity), int64(cfg.RefillCooldown))

	commitLog, _ := wal.MakeWAL(int64(cfg.WalSize))

	sst, _ := sstable.MakeSSTable(
		int(cfg.SummaryIndexDensity),
		cfg.MultipleFilesSST,
		cfg.FilterPrecsion,
		cfg.Compress,
		int(cfg.MaxLsmLevels),
		int(cfg.TablesToCompress),
		cfg.CompressionType,
		cfg.FirstLeveledSize,
		cfg.LeveledInc,
	)

	lruCache := cache.NewLRUCache(int(cfg.CacheSize))

	memMan := memtable.MakeMemTableManager(int(cfg.MemtableCount), int(cfg.MemtableSize), cfg.MemtableStructure, sst)

	restore, err := wputils.Restore(memMan, commitLog)
	if err != nil {
		return nil
	}

	return &Engine{
		config:           cfg,
		tokenBucket:      tb,
		commitLog:        commitLog,
		sst:              sst,
		lruCache:         lruCache,
		memMan:           memMan,
		walRestoreOffset: restore,
	}
}

func (e *Engine) Main() {
	for {
		input := getInput()
		option := checkInput(input)

		if option == OPTION_INVALID {
			showValidOptions()
			pauseTerminal()
			continue
		}

		tokenBytes, err := e.tokenBucket.TakeToken(1)
		if err != nil {
			displayError(err)
			continue
		}
		e.logToken(tokenBytes)

		if option == OPTION_GET {
			e.get(input)
		} else if option == OPTION_PUT {
			e.put(input)
		} else if option == OPTION_DELETE {
			e.delete(input)
		} else if option == OPTION_EXIT {
			e.quit()
			return
		} else if option == OPTION_MAKE {
			e.makeStruct(input)
		} else if option == OPTION_DESTROY {
			e.destroy(input)
		} else if option == OPTION_ADDTOSTRUCT {
			e.populateStruct(input)
		} else if option == OPTION_CHECKSTRUCT {
			e.checkStruct(input)
		} else if option == OPTION_FINGERPRINT {
			e.storeFingerprint(input)
		} else if option == OPTION_SIMHASH {
			e.simhHash(input)
		} else if option == OPTION_PREFIXSCAN {
			e.prefixScan(input)
		} else if option == OPTION_RANGESCAN {
			e.rangeScan(input)
		} else if option == OPTION_PREFIXITER {
			e.prefixIterator(input)
		} else if option == OPTION_RANGEITER {
			e.rangeIterator(input)
		}
	}
}
