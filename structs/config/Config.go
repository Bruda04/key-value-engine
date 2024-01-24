package config

import (
	"encoding/json"
	"fmt"
	"os"
)

const (
	CONFIG_DIR  = "conf"
	CONFIG_PATH = "conf" + string(os.PathSeparator) + "config.json"

	DEFAULT_WALSIZE        = 500
	DEFAULT_MEMTABLESIZE   = 1000
	DEFAULT_MEMTABLESTRUCT = "btree"
	DEFAULT_BTREEDEGREE    = 4
	DEFAULT_CACHESIZE      = 50
)

type Config struct {
	WalSize           uint64 `json:"wal_size"`
	MemtableSize      uint64 `json:"memtable_size"`
	MemtableStructure string `json:"memtable_structure"`
	BTreeDegree       uint64 `json:"btree_degree"`
	CacheSize         uint64 `json:"cahce_size"`
}

func MakeConfig() (*Config, error) {
	if _, err := os.Stat(CONFIG_DIR); os.IsNotExist(err) {
		if err := os.MkdirAll(CONFIG_DIR, 0755); err != nil {
			return nil, fmt.Errorf("error creating conf directory: %s", err)
		}
	}

	if _, err := os.Stat(CONFIG_PATH); os.IsNotExist(err) {
		cfg := Config{
			WalSize:           DEFAULT_WALSIZE,
			MemtableSize:      DEFAULT_MEMTABLESIZE,
			MemtableStructure: DEFAULT_MEMTABLESTRUCT,
			BTreeDegree:       DEFAULT_BTREEDEGREE,
			CacheSize:         DEFAULT_CACHESIZE,
		}

		// Marshal the modified config back to JSON
		marshalled, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("error converting config to json: %s", err)

		}

		// Write the JSON data to the file
		err = os.WriteFile(CONFIG_PATH, marshalled, 0644)
		if err != nil {
			return nil, fmt.Errorf("error writing config to file: %s", err)
		}

		return &cfg, nil
	} else {
		var cfg Config

		configData, err := os.ReadFile(CONFIG_PATH)
		if err != nil {
			return nil, fmt.Errorf("error reading file: %s", err)
		}

		err = json.Unmarshal(configData, &cfg)
		if err != nil {
			return nil, fmt.Errorf("error converting json to config: %s", err)
		}

		return &cfg, nil
	}
}
