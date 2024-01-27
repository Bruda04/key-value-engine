package config

import (
	"encoding/json"
	"fmt"
	"os"
)

const (
	CONFIG_DIR  = "conf"
	CONFIG_PATH = "conf" + string(os.PathSeparator) + "config.json"

	DEFAULT_WALSIZE             = 20
	DEFAULT_MEMTABLESIZE        = 20
	DEFAULT_MEMTABLESTRUCT      = "btree"
	DEFAULT_SKIPLISTMAXHEIGHT   = 32
	DEFAULT_BTREEDEGREE         = 4
	DEFAULT_CACHESIZE           = 5
	DEFAULT_SUMMARYINDEXDENSITY = 5
)

type Config struct {
	WalSize             uint64 `json:"wal_size"`
	MemtableSize        uint64 `json:"memtable_size"`
	MemtableStructure   string `json:"memtable_structure"`
	BTreeDegree         uint64 `json:"btree_degree"`
	SkipListMaxHeight   uint64 `json:"skip_list_max_height"`
	CacheSize           uint64 `json:"cahce_size"`
	SummaryIndexDensity uint64 `json:"summary_index_density"`
}

/*
MakeConfig creates a new Config instance with default values or loads an existing
configuration from a file. The function performs the following steps:

Returns:
  - *Config: Pointer to the Config instance representing the configuration.
  - error: An error, if any, encountered during the process.
*/
func MakeConfig() (*Config, error) {
	var cfg Config

	cfg = Config{
		WalSize:             DEFAULT_WALSIZE,
		MemtableSize:        DEFAULT_MEMTABLESIZE,
		MemtableStructure:   DEFAULT_MEMTABLESTRUCT,
		BTreeDegree:         DEFAULT_BTREEDEGREE,
		SkipListMaxHeight:   DEFAULT_SKIPLISTMAXHEIGHT,
		CacheSize:           DEFAULT_CACHESIZE,
		SummaryIndexDensity: DEFAULT_SUMMARYINDEXDENSITY,
	}

	if _, err := os.Stat(CONFIG_DIR); os.IsNotExist(err) {
		if err := os.MkdirAll(CONFIG_DIR, 0755); err != nil {
			return &cfg, fmt.Errorf("error creating conf directory: %s", err)
		}
	}

	if _, err := os.Stat(CONFIG_PATH); os.IsExist(err) {
		configData, err := os.ReadFile(CONFIG_PATH)
		if err != nil {
			return &cfg, fmt.Errorf("error reading file: %s", err)
		}

		err = json.Unmarshal(configData, &cfg)
		if err != nil {
			return &cfg, fmt.Errorf("error converting json to config: %s", err)
		}

		cfg.validate()

	}

	// Marshal the modified config back to JSON
	marshalled, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return &cfg, fmt.Errorf("error converting config to json: %s", err)

	}

	// Write the JSON data to the file
	err = os.WriteFile(CONFIG_PATH, marshalled, 0644)
	if err != nil {
		return &cfg, fmt.Errorf("error writing config to file: %s", err)
	}

	return &cfg, nil
}

/*
validate performs validation on the given Config object and adjusts
any invalid or missing values to their default counterparts.
*/
func (cfg *Config) validate() {
	if cfg.WalSize < 0 {
		cfg.WalSize = DEFAULT_WALSIZE
	}

	if cfg.MemtableSize < 0 {
		cfg.MemtableSize = DEFAULT_MEMTABLESIZE
	}

	if cfg.MemtableStructure != "btree" && cfg.MemtableStructure != "skiplist" && cfg.MemtableStructure != "hashmap" {
		cfg.MemtableStructure = DEFAULT_MEMTABLESTRUCT
	}

	if cfg.BTreeDegree < 4 {
		cfg.BTreeDegree = DEFAULT_BTREEDEGREE
	}

	if cfg.SkipListMaxHeight < 32 {
		cfg.SkipListMaxHeight = DEFAULT_SKIPLISTMAXHEIGHT
	}

	if cfg.CacheSize < 5 {
		cfg.CacheSize = DEFAULT_CACHESIZE
	}

	if cfg.SummaryIndexDensity < 2 {
		cfg.SummaryIndexDensity = DEFAULT_SUMMARYINDEXDENSITY
	}
}
