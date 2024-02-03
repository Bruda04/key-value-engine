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
	DEFAULT_MEMTABLECOUNT       = 5
	DEFAULT_MEMTABLESTRUCT      = "btree"
	DEFAULT_SKIPLISTMAXHEIGHT   = 20
	DEFAULT_BTREEDEGREE         = 4
	DEFAULT_CACHESIZE           = 5
	DEFAULT_FILESSST            = true
	DEFAULT_SUMMARYINDEXDENSITY = 5
	DEFAULT_DO_COMPRESSION      = false
	DEFAULT_MAXLSMLEVELS        = 10
	DEFAULT_TABLESTOCOMPRESS    = 5
	DEFAULT_COMPRESSIONTYPE     = "size-tiered"
	DEFAULT_TOKENCAPACITY       = 10
	DEFAULT_REFILLCOOLDOWN      = 60
	DEFAULT_FILTERPRECISION     = 0.1
)

type Config struct {
	WalSize             uint64  `json:"wal_size"`
	MemtableSize        uint64  `json:"memtable_size"`
	MemtableCount       uint64  `json:"memtable_count"`
	MemtableStructure   string  `json:"memtable_structure"`
	BTreeDegree         uint64  `json:"btree_degree"`
	SkipListMaxHeight   uint64  `json:"skip_list_max_height"`
	CacheSize           uint64  `json:"cahce_size"`
	MultipleFilesSST    bool    `json:"separate_sst_files"`
	SummaryIndexDensity uint64  `json:"summary_index_density"`
	Compress            bool    `json:"do_compression"`
	CompressionType     string  `json:"compression_type"`
	MaxLsmLevels        uint64  `json:"max_lsm_levels"`
	TablesToCompress    uint64  `json:"tables_to_compress"`
	TokenCapacity       uint64  `json:"token_capacity"`
	RefillCooldown      uint64  `json:"refill_cooldown"`
	FilterPrecsion      float64 `json:"filter_precsion"`
}

func MakeConfig() (*Config, error) {
	var cfg Config

	cfg = Config{
		WalSize:             DEFAULT_WALSIZE,
		MemtableSize:        DEFAULT_MEMTABLESIZE,
		MemtableCount:       DEFAULT_MEMTABLECOUNT,
		MemtableStructure:   DEFAULT_MEMTABLESTRUCT,
		BTreeDegree:         DEFAULT_BTREEDEGREE,
		SkipListMaxHeight:   DEFAULT_SKIPLISTMAXHEIGHT,
		CacheSize:           DEFAULT_CACHESIZE,
		MultipleFilesSST:    DEFAULT_FILESSST,
		SummaryIndexDensity: DEFAULT_SUMMARYINDEXDENSITY,
		Compress:            DEFAULT_DO_COMPRESSION,
		CompressionType:     DEFAULT_COMPRESSIONTYPE,
		MaxLsmLevels:        DEFAULT_MAXLSMLEVELS,
		TablesToCompress:    DEFAULT_TABLESTOCOMPRESS,
		TokenCapacity:       DEFAULT_TOKENCAPACITY,
		RefillCooldown:      DEFAULT_REFILLCOOLDOWN,
		FilterPrecsion:      DEFAULT_FILTERPRECISION,
	}

	if _, err := os.Stat(CONFIG_DIR); os.IsNotExist(err) {
		if err := os.MkdirAll(CONFIG_DIR, 0755); err != nil {
			return &cfg, fmt.Errorf("error creating conf directory: %s", err)
		}
	}

	if _, err := os.Stat(CONFIG_PATH); !os.IsNotExist(err) {
		configData, err := os.ReadFile(CONFIG_PATH)
		if err != nil {
			return &cfg, fmt.Errorf("error reading file: %s", err)
		}

		err = json.Unmarshal(configData, &cfg)
		if err != nil {
			err := cfg.writeConfig()
			if err != nil {
				return &cfg, err
			}
			return &cfg, fmt.Errorf("error converting json to config: %s", err)
		}

		cfg.validate()

	}

	err := cfg.writeConfig()
	if err != nil {
		return nil, err
	}

	return &cfg, nil

}

func (cfg *Config) validate() {
	if cfg.WalSize < 0 {
		cfg.WalSize = DEFAULT_WALSIZE
	}

	if cfg.MemtableSize < 0 {
		cfg.MemtableSize = DEFAULT_MEMTABLESIZE
	}

	if cfg.MemtableCount < 2 || cfg.MemtableCount > 10 {
		cfg.MemtableCount = DEFAULT_MEMTABLECOUNT
	}

	if cfg.MemtableStructure != "btree" && cfg.MemtableStructure != "skiplist" && cfg.MemtableStructure != "hashmap" {
		cfg.MemtableStructure = DEFAULT_MEMTABLESTRUCT
	}

	if cfg.BTreeDegree < 4 {
		cfg.BTreeDegree = DEFAULT_BTREEDEGREE
	}

	if cfg.SkipListMaxHeight < 16 || cfg.SkipListMaxHeight > 32 {
		cfg.SkipListMaxHeight = DEFAULT_SKIPLISTMAXHEIGHT
	}

	if cfg.CacheSize < 5 {
		cfg.CacheSize = DEFAULT_CACHESIZE
	}

	if cfg.SummaryIndexDensity < 2 {
		cfg.SummaryIndexDensity = DEFAULT_SUMMARYINDEXDENSITY
	}

	if cfg.CompressionType != "size-tiered" && cfg.CompressionType != "leveled" {
		cfg.CompressionType = DEFAULT_COMPRESSIONTYPE
	}

	if cfg.MaxLsmLevels < 2 || cfg.MaxLsmLevels > 20 {
		cfg.MaxLsmLevels = DEFAULT_MAXLSMLEVELS
	}

	if cfg.TablesToCompress < 2 || cfg.TablesToCompress > 8 {
		cfg.TablesToCompress = DEFAULT_TABLESTOCOMPRESS
	}

	if cfg.TokenCapacity < 2 || cfg.TokenCapacity > 100 {
		cfg.TokenCapacity = DEFAULT_TOKENCAPACITY
	}

	if cfg.RefillCooldown < 1 || cfg.RefillCooldown > 600 {
		cfg.RefillCooldown = DEFAULT_REFILLCOOLDOWN
	}

	if cfg.FilterPrecsion < 0.01 || cfg.FilterPrecsion > 0.5 {
		cfg.FilterPrecsion = DEFAULT_FILTERPRECISION
	}
}

func (cfg *Config) writeConfig() error {
	// Marshal the modified config back to JSON
	marshalled, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("error converting config to json: %s", err)

	}

	// Write the JSON data to the file
	err = os.WriteFile(CONFIG_PATH, marshalled, 0644)
	if err != nil {
		return fmt.Errorf("error writing config to file: %s", err)
	}

	return nil
}
