package polymarket

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	PolyBase       string
	PageLimit      int
	MaxPages       int
	OrderPrimary   string
	OrderFallback  string
	RequestTimeout time.Duration
	MaxRetries     int
	BaseSleep      time.Duration

	CHConnectTimeout        time.Duration
	CHSendReceiveTimeout    time.Duration
	CHQueryRetries          int
	InsertBatchSize         int
	InsertBatchSizeEvents   int
	InsertBatchSizeMarkets  int
	InsertBatchSizeSeries   int
	InsertMaxRetries        int
	InsertRetryBaseSleep    time.Duration
	InsertSplitAfterAttempt int
	InsertMinSplitBatchRows int

	InsertMaxPartitionsPerBlock        int
	InsertThrowOnMaxPartitionsPerBlock bool

	Org              string
	OrchestratorRepo string
	DefaultBranch    string
	GHToken          string
	CheckpointPath   string

	CHHost                string
	CHPort                int
	CHUser                string
	CHPassword            string
	CHDatabase            string
	CHInterface           string
	CHScheme              string
	CHDateTimeInputFormat string

	EventTable  string
	MarketTable string
	SeriesTable string

	OptimizeAfterRun      bool
	OptimizeProb          float64
	OptimizePartitions    int
	OptimizeMaxPartitions int

	LookbackHours     int
	RefreshReportPath string

	StatsCommitMessage   string
	StatsPushMaxAttempts int

	RepoRoot string
}

func LoadConfig() (*Config, error) {
	repoRoot, err := FindRepoRoot()
	if err != nil {
		repoRoot, _ = os.Getwd()
	}

	insertBatchSize := envInt("INSERT_BATCH_SIZE", 1000)
	optimizePartitions := envInt("OPTIMIZE_PARTITIONS", 128)
	cfg := &Config{
		PolyBase:       strings.TrimRight(envString("POLY_BASE", "https://gamma-api.polymarket.com"), "/"),
		PageLimit:      maxInt(1, envInt("PAGE_LIMIT", 100)),
		MaxPages:       maxInt(1, envInt("MAX_PAGES", 200)),
		OrderPrimary:   envString("ORDER_PRIMARY", "updatedAt"),
		OrderFallback:  envString("ORDER_FALLBACK", "id"),
		RequestTimeout: time.Duration(maxInt(1, envInt("REQUEST_TIMEOUT", 30))) * time.Second,
		MaxRetries:     maxInt(1, envInt("MAX_RETRIES", 6)),
		BaseSleep:      envFloatDuration("BASE_SLEEP", 0.2),

		CHConnectTimeout:        time.Duration(maxInt(1, envInt("CH_CONNECT_TIMEOUT", 10))) * time.Second,
		CHSendReceiveTimeout:    time.Duration(maxInt(1, envInt("CH_SEND_RECEIVE_TIMEOUT", 900))) * time.Second,
		CHQueryRetries:          maxInt(0, envInt("CH_QUERY_RETRIES", 2)),
		InsertBatchSize:         maxInt(1, insertBatchSize),
		InsertBatchSizeEvents:   maxInt(1, envInt("INSERT_BATCH_SIZE_EVENTS", insertBatchSize)),
		InsertBatchSizeMarkets:  maxInt(1, envInt("INSERT_BATCH_SIZE_MARKETS", insertBatchSize)),
		InsertBatchSizeSeries:   maxInt(1, envInt("INSERT_BATCH_SIZE_SERIES", minInt(insertBatchSize, 200))),
		InsertMaxRetries:        maxInt(1, envInt("INSERT_MAX_RETRIES", 4)),
		InsertRetryBaseSleep:    envFloatDuration("INSERT_RETRY_BASE_SLEEP", 1.0),
		InsertSplitAfterAttempt: maxInt(1, envInt("INSERT_SPLIT_AFTER_ATTEMPT", 2)),
		InsertMinSplitBatchRows: maxInt(2, envInt("INSERT_MIN_SPLIT_BATCH_ROWS", 25)),

		InsertMaxPartitionsPerBlock:        maxInt(0, envInt("INSERT_MAX_PARTITIONS_PER_BLOCK", optimizePartitions)),
		InsertThrowOnMaxPartitionsPerBlock: envBool("INSERT_THROW_ON_MAX_PARTITIONS_PER_BLOCK", true),

		Org:              envString("ORG", "statground"),
		OrchestratorRepo: envString("ORCHESTRATOR_REPO", "Statground_Data_Polymarket"),
		DefaultBranch:    envString("DEFAULT_BRANCH", "main"),
		GHToken:          firstNonEmpty(os.Getenv("GH_TOKEN"), os.Getenv("GITHUB_TOKEN")),
		CheckpointPath:   envString("CHECKPOINT_PATH", ".state/polymarket_checkpoint.json"),

		CHHost:                os.Getenv("CLICKHOUSE_HOST"),
		CHPort:                envInt("CLICKHOUSE_PORT", 8123),
		CHUser:                envString("CLICKHOUSE_USER", "default"),
		CHPassword:            os.Getenv("CLICKHOUSE_PASSWORD"),
		CHDatabase:            envString("CLICKHOUSE_DATABASE", "statground_polymarket"),
		CHInterface:           strings.ToLower(envString("CLICKHOUSE_INTERFACE", "http")),
		CHScheme:              strings.ToLower(envString("CLICKHOUSE_SCHEME", "http")),
		CHDateTimeInputFormat: envString("CLICKHOUSE_DATE_TIME_INPUT_FORMAT", envString("CH_DATE_TIME_INPUT_FORMAT", "best_effort")),

		EventTable:  envString("EVENT_TABLE", "polymarket_event"),
		MarketTable: envString("MARKET_TABLE", "polymarket_market"),
		SeriesTable: envString("SERIES_TABLE", "polymarket_series"),

		OptimizeAfterRun:      envBool("OPTIMIZE_AFTER_RUN", true),
		OptimizeProb:          envFloat("OPTIMIZE_PROB", 0.15),
		OptimizePartitions:    maxInt(1, optimizePartitions),
		OptimizeMaxPartitions: maxInt(1, envInt("OPTIMIZE_MAX_PARTITIONS", 4)),

		LookbackHours:     maxInt(1, envInt("LOOKBACK_HOURS", 72)),
		RefreshReportPath: strings.TrimSpace(os.Getenv("REFRESH_REPORT_PATH")),

		StatsCommitMessage:   envString("STATS_COMMIT_MESSAGE", "chore(stats): update polymarket stats"),
		StatsPushMaxAttempts: maxInt(1, envInt("STATS_PUSH_MAX_ATTEMPTS", 4)),

		RepoRoot: repoRoot,
	}

	if cfg.CHInterface == "https" {
		cfg.CHScheme = "https"
		cfg.CHInterface = "http"
	}
	if cfg.CHScheme == "" {
		cfg.CHScheme = "http"
	}

	return cfg, nil
}

func (c *Config) Endpoint(entity string) string {
	return fmt.Sprintf("%s/%s", c.PolyBase, entity)
}

func (c *Config) EntityTable(entity string) string {
	switch entity {
	case "events":
		return c.EventTable
	case "markets":
		return c.MarketTable
	case "series":
		return c.SeriesTable
	default:
		return ""
	}
}

func (c *Config) EntityQualifiedTable(entity string) string {
	return fmt.Sprintf("%s.%s", c.CHDatabase, c.EntityTable(entity))
}

func (c *Config) InsertBatchSizeForEntity(entity string) int {
	switch entity {
	case "events":
		return c.InsertBatchSizeEvents
	case "markets":
		return c.InsertBatchSizeMarkets
	case "series":
		return c.InsertBatchSizeSeries
	default:
		return c.InsertBatchSize
	}
}

func (c *Config) CheckpointAbsPath() string {
	if filepath.IsAbs(c.CheckpointPath) {
		return c.CheckpointPath
	}
	return filepath.Join(c.RepoRoot, c.CheckpointPath)
}

func envString(key, defaultValue string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return defaultValue
}

func envInt(key string, defaultValue int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultValue
	}
	x, err := strconv.Atoi(v)
	if err != nil {
		return defaultValue
	}
	return x
}

func envFloat(key string, defaultValue float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultValue
	}
	x, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return defaultValue
	}
	return x
}

func envFloatDuration(key string, defaultSeconds float64) time.Duration {
	seconds := envFloat(key, defaultSeconds)
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

func envBool(key string, defaultValue bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return defaultValue
	}
	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return defaultValue
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
