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
	PolyBase             string
	PageLimit            int
	MaxPages             int
	MaxPagesNoCheckpoint int
	Entities             []string
	OrderPrimary         string
	OrderFallback        string
	RequestTimeout       time.Duration
	ConnectTimeout       time.Duration
	MaxRetries           int
	BaseSleep            time.Duration

	InsertBatchSize        int
	InsertBatchSizeEvents  int
	InsertBatchSizeMarkets int
	InsertBatchSizeSeries  int

	IngestMode           string
	KafkaBrokers         []string
	KafkaUsername        string
	KafkaPassword        string
	KafkaTopic           string
	KafkaClientID        string
	KafkaBatchSize       int
	KafkaBatchBytes      int
	KafkaBatchTimeout    time.Duration
	KafkaWriteChunkSize  int
	KafkaMaxMessageBytes int
	KafkaMaxArrayItems   int
	ProducerSource       string
	ProducerIP           string

	Org              string
	OrchestratorRepo string
	DefaultBranch    string
	GHToken          string
	CheckpointPath   string
	StateBackend     string

	LookbackHours int

	RepoRoot string
}

func LoadConfig() (*Config, error) {
	repoRoot, err := FindRepoRoot()
	if err != nil {
		repoRoot, _ = os.Getwd()
	}

	batchSize := envInt("BATCH_SIZE", envInt("INSERT_BATCH_SIZE", 1000))
	cfg := &Config{
		PolyBase:             strings.TrimRight(envString("POLY_BASE", "https://gamma-api.polymarket.com"), "/"),
		PageLimit:            maxInt(1, envInt("PAGE_LIMIT", 100)),
		MaxPages:             maxInt(1, envInt("MAX_PAGES", 50)),
		MaxPagesNoCheckpoint: envInt("MAX_PAGES_NO_CHECKPOINT", 20),
		Entities:             normalizePolymarketEntities(splitCSV(envString("ENTITIES", "events,markets,series"))),
		OrderPrimary:         envString("ORDER_PRIMARY", "updatedAt"),
		OrderFallback:        envString("ORDER_FALLBACK", "id"),
		RequestTimeout:       time.Duration(maxInt(1, envInt("REQUEST_TIMEOUT", 30))) * time.Second,
		ConnectTimeout:       time.Duration(maxInt(1, envInt("CONNECT_TIMEOUT", envInt("CH_CONNECT_TIMEOUT", 10)))) * time.Second,
		MaxRetries:           maxInt(1, envInt("MAX_RETRIES", 6)),
		BaseSleep:            envFloatDuration("BASE_SLEEP", 0.2),

		InsertBatchSize:        maxInt(1, batchSize),
		InsertBatchSizeEvents:  maxInt(1, envInt("BATCH_SIZE_EVENTS", envInt("INSERT_BATCH_SIZE_EVENTS", batchSize))),
		InsertBatchSizeMarkets: maxInt(1, envInt("BATCH_SIZE_MARKETS", envInt("INSERT_BATCH_SIZE_MARKETS", batchSize))),
		InsertBatchSizeSeries:  maxInt(1, envInt("BATCH_SIZE_SERIES", envInt("INSERT_BATCH_SIZE_SERIES", minInt(batchSize, 50)))),

		IngestMode:           strings.ToLower(envString("INGEST_MODE", "kafka")),
		KafkaBrokers:         splitCSV(envString("KAFKA_BROKERS", "")),
		KafkaUsername:        firstNonEmpty(os.Getenv("KAFKA_USERNAME"), os.Getenv("KAFKA_EXTERNAL_USER")),
		KafkaPassword:        firstNonEmpty(os.Getenv("KAFKA_PASSWORD"), os.Getenv("KAFKA_EXTERNAL_PASSWORD")),
		KafkaTopic:           envString("KAFKA_TOPIC", "prediction.events"),
		KafkaClientID:        envString("KAFKA_CLIENT_ID", "statground-polymarket-crawler"),
		KafkaBatchSize:       maxInt(1, envInt("KAFKA_BATCH_SIZE", 1)),
		KafkaBatchBytes:      maxInt(65536, envInt("KAFKA_BATCH_BYTES", 262144)),
		KafkaBatchTimeout:    envFloatDuration("KAFKA_BATCH_TIMEOUT", 0.5),
		KafkaWriteChunkSize:  maxInt(1, envInt("KAFKA_WRITE_CHUNK_SIZE", envInt("KAFKA_BATCH_SIZE", 1))),
		KafkaMaxMessageBytes: maxInt(131072, envInt("KAFKA_MAX_MESSAGE_BYTES", 524288)),
		KafkaMaxArrayItems:   maxInt(0, envInt("KAFKA_MAX_ARRAY_ITEMS", 512)),
		ProducerSource:       envString("PRODUCER_SOURCE", "github_actions"),
		ProducerIP:           envString("PRODUCER_IP", "::"),

		Org:              envString("ORG", "statground"),
		OrchestratorRepo: envString("ORCHESTRATOR_REPO", "Statground_Data_Polymarket"),
		DefaultBranch:    envString("DEFAULT_BRANCH", "main"),
		GHToken:          firstNonEmpty(os.Getenv("GH_TOKEN"), os.Getenv("GITHUB_TOKEN")),
		CheckpointPath:   envString("CHECKPOINT_PATH", ".statground_state/polymarket_checkpoint.json"),
		StateBackend:     strings.ToLower(envString("STATE_BACKEND", "local")),

		LookbackHours: maxInt(1, envInt("LOOKBACK_HOURS", 72)),

		RepoRoot: repoRoot,
	}

	switch strings.ToLower(strings.TrimSpace(cfg.IngestMode)) {
	case "kafka", "kafka_clickhouse", "kafka-clickhouse", "event", "events":
		cfg.IngestMode = "kafka"
	default:
		return nil, fmt.Errorf("unsupported INGEST_MODE=%q; Statground Polymarket crawler now supports Kafka ingestion only", cfg.IngestMode)
	}
	if len(cfg.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("missing required env: KAFKA_BROKERS")
	}
	if strings.TrimSpace(cfg.KafkaTopic) == "" {
		return nil, fmt.Errorf("missing required env: KAFKA_TOPIC")
	}
	if len(cfg.Entities) == 0 {
		return nil, fmt.Errorf("ENTITIES must contain at least one of: events, markets, series")
	}

	return cfg, nil
}

func normalizePolymarketEntities(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, 3)
	for _, raw := range values {
		entity := strings.ToLower(strings.TrimSpace(raw))
		switch entity {
		case "event":
			entity = "events"
		case "market":
			entity = "markets"
		case "serie":
			entity = "series"
		}
		switch entity {
		case "events", "markets", "series":
			if !seen[entity] {
				seen[entity] = true
				out = append(out, entity)
			}
		}
	}
	return out
}

func joinCSV(values []string) string {
	return strings.Join(values, ",")
}

func (c *Config) Endpoint(entity string) string {
	return fmt.Sprintf("%s/%s", c.PolyBase, entity)
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

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
