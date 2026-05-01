package polymarket

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type Ingestor struct {
	cfg   *Config
	api   *HTTPJSONClient
	state StateStore
}

type FetchResult struct {
	TotalWritten  int
	NewCheckpoint string
}

func NewIngestor(cfg *Config) (*Ingestor, error) {
	return &Ingestor{
		cfg:   cfg,
		api:   NewHTTPJSONClient(cfg.RequestTimeout, cfg.ConnectTimeout, "statground-polymarket-crawler"),
		state: NewStateStore(cfg),
	}, nil
}

func (i *Ingestor) PickOrder(ctx context.Context, entity string) (string, error) {
	url := i.cfg.Endpoint(entity)
	orders := []string{i.cfg.OrderPrimary, i.cfg.OrderFallback}
	for _, order := range orders {
		data, _, err := i.api.SafeGetJSON(ctx, url, map[string]string{
			"limit":     fmt.Sprint(i.cfg.PageLimit),
			"offset":    "0",
			"order":     order,
			"ascending": "false",
		}, i.cfg.MaxRetries, i.cfg.BaseSleep)
		if err != nil {
			continue
		}
		items := ExtractItems(data, entity)
		if len(items) > 0 || data != nil {
			return order, nil
		}
	}
	return "", fmt.Errorf("failed to fetch %s: API returned non-list for both orders", entity)
}

func (i *Ingestor) FetchAndPublish(ctx context.Context, entity string, checkpoint map[string]string) (FetchResult, error) {
	lastCP := checkpoint[entity]
	bestSeen := lastCP

	orderUsed, err := i.PickOrder(ctx, entity)
	if err != nil {
		return FetchResult{}, err
	}
	fmt.Printf("[FETCH] %s order=%s checkpoint=%s\n", entity, orderUsed, defaultDisplay(lastCP, "(none)"))

	totalWritten := 0
	rows := make([]map[string]any, 0, i.cfg.InsertBatchSizeForEntity(entity))
	url := i.cfg.Endpoint(entity)
	effectiveMaxPages := i.cfg.MaxPages
	if strings.TrimSpace(lastCP) == "" && i.cfg.MaxPagesNoCheckpoint > 0 {
		effectiveMaxPages = minInt(effectiveMaxPages, i.cfg.MaxPagesNoCheckpoint)
		fmt.Printf("[BOOTSTRAP-CAP] %s checkpoint is empty; max_pages capped to %d by MAX_PAGES_NO_CHECKPOINT to keep GitHub Actions bounded. Override only for manual backfill.\n", entity, effectiveMaxPages)
	}

	for page := 0; page < effectiveMaxPages; page++ {
		offset := page * i.cfg.PageLimit
		data, meta, err := i.api.SafeGetJSON(ctx, url, map[string]string{
			"limit":     fmt.Sprint(i.cfg.PageLimit),
			"offset":    fmt.Sprint(offset),
			"order":     orderUsed,
			"ascending": "false",
		}, i.cfg.MaxRetries, i.cfg.BaseSleep)
		if err != nil {
			fmt.Printf("[STOP] %s fetch failed at offset=%d err=%v\n", entity, offset, err)
			break
		}
		items := ExtractItems(data, entity)
		if len(items) == 0 {
			break
		}

		stop := false
		collectedAt := wrapTime(UTCNow())
		for _, obj := range items {
			updated := SafeString(firstNonNil(obj["updatedAt"], obj["updated_at"]))
			if lastCP != "" && stringsEqualFold(orderUsed, "updatedAt") && ISOLEQ(updated, lastCP) {
				stop = true
				break
			}
			rawKey, err := UUIDv7()
			if err != nil {
				return FetchResult{}, err
			}
			row, err := BuildEntityRow(entity, NormalizeRawJSON(obj), collectedAt, rawKey)
			if err != nil {
				return FetchResult{}, err
			}
			if row != nil {
				rows = append(rows, row)
				totalWritten++
			}
			bestSeen = MaxTimeISO(bestSeen, updated)
		}

		if err := i.FlushEntityRows(ctx, entity, &rows, false); err != nil {
			return FetchResult{}, err
		}

		fmt.Printf("[%s] page=%d offset=%d published=%d http_status=%d\n", entity, page+1, offset, totalWritten, meta.HTTPStatus)
		if stop {
			fmt.Printf("[%s] reached checkpoint -> stop\n", entity)
			break
		}
		if len(items) < i.cfg.PageLimit {
			break
		}
		if err := SleepContext(ctx, i.cfg.BaseSleep); err != nil {
			return FetchResult{}, err
		}
	}

	if err := i.FlushEntityRows(ctx, entity, &rows, true); err != nil {
		return FetchResult{}, err
	}

	return FetchResult{TotalWritten: totalWritten, NewCheckpoint: bestSeen}, nil
}

func RunIngest() error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	if err := EnsureStatePath(cfg); err != nil {
		return err
	}
	ingestor, err := NewIngestor(cfg)
	if err != nil {
		return err
	}

	fmt.Printf("[CONFIG] ingest_mode=%s kafka_topic=%s entities=%s max_pages=%d max_pages_no_checkpoint=%d batch_size_default=%d batch_size_events=%d batch_size_markets=%d batch_size_series=%d kafka_batch_size=%d kafka_write_chunk_size=%d kafka_batch_bytes=%d kafka_max_message_bytes=%d kafka_max_array_items=%d\n",
		cfg.IngestMode,
		cfg.KafkaTopic,
		joinCSV(cfg.Entities),
		cfg.MaxPages,
		cfg.MaxPagesNoCheckpoint,
		cfg.InsertBatchSize,
		cfg.InsertBatchSizeForEntity("events"),
		cfg.InsertBatchSizeForEntity("markets"),
		cfg.InsertBatchSizeForEntity("series"),
		cfg.KafkaBatchSize,
		cfg.KafkaWriteChunkSize,
		cfg.KafkaBatchBytes,
		cfg.KafkaMaxMessageBytes,
		cfg.KafkaMaxArrayItems,
	)

	ctx := context.Background()
	if err := ingestor.ValidateKafkaIngest(ctx); err != nil {
		return err
	}
	checkpoint, err := ingestor.state.LoadCheckpoint(ctx)
	if err != nil {
		return err
	}
	newCheckpoint := CopyMapStringString(checkpoint)
	if newCheckpoint == nil {
		newCheckpoint = map[string]string{}
	}

	wroteTotal := 0
	for _, entity := range cfg.Entities {
		res, err := ingestor.FetchAndPublish(ctx, entity, checkpoint)
		if err != nil {
			return err
		}
		wroteTotal += res.TotalWritten
		if res.NewCheckpoint != "" {
			newCheckpoint[entity] = res.NewCheckpoint
			if err := ingestor.state.SaveCheckpoint(ctx, newCheckpoint); err != nil {
				return err
			}
			if err := ingestor.PublishCheckpoint(ctx, newCheckpoint); err != nil {
				return err
			}
			checkpoint = CopyMapStringString(newCheckpoint)
		}
	}

	if err := ingestor.state.SaveCheckpoint(ctx, newCheckpoint); err != nil {
		return err
	}
	fmt.Printf("\nDONE. published_objects=%d\n", wroteTotal)
	return nil
}

func defaultDisplay(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func firstNonNil(values ...any) any {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

func stringsEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return strings.ToLower(a) == strings.ToLower(b)
}

func toISO(t time.Time) string {
	return FormatISO8601UTC(t)
}
