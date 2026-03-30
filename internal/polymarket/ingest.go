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
	click *ClickHouseClient
}

type FetchResult struct {
	TotalWritten  int
	NewCheckpoint string
	Client        *ClickHouseClient
}

func NewIngestor(cfg *Config) (*Ingestor, error) {
	ch, err := NewClickHouseClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Ingestor{
		cfg:   cfg,
		api:   NewHTTPJSONClient(cfg.RequestTimeout, cfg.CHConnectTimeout, "statground-polymarket-crawler"),
		state: NewStateStore(cfg),
		click: ch,
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

func (i *Ingestor) FetchAndInsert(ctx context.Context, entity string, checkpoint map[string]string, ch *ClickHouseClient) (FetchResult, error) {
	lastCP := checkpoint[entity]
	bestSeen := lastCP

	orderUsed, err := i.PickOrder(ctx, entity)
	if err != nil {
		return FetchResult{Client: ch}, err
	}
	fmt.Printf("[FETCH] %s order=%s checkpoint=%s\n", entity, orderUsed, defaultDisplay(lastCP, "(none)"))

	totalWritten := 0
	rows := make([]map[string]any, 0, i.cfg.InsertBatchSizeForEntity(entity))
	url := i.cfg.Endpoint(entity)
	current := ch

	for page := 0; page < i.cfg.MaxPages; page++ {
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
				return FetchResult{Client: current}, err
			}
			row, err := BuildEntityRow(ctx, current, entity, NormalizeRawJSON(obj), collectedAt, rawKey)
			if err != nil {
				return FetchResult{Client: current}, err
			}
			if row != nil {
				rows = append(rows, row)
				totalWritten++
			}
			bestSeen = MaxTimeISO(bestSeen, updated)
		}

		current, err = current.FlushEntityRows(ctx, entity, &rows, false)
		if err != nil {
			return FetchResult{Client: current}, err
		}

		fmt.Printf("[%s] page=%d offset=%d inserted=%d http_status=%d\n", entity, page+1, offset, totalWritten, meta.HTTPStatus)
		if stop {
			fmt.Printf("[%s] reached checkpoint -> stop\n", entity)
			break
		}
		if len(items) < i.cfg.PageLimit {
			break
		}
		if err := SleepContext(ctx, i.cfg.BaseSleep); err != nil {
			return FetchResult{Client: current}, err
		}
	}

	current, err = current.FlushEntityRows(ctx, entity, &rows, true)
	if err != nil {
		return FetchResult{Client: current}, err
	}

	return FetchResult{TotalWritten: totalWritten, NewCheckpoint: bestSeen, Client: current}, nil
}

func (i *Ingestor) OptimizeAfterBatch(ctx context.Context, ch *ClickHouseClient) {
	ch.OptimizeRandomPartitions(ctx, []string{
		i.cfg.EntityQualifiedTable("events"),
		i.cfg.EntityQualifiedTable("markets"),
		i.cfg.EntityQualifiedTable("series"),
	})
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

	fmt.Printf("[CONFIG] insert_batch_size_default=%d insert_batch_size_events=%d insert_batch_size_markets=%d insert_batch_size_series=%d insert_split_after_attempt=%d insert_min_split_batch_rows=%d max_partitions_per_insert_block=%d throw_on_max_partitions_per_insert_block=%d\n",
		cfg.InsertBatchSize,
		cfg.InsertBatchSizeForEntity("events"),
		cfg.InsertBatchSizeForEntity("markets"),
		cfg.InsertBatchSizeForEntity("series"),
		cfg.InsertSplitAfterAttempt,
		cfg.InsertMinSplitBatchRows,
		cfg.InsertMaxPartitionsPerBlock,
		boolToInt(cfg.InsertThrowOnMaxPartitionsPerBlock),
	)

	ctx := context.Background()
	checkpoint, err := ingestor.state.LoadCheckpoint(ctx)
	if err != nil {
		return err
	}
	newCheckpoint := CopyMapStringString(checkpoint)
	if newCheckpoint == nil {
		newCheckpoint = map[string]string{}
	}

	current := ingestor.click
	wroteTotal := 0
	for _, entity := range []string{"events", "markets", "series"} {
		res, err := ingestor.FetchAndInsert(ctx, entity, checkpoint, current)
		if err != nil {
			return err
		}
		current = res.Client
		wroteTotal += res.TotalWritten
		if res.NewCheckpoint != "" {
			newCheckpoint[entity] = res.NewCheckpoint
		}
	}

	if err := ingestor.state.SaveCheckpoint(ctx, newCheckpoint); err != nil {
		return err
	}
	ingestor.OptimizeAfterBatch(ctx, current)
	fmt.Printf("\nDONE. inserted_objects=%d\n", wroteTotal)
	return nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
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
