package polymarket

import (
	"bytes"
	"context"
	"fmt"
	"html"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type entityMeta struct {
	Key       string
	KR        string
	EN        string
	Table     string
	IDCol     string
	Created   string
	Collected string
}

type periodMeta struct {
	Key    string
	Label  string
	XLabel string
}

type bucketPoint struct {
	Bucket time.Time
	Count  int64
	Cum    int64
}

func bucketExpr(dtCol, period string) string {
	switch period {
	case "hourly":
		return fmt.Sprintf("toStartOfHour(%s)", dtCol)
	case "daily":
		return fmt.Sprintf("toDate(%s)", dtCol)
	case "monthly":
		return fmt.Sprintf("toStartOfMonth(%s)", dtCol)
	case "yearly":
		return fmt.Sprintf("toStartOfYear(%s)", dtCol)
	default:
		panic("unknown period: " + period)
	}
}

func buildCreatedSeries(ctx context.Context, ch *ClickHouseClient, db string, entity entityMeta, period string) ([]bucketPoint, error) {
	bucket := bucketExpr(entity.Created, period)
	sql := fmt.Sprintf(`
SELECT
    %s AS bucket,
    uniqExact(%s) AS cnt
FROM %s.%s
WHERE %s IS NOT NULL
GROUP BY bucket
ORDER BY bucket`, bucket, entity.IDCol, db, entity.Table, entity.Created)
	rows, err := ch.QueryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	return rowsToSeries(rows), nil
}

func buildLatestCollectedSeries(ctx context.Context, ch *ClickHouseClient, db string, entity entityMeta, period string) ([]bucketPoint, error) {
	bucket := bucketExpr("latest_collected_at", period)
	sql := fmt.Sprintf(`
SELECT
    %s AS bucket,
    count() AS cnt
FROM
(
    SELECT
        %s AS entity_id,
        max(%s) AS latest_collected_at
    FROM %s.%s
    WHERE %s IS NOT NULL
    GROUP BY entity_id
)
GROUP BY bucket
ORDER BY bucket`, bucket, entity.IDCol, entity.Collected, db, entity.Table, entity.Collected)
	rows, err := ch.QueryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	return rowsToSeries(rows), nil
}

func latestCollectedMinMax(ctx context.Context, ch *ClickHouseClient, db string, entity entityMeta) (*time.Time, *time.Time, error) {
	sql := fmt.Sprintf(`
SELECT
    min(latest_collected_at) AS min_collected,
    max(latest_collected_at) AS max_collected
FROM
(
    SELECT
        %s AS entity_id,
        max(%s) AS latest_collected_at
    FROM %s.%s
    WHERE %s IS NOT NULL
    GROUP BY entity_id
)`, entity.IDCol, entity.Collected, db, entity.Table, entity.Collected)
	row, err := ch.QueryOneRow(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	return ParseClickHouseTime(row["min_collected"]), ParseClickHouseTime(row["max_collected"]), nil
}

func rowsToSeries(rows []map[string]any) []bucketPoint {
	points := make([]bucketPoint, 0, len(rows))
	var cum int64
	for _, row := range rows {
		bucket := ParseClickHouseTime(row["bucket"])
		if bucket == nil {
			continue
		}
		cnt := int64(0)
		if n := SafeUint64(row["cnt"]); n != nil {
			cnt = int64(*n)
		}
		cum += cnt
		points = append(points, bucketPoint{Bucket: *bucket, Count: cnt, Cum: cum})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Bucket.Before(points[j].Bucket) })
	return points
}

func renderBarLineSVG(points []bucketPoint, title string, xlabel string, outPath string, useKorean bool) error {
	if len(points) == 0 {
		return nil
	}
	const (
		width  = 1280
		height = 460
		left   = 80.0
		right  = 82.0
		top    = 52.0
		bottom = 96.0
	)
	plotW := float64(width) - left - right
	plotH := float64(height) - top - bottom

	maxCount := int64(0)
	maxCum := int64(0)
	for _, p := range points {
		if p.Count > maxCount {
			maxCount = p.Count
		}
		if p.Cum > maxCum {
			maxCum = p.Cum
		}
	}
	if maxCount < 1 {
		maxCount = 1
	}
	if maxCum < 1 {
		maxCum = 1
	}

	step := plotW / float64(maxInt(1, len(points)))
	barW := math.Max(1, step-2)
	if len(points) > 250 {
		barW = math.Max(1, step)
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">`, width, height, width, height))
	b.WriteString(`<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>`)
	b.WriteString(fmt.Sprintf(`<text x="%0.1f" y="28" font-size="20" text-anchor="middle" font-family="sans-serif" fill="#111827">%s</text>`, float64(width)/2, html.EscapeString(title)))

	for i := 0; i <= 5; i++ {
		y := top + (plotH/5.0)*float64(i)
		b.WriteString(fmt.Sprintf(`<line x1="%0.1f" y1="%0.1f" x2="%0.1f" y2="%0.1f" stroke="#e5e7eb" stroke-width="1"/>`, left, y, left+plotW, y))
		countVal := float64(maxCount) * (1.0 - float64(i)/5.0)
		cumVal := float64(maxCum) * (1.0 - float64(i)/5.0)
		b.WriteString(fmt.Sprintf(`<text x="%0.1f" y="%0.1f" font-size="11" text-anchor="end" font-family="sans-serif" fill="#374151">%s</text>`, left-8, y+4, humanInt(int64(math.Round(countVal)))))
		b.WriteString(fmt.Sprintf(`<text x="%0.1f" y="%0.1f" font-size="11" text-anchor="start" font-family="sans-serif" fill="#374151">%s</text>`, left+plotW+8, y+4, humanInt(int64(math.Round(cumVal)))))
	}

	b.WriteString(fmt.Sprintf(`<line x1="%0.1f" y1="%0.1f" x2="%0.1f" y2="%0.1f" stroke="#111827" stroke-width="1.2"/>`, left, top+plotH, left+plotW, top+plotH))
	b.WriteString(fmt.Sprintf(`<line x1="%0.1f" y1="%0.1f" x2="%0.1f" y2="%0.1f" stroke="#111827" stroke-width="1.2"/>`, left, top, left, top+plotH))
	b.WriteString(fmt.Sprintf(`<line x1="%0.1f" y1="%0.1f" x2="%0.1f" y2="%0.1f" stroke="#111827" stroke-width="1.2"/>`, left+plotW, top, left+plotW, top+plotH))

	var linePoints strings.Builder
	for idx, p := range points {
		xCenter := left + step*float64(idx) + step/2.0
		barH := 0.0
		if maxCount > 0 {
			barH = (float64(p.Count) / float64(maxCount)) * plotH
		}
		yBar := top + plotH - barH
		b.WriteString(fmt.Sprintf(`<rect x="%0.1f" y="%0.1f" width="%0.1f" height="%0.1f" fill="#93c5fd" stroke="#60a5fa" stroke-width="0.5"/>`, xCenter-barW/2.0, yBar, barW, barH))

		yLine := top + plotH
		if maxCum > 0 {
			yLine = top + plotH - (float64(p.Cum)/float64(maxCum))*plotH
		}
		linePoints.WriteString(fmt.Sprintf("%0.1f,%0.1f ", xCenter, yLine))
	}
	b.WriteString(fmt.Sprintf(`<polyline fill="none" stroke="#ef4444" stroke-width="2" points="%s"/>`, strings.TrimSpace(linePoints.String())))

	tickCount := minInt(len(points), 8)
	if tickCount < 1 {
		tickCount = 1
	}
	if len(points) == 1 {
		tickCount = 1
	}
	seen := make(map[int]struct{})
	for i := 0; i < tickCount; i++ {
		idx := 0
		if len(points) > 1 {
			idx = int(math.Round(float64(i) * float64(len(points)-1) / float64(tickCount-1)))
		}
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		xCenter := left + step*float64(idx) + step/2.0
		label := formatBucketLabel(inferPeriodFromTitle(title), points[idx].Bucket)
		b.WriteString(fmt.Sprintf(`<line x1="%0.1f" y1="%0.1f" x2="%0.1f" y2="%0.1f" stroke="#111827" stroke-width="1"/>`, xCenter, top+plotH, xCenter, top+plotH+6))
		b.WriteString(fmt.Sprintf(`<text x="%0.1f" y="%0.1f" font-size="11" text-anchor="end" transform="rotate(-35 %0.1f %0.1f)" font-family="sans-serif" fill="#374151">%s</text>`, xCenter+2, top+plotH+22, xCenter+2, top+plotH+22, html.EscapeString(label)))
	}

	leftLabel := "Bucket count"
	rightLabel := "Cumulative"
	if useKorean {
		leftLabel = "구간별 수"
		rightLabel = "누적"
	}
	b.WriteString(fmt.Sprintf(`<text x="26" y="%0.1f" font-size="12" transform="rotate(-90 26 %0.1f)" text-anchor="middle" font-family="sans-serif" fill="#1f2937">%s</text>`, top+plotH/2.0, top+plotH/2.0, html.EscapeString(leftLabel)))
	b.WriteString(fmt.Sprintf(`<text x="%0.1f" y="%0.1f" font-size="12" transform="rotate(90 %0.1f %0.1f)" text-anchor="middle" font-family="sans-serif" fill="#1f2937">%s</text>`, float64(width)-24, top+plotH/2.0, float64(width)-24, top+plotH/2.0, html.EscapeString(rightLabel)))
	b.WriteString(fmt.Sprintf(`<text x="%0.1f" y="%0.1f" font-size="13" text-anchor="middle" font-family="sans-serif" fill="#111827">%s</text>`, left+plotW/2.0, float64(height)-18, html.EscapeString(xlabel)))
	b.WriteString(`</svg>`)

	return WriteFileAtomic(outPath, []byte(b.String()))
}

func inferPeriodFromTitle(title string) string {
	lower := strings.ToLower(title)
	switch {
	case strings.Contains(lower, "hour") || strings.Contains(title, "시간"):
		return "hourly"
	case strings.Contains(lower, "day") || strings.Contains(title, "일별"):
		return "daily"
	case strings.Contains(lower, "month") || strings.Contains(title, "월별"):
		return "monthly"
	default:
		return "yearly"
	}
}

func formatBucketLabel(period string, t time.Time) string {
	t = t.UTC()
	switch period {
	case "yearly":
		return t.Format("2006")
	case "monthly":
		return t.Format("2006-01")
	case "daily":
		return t.Format("2006-01-02")
	case "hourly":
		return t.Format("2006-01-02 15:00")
	default:
		return t.Format(time.RFC3339)
	}
}

func humanInt(v int64) string {
	negative := v < 0
	if negative {
		v = -v
	}
	s := fmt.Sprintf("%d", v)
	if len(s) <= 3 {
		if negative {
			return "-" + s
		}
		return s
	}
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	parts = append([]string{s}, parts...)
	joined := strings.Join(parts, ",")
	if negative {
		return "-" + joined
	}
	return joined
}

func topBuckets(points []bucketPoint, n int) []bucketPoint {
	copied := append([]bucketPoint{}, points...)
	sort.Slice(copied, func(i, j int) bool {
		if copied[i].Count == copied[j].Count {
			return copied[i].Bucket.Before(copied[j].Bucket)
		}
		return copied[i].Count > copied[j].Count
	})
	if len(copied) > n {
		copied = copied[:n]
	}
	return copied
}

func fmtMaybeTime(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func reportTitle(useKorean bool) (title string, subtitle string, note string) {
	if useKorean {
		return "Polymarket 통계 리포트", "생성 시점 / 최근 반영 시점 기준", strings.Join([]string{
			"본 문서는 Polymarket의 공개 데이터를 기준으로 집계한 결과입니다.",
			"생성 시점은 원본 createdAt, 반영 시점은 각 id의 최신 collected_at_utc를 기준으로 계산합니다.",
			"내부 저장/구현 방식과 무관하게, 공개 데이터 흐름 관점에서 정리된 통계만 제공합니다.",
		}, "\n")
	}
	return "Polymarket Statistics Report", "Based on created/reflected time.", strings.Join([]string{
		"This document aggregates public Polymarket data.",
		"Creation time uses the original createdAt, and reflection time uses the latest collected_at_utc per id.",
		"It intentionally avoids internal storage/implementation details.",
	}, "\n")
}

func writeReport(path string, useKorean bool, sections []string) error {
	title, subtitle, note := reportTitle(useKorean)
	var b bytes.Buffer
	fmt.Fprintf(&b, "# %s\n\n%s\n\n%s\n\n", title, subtitle, note)
	for _, section := range sections {
		b.WriteString(section)
		if !strings.HasSuffix(section, "\n") {
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	return WriteFileAtomic(path, b.Bytes())
}

func RunStatsReport() error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	ctx := context.Background()
	ch, err := NewClickHouseClient(cfg)
	if err != nil {
		return err
	}

	outDir := filepath.Join(cfg.RepoRoot, "reports", "polymarket_stats")
	chartsDir := filepath.Join(outDir, "charts")
	if err := EnsureDir(chartsDir); err != nil {
		return err
	}

	useKorean := true
	entities := []entityMeta{
		{Key: "series", KR: "시리즈", EN: "Series", Table: cfg.SeriesTable, IDCol: "series_id", Created: "created_at_utc", Collected: "collected_at_utc"},
		{Key: "event", KR: "이벤트", EN: "Event", Table: cfg.EventTable, IDCol: "event_id", Created: "created_at_utc", Collected: "collected_at_utc"},
		{Key: "market", KR: "마켓", EN: "Market", Table: cfg.MarketTable, IDCol: "market_id", Created: "created_at_utc", Collected: "collected_at_utc"},
	}
	periods := []periodMeta{
		{Key: "yearly", Label: choose(useKorean, "연도별", "Yearly"), XLabel: choose(useKorean, "연도", "Year")},
		{Key: "monthly", Label: choose(useKorean, "월별", "Monthly"), XLabel: choose(useKorean, "월", "Month")},
		{Key: "daily", Label: choose(useKorean, "일별", "Daily"), XLabel: choose(useKorean, "일", "Day")},
		{Key: "hourly", Label: choose(useKorean, "시간별", "Hourly"), XLabel: choose(useKorean, "시간", "Hour")},
	}

	sections := make([]string, 0)
	for _, entity := range entities {
		label := choose(useKorean, entity.KR, entity.EN)
		sqlTotal := fmt.Sprintf("SELECT uniqExact(%s) AS total FROM %s.%s", entity.IDCol, cfg.CHDatabase, entity.Table)
		totalRow, err := ch.QueryOneRow(ctx, sqlTotal)
		if err != nil {
			return err
		}
		total := int64(0)
		if n := SafeUint64(totalRow["total"]); n != nil {
			total = int64(*n)
		}

		sqlCreatedRange := fmt.Sprintf(`SELECT minIf(%s, %s IS NOT NULL) AS min_created, maxIf(%s, %s IS NOT NULL) AS max_created FROM %s.%s`, entity.Created, entity.Created, entity.Created, entity.Created, cfg.CHDatabase, entity.Table)
		createdRange, err := ch.QueryOneRow(ctx, sqlCreatedRange)
		if err != nil {
			return err
		}
		minCreated := ParseClickHouseTime(createdRange["min_created"])
		maxCreated := ParseClickHouseTime(createdRange["max_created"])
		minCollected, maxCollected, err := latestCollectedMinMax(ctx, ch, cfg.CHDatabase, entity)
		if err != nil {
			return err
		}

		if useKorean {
			sections = append(sections, fmt.Sprintf("## %s\n\n- 전체 고유 개수: **%s**\n- 생성 시점 범위: %s ~ %s\n- 최근 반영 시점 범위: %s ~ %s\n", label, humanInt(total), fmtMaybeTime(minCreated), fmtMaybeTime(maxCreated), fmtMaybeTime(minCollected), fmtMaybeTime(maxCollected)))
		} else {
			sections = append(sections, fmt.Sprintf("## %s\n\n- Total unique count: **%s**\n- Created time range: %s ~ %s\n- Latest reflected time range: %s ~ %s\n", label, humanInt(total), fmtMaybeTime(minCreated), fmtMaybeTime(maxCreated), fmtMaybeTime(minCollected), fmtMaybeTime(maxCollected)))
		}
	}

	for _, entity := range entities {
		label := choose(useKorean, entity.KR, entity.EN)
		for _, period := range periods {
			createdSeries, err := buildCreatedSeries(ctx, ch, cfg.CHDatabase, entity, period.Key)
			if err != nil {
				return err
			}
			createdSlug := fmt.Sprintf("%s_created_%s.svg", entity.Key, period.Key)
			createdPath := filepath.Join(chartsDir, createdSlug)
			createdTitle := fmt.Sprintf("%s / %s / %s", label, choose(useKorean, "생성 시점", "Created time"), period.Label)
			if err := renderBarLineSVG(createdSeries, createdTitle, period.XLabel, createdPath, useKorean); err != nil {
				return err
			}
			if len(createdSeries) > 0 {
				tops := topBuckets(createdSeries, 5)
				rows := make([]string, 0, len(tops))
				for _, p := range tops {
					rows = append(rows, fmt.Sprintf("  - %s: %s", formatBucketLabel(period.Key, p.Bucket), humanInt(p.Count)))
				}
				sections = append(sections, fmt.Sprintf("### %s / %s / %s\n\n![](charts/%s)\n\n%s:\n%s\n",
					label,
					choose(useKorean, "생성 시점", "Created time"),
					period.Label,
					createdSlug,
					choose(useKorean, "상위 구간", "Top buckets"),
					strings.Join(rows, "\n"),
				))
			}

			collectedSeries, err := buildLatestCollectedSeries(ctx, ch, cfg.CHDatabase, entity, period.Key)
			if err != nil {
				return err
			}
			collectedSlug := fmt.Sprintf("%s_collected_%s.svg", entity.Key, period.Key)
			collectedPath := filepath.Join(chartsDir, collectedSlug)
			collectedTitle := fmt.Sprintf("%s / %s / %s", label, choose(useKorean, "최근 반영 시점", "Latest reflected time"), period.Label)
			if err := renderBarLineSVG(collectedSeries, collectedTitle, period.XLabel, collectedPath, useKorean); err != nil {
				return err
			}
			if len(collectedSeries) > 0 {
				tops := topBuckets(collectedSeries, 5)
				rows := make([]string, 0, len(tops))
				for _, p := range tops {
					rows = append(rows, fmt.Sprintf("  - %s: %s", formatBucketLabel(period.Key, p.Bucket), humanInt(p.Count)))
				}
				sections = append(sections, fmt.Sprintf("### %s / %s / %s\n\n![](charts/%s)\n\n%s:\n%s\n",
					label,
					choose(useKorean, "최근 반영 시점", "Latest reflected time"),
					period.Label,
					collectedSlug,
					choose(useKorean, "상위 구간", "Top buckets"),
					strings.Join(rows, "\n"),
				))
			}
		}
	}

	readmePath := filepath.Join(outDir, "README.md")
	if err := writeReport(readmePath, useKorean, sections); err != nil {
		return err
	}
	fmt.Printf("DONE. report=%s\n", readmePath)
	return nil
}

func choose(useKorean bool, kr, en string) string {
	if useKorean {
		return kr
	}
	return en
}
