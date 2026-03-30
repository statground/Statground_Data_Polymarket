package polymarket

import (
	"context"
	"encoding/json"
	"time"
)

var baseInsertColumns = map[string][]string{
	"events": {
		"event_id", "raw_key", "collected_at_utc", "created_at_utc", "updated_at_utc",
		"title", "ticker", "slug", "description",
		"active", "archived", "closed", "restricted",
		"start_date_utc", "end_date_utc", "closed_time_utc", "creation_date_utc",
		"series_slug", "series_ids", "market_ids",
		"icon_url", "image_url", "volume",
	},
	"markets": {
		"market_id", "raw_key", "collected_at_utc", "created_at_utc", "updated_at_utc",
		"condition_id", "question_id", "slug", "question", "description",
		"resolution_source", "resolved_by",
		"active", "approved", "archived", "closed", "restricted", "neg_risk",
		"start_date_utc", "end_date_utc", "closed_time_utc",
		"best_ask", "best_bid", "last_trade_price", "spread", "volume",
		"outcomes", "outcome_prices", "clob_token_ids",
		"series_slug", "series_ids", "event_ids",
	},
	"series": {
		"series_id", "raw_key", "collected_at_utc", "created_at_utc", "updated_at_utc",
		"slug", "ticker", "title",
		"active", "archived", "closed",
		"recurrence", "series_type",
		"liquidity", "volume", "volume_24h",
		"event_ids",
	},
}

func BuildEntityRow(ctx context.Context, ch *ClickHouseClient, entity string, obj map[string]any, collectedAt timeWrapper, rawKey string) (map[string]any, error) {
	rawJSON, hasRawJSON, err := ch.PrepareRawJSONValue(ctx, entity, obj)
	if err != nil {
		return nil, err
	}
	var row map[string]any
	switch entity {
	case "events":
		row = buildEventRow(obj, collectedAt, rawKey)
	case "markets":
		row = buildMarketRow(obj, collectedAt, rawKey)
	case "series":
		row = buildSeriesRow(obj, collectedAt, rawKey)
	default:
		return nil, nil
	}
	if row == nil {
		return nil, nil
	}
	if hasRawJSON {
		row["raw_json"] = rawJSON
	}
	return row, nil
}

type timeWrapper struct{ value string }

func wrapTime(t time.Time) timeWrapper { return timeWrapper{value: FormatISO8601UTC(t)} }

func buildEventRow(obj map[string]any, collectedAt timeWrapper, rawKey string) map[string]any {
	eventID := firstUint64(obj["id"], obj["eventId"], obj["event_id"])
	if eventID == nil {
		return nil
	}
	row := map[string]any{
		"event_id":          *eventID,
		"raw_key":           rawKey,
		"collected_at_utc":  collectedAt.value,
		"created_at_utc":    formatAnyTime(obj["createdAt"]),
		"updated_at_utc":    formatAnyTime(obj["updatedAt"]),
		"title":             SafeString(obj["title"]),
		"ticker":            SafeString(obj["ticker"]),
		"slug":              SafeString(obj["slug"]),
		"description":       SafeString(obj["description"]),
		"active":            Bool01(obj["active"]),
		"archived":          Bool01(obj["archived"]),
		"closed":            Bool01(obj["closed"]),
		"restricted":        Bool01(obj["restricted"]),
		"start_date_utc":    formatAnyTime(obj["startDate"]),
		"end_date_utc":      formatAnyTime(obj["endDate"]),
		"closed_time_utc":   formatAnyTime(obj["closedTime"]),
		"creation_date_utc": formatAnyTime(obj["creationDate"]),
		"series_slug":       SafeString(obj["seriesSlug"]),
		"series_ids":        firstIDSlice(ExtractIDs(obj["series"], "id"), ExtractIDs(obj["seriesIds"], "id"), ExtractIDs(obj["series_id"], "id"), ExtractIDs(obj["seriesID"], "id")),
		"market_ids":        firstIDSlice(ExtractIDs(obj["markets"], "id"), ExtractIDs(obj["marketIds"], "id"), ExtractIDs(obj["market_ids"], "id")),
		"icon_url":          SafeString(obj["icon"]),
		"image_url":         SafeString(obj["image"]),
		"volume":            safeFloatOrNil(obj["volume"]),
	}
	return row
}

func buildMarketRow(obj map[string]any, collectedAt timeWrapper, rawKey string) map[string]any {
	marketID := firstUint64(obj["id"], obj["marketId"], obj["market_id"])
	if marketID == nil {
		return nil
	}
	resolvedBy := any(nil)
	if obj["resolvedBy"] != nil {
		resolvedBy = SafeString(obj["resolvedBy"])
	}
	row := map[string]any{
		"market_id":         *marketID,
		"raw_key":           rawKey,
		"collected_at_utc":  collectedAt.value,
		"created_at_utc":    formatAnyTime(obj["createdAt"]),
		"updated_at_utc":    formatAnyTime(obj["updatedAt"]),
		"condition_id":      SafeString(obj["conditionId"]),
		"question_id":       firstString(obj["questionID"], obj["questionId"]),
		"slug":              SafeString(obj["slug"]),
		"question":          SafeString(obj["question"]),
		"description":       SafeString(obj["description"]),
		"resolution_source": SafeString(obj["resolutionSource"]),
		"resolved_by":       resolvedBy,
		"active":            Bool01(obj["active"]),
		"approved":          Bool01(obj["approved"]),
		"archived":          Bool01(obj["archived"]),
		"closed":            Bool01(obj["closed"]),
		"restricted":        Bool01(obj["restricted"]),
		"neg_risk":          Bool01(obj["negRisk"]),
		"start_date_utc":    formatAnyTime(obj["startDate"]),
		"end_date_utc":      formatAnyTime(obj["endDate"]),
		"closed_time_utc":   formatAnyTime(obj["closedTime"]),
		"best_ask":          safeFloatOrNil(obj["bestAsk"]),
		"best_bid":          safeFloatOrNil(obj["bestBid"]),
		"last_trade_price":  safeFloatOrNil(obj["lastTradePrice"]),
		"spread":            safeFloatOrNil(obj["spread"]),
		"volume":            safeFloatOrNil(obj["volume"]),
		"outcomes":          ExtractStringArray(obj["outcomes"]),
		"outcome_prices":    ExtractStringArray(obj["outcomePrices"]),
		"clob_token_ids":    ExtractStringArray(obj["clobTokenIds"]),
		"series_slug":       SafeString(obj["seriesSlug"]),
		"series_ids":        ExtractIDs(obj["series"], "id"),
		"event_ids":         firstIDSlice(ExtractIDs(obj["events"], "id"), ExtractIDs(obj["eventIds"], "id")),
	}
	return row
}

func buildSeriesRow(obj map[string]any, collectedAt timeWrapper, rawKey string) map[string]any {
	seriesID := firstUint64(obj["id"], obj["seriesId"], obj["series_id"])
	if seriesID == nil {
		return nil
	}
	row := map[string]any{
		"series_id":        *seriesID,
		"raw_key":          rawKey,
		"collected_at_utc": collectedAt.value,
		"created_at_utc":   formatAnyTime(obj["createdAt"]),
		"updated_at_utc":   formatAnyTime(obj["updatedAt"]),
		"slug":             SafeString(obj["slug"]),
		"ticker":           SafeString(obj["ticker"]),
		"title":            SafeString(obj["title"]),
		"active":           Bool01(obj["active"]),
		"archived":         Bool01(obj["archived"]),
		"closed":           Bool01(obj["closed"]),
		"recurrence":       SafeString(obj["recurrence"]),
		"series_type":      SafeString(obj["seriesType"]),
		"liquidity":        safeFloatOrNil(obj["liquidity"]),
		"volume":           safeFloatOrNil(obj["volume"]),
		"volume_24h":       firstFloat(obj["volume24hr"], obj["volume24h"]),
		"event_ids":        firstIDSlice(ExtractIDs(obj["events"], "id"), ExtractIDs(obj["eventIds"], "id")),
	}
	return row
}

func formatAnyTime(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		return formatTimeString(x)
	default:
		return formatTimeString(SafeString(v))
	}
}

func formatTimeString(s string) any {
	if t := ParseISOUTC(s); t != nil {
		return FormatISO8601UTC(*t)
	}
	return nil
}

func safeFloatOrNil(v any) any {
	if f := SafeFloat64(v); f != nil {
		return *f
	}
	return nil
}

func firstUint64(values ...any) *uint64 {
	for _, v := range values {
		if u := SafeUint64(v); u != nil {
			return u
		}
	}
	return nil
}

func firstString(values ...any) string {
	for _, v := range values {
		if s := SafeString(v); s != "" {
			return s
		}
	}
	return ""
}

func firstFloat(values ...any) any {
	for _, v := range values {
		if f := SafeFloat64(v); f != nil {
			return *f
		}
	}
	return nil
}

func firstIDSlice(slices ...[]uint64) []uint64 {
	for _, s := range slices {
		if len(s) > 0 {
			return s
		}
	}
	return []uint64{}
}

func NormalizeRawJSON(obj map[string]any) map[string]any {
	cloned := make(map[string]any, len(obj))
	for k, v := range obj {
		cloned[k] = normalizeJSONValue(v)
	}
	return cloned
}

func normalizeJSONValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return NormalizeRawJSON(x)
	case []any:
		out := make([]any, 0, len(x))
		for _, item := range x {
			out = append(out, normalizeJSONValue(item))
		}
		return out
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		if f, err := x.Float64(); err == nil {
			return f
		}
		return x.String()
	default:
		return x
	}
}
