package polymarket

import (
	"context"
	"fmt"
	"strings"
)

type EntityPage struct {
	Items      []map[string]any
	Meta       FetchMeta
	NextCursor string
	UsedKeyset bool
	CursorIn   string
	Offset     int
}

func (c *Config) KeysetEndpoint(entity string) string {
	return fmt.Sprintf("%s/%s/keyset", c.PolyBase, entity)
}

func (c *Config) PageLimitForEntity(entity string) int {
	limit := maxInt(1, c.PageLimit)
	if !c.UseKeysetPagination {
		return limit
	}
	switch entity {
	case "events":
		return minInt(limit, 500)
	case "markets":
		return minInt(limit, 1000)
	default:
		return limit
	}
}

func supportsKeysetPagination(entity string) bool {
	switch entity {
	case "events", "markets":
		return true
	default:
		return false
	}
}

func (i *Ingestor) FetchEntityPage(ctx context.Context, entity string, orderUsed string, page int, afterCursor string) (EntityPage, error) {
	pageLimit := i.cfg.PageLimitForEntity(entity)
	useKeyset := i.cfg.UseKeysetPagination && supportsKeysetPagination(entity)
	query := map[string]string{
		"limit":     fmt.Sprint(pageLimit),
		"order":     orderUsed,
		"ascending": "false",
	}
	url := i.cfg.Endpoint(entity)
	result := EntityPage{UsedKeyset: useKeyset, CursorIn: afterCursor}
	if useKeyset {
		url = i.cfg.KeysetEndpoint(entity)
		if strings.TrimSpace(afterCursor) != "" {
			query["after_cursor"] = afterCursor
		}
	} else {
		result.Offset = page * pageLimit
		query["offset"] = fmt.Sprint(result.Offset)
	}

	data, meta, err := i.api.SafeGetJSON(ctx, url, query, i.cfg.MaxRetries, i.cfg.BaseSleep)
	if err != nil {
		return result, err
	}
	result.Meta = meta
	result.Items = ExtractItems(data, entity)
	result.NextCursor = ExtractNextCursor(data)
	return result, nil
}

func ExtractNextCursor(data any) string {
	obj, ok := data.(map[string]any)
	if !ok {
		return ""
	}
	for _, key := range []string{"next_cursor", "nextCursor", "cursor"} {
		cursor := strings.TrimSpace(SafeString(obj[key]))
		if cursor != "" && !strings.EqualFold(cursor, "null") {
			return cursor
		}
	}
	return ""
}

func shortCursor(cursor string) string {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return "(none)"
	}
	if len(cursor) <= 16 {
		return cursor
	}
	return cursor[:8] + "…" + cursor[len(cursor)-8:]
}
