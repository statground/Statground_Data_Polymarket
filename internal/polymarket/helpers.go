package polymarket

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func UTCNow() time.Time {
	return time.Now().UTC()
}

func ParseISOUTC(value string) *time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	value = strings.ReplaceAll(value, "Z", "+00:00")
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, value); err == nil {
			ut := t.UTC()
			return &ut
		}
	}
	return nil
}

func ParseClickHouseTime(value any) *time.Time {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		return ParseISOUTC(v)
	case json.Number:
		if i, err := v.Int64(); err == nil {
			t := time.Unix(i, 0).UTC()
			return &t
		}
	case float64:
		sec, dec := math.Modf(v)
		t := time.Unix(int64(sec), int64(dec*float64(time.Second))).UTC()
		return &t
	}
	return ParseISOUTC(fmt.Sprint(value))
}

func FormatISO8601UTC(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func FormatISO8601UTCMicro(t time.Time) string {
	return t.UTC().Truncate(time.Microsecond).Format("2006-01-02T15:04:05.000000Z")
}

func FormatDateTimeInput(t *time.Time) any {
	if t == nil {
		return nil
	}
	return FormatISO8601UTCMicro(*t)
}

func Bool01(v any) int {
	switch x := v.(type) {
	case nil:
		return 0
	case bool:
		if x {
			return 1
		}
		return 0
	case string:
		xs := strings.TrimSpace(strings.ToLower(x))
		switch xs {
		case "1", "true", "yes", "y", "on":
			return 1
		default:
			return 0
		}
	case json.Number:
		if i, err := x.Int64(); err == nil && i != 0 {
			return 1
		}
	case float64:
		if x != 0 {
			return 1
		}
	case int, int8, int16, int32, int64:
		return 1
	case uint, uint8, uint16, uint32, uint64:
		return 1
	}
	return 0
}

func SafeString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case json.Number:
		return x.String()
	case []byte:
		return string(x)
	case map[string]any, []any:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(b)
	default:
		return fmt.Sprint(v)
	}
}

func SafeFloat64(v any) *float64 {
	switch x := v.(type) {
	case nil:
		return nil
	case float64:
		return &x
	case float32:
		y := float64(x)
		return &y
	case json.Number:
		if f, err := x.Float64(); err == nil {
			return &f
		}
	case string:
		xs := strings.TrimSpace(x)
		if xs == "" {
			return nil
		}
		if f, err := strconv.ParseFloat(xs, 64); err == nil {
			return &f
		}
	default:
		if f, err := strconv.ParseFloat(fmt.Sprint(v), 64); err == nil {
			return &f
		}
	}
	return nil
}

func SafeUint64(v any) *uint64 {
	switch x := v.(type) {
	case nil:
		return nil
	case uint64:
		return &x
	case uint:
		y := uint64(x)
		return &y
	case int:
		if x < 0 {
			return nil
		}
		y := uint64(x)
		return &y
	case int64:
		if x < 0 {
			return nil
		}
		y := uint64(x)
		return &y
	case float64:
		if x < 0 {
			return nil
		}
		y := uint64(x)
		return &y
	case json.Number:
		if i, err := x.Int64(); err == nil && i >= 0 {
			y := uint64(i)
			return &y
		}
		if f, err := x.Float64(); err == nil && f >= 0 {
			y := uint64(f)
			return &y
		}
	case string:
		xs := strings.TrimSpace(x)
		if xs == "" {
			return nil
		}
		if u, err := strconv.ParseUint(xs, 10, 64); err == nil {
			return &u
		}
		if i, err := strconv.ParseInt(xs, 10, 64); err == nil && i >= 0 {
			y := uint64(i)
			return &y
		}
	default:
		if u, err := strconv.ParseUint(strings.TrimSpace(fmt.Sprint(v)), 10, 64); err == nil {
			return &u
		}
	}
	return nil
}

func ExtractIDs(v any, idKey string) []uint64 {
	out := make([]uint64, 0)
	arr, ok := v.([]any)
	if !ok {
		return out
	}
	for _, item := range arr {
		switch x := item.(type) {
		case map[string]any:
			if u := SafeUint64(x[idKey]); u != nil {
				out = append(out, *u)
			}
		default:
			if u := SafeUint64(item); u != nil {
				out = append(out, *u)
			}
		}
	}
	return out
}

func ExtractStringArray(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return []string{}
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		out = append(out, SafeString(item))
	}
	return out
}

func ISOLEQ(a, b string) bool {
	ta := ParseISOUTC(a)
	tb := ParseISOUTC(b)
	if ta == nil || tb == nil {
		return false
	}
	return !ta.After(*tb)
}

func UUIDv7() (string, error) {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	if _, err := crand.Read(b[6:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80

	dst := make([]byte, 36)
	hex.Encode(dst[0:8], b[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:36], b[10:16])
	return string(dst), nil
}

func DecodeJSON(data []byte, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	return dec.Decode(v)
}

func DecodeJSONReader(r *strings.Reader, v any) error {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	return dec.Decode(v)
}

func DecodeJSONFromBytes(data []byte) (any, error) {
	var v any
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func MaxTimeISO(existing string, candidate string) string {
	if candidate == "" {
		return existing
	}
	if existing == "" {
		return candidate
	}
	te := ParseISOUTC(existing)
	tc := ParseISOUTC(candidate)
	if te == nil {
		return candidate
	}
	if tc == nil {
		return existing
	}
	if tc.After(*te) {
		return candidate
	}
	return existing
}

func CopyMapStringString(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func SleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func RetryableHTTPStatus(code int) bool {
	return code == 429 || code >= 500
}

func RetryBackoff(base time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	maxSec := float64(60)
	factor := math.Pow(2, float64(attempt-1))
	jitter := mrand.New(mrand.NewSource(time.Now().UnixNano())).Float64()
	seconds := math.Min(maxSec, base.Seconds()*factor) + jitter
	return time.Duration(seconds * float64(time.Second))
}

func IsRetryableInsertError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	msg := strings.ToLower(err.Error())
	markers := []string{
		"timed out",
		"timeout",
		"connection aborted",
		"connection reset",
		"broken pipe",
		"remote disconnected",
		"temporarily unavailable",
		"network",
		"bad gateway",
		"service unavailable",
		"gateway timeout",
		"eof",
	}
	for _, marker := range markers {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func EnsureDir(path string) error {
	return os.MkdirAll(path, 0o755)
}

func WriteFileAtomic(path string, data []byte) error {
	if err := EnsureDir(filepath.Dir(path)); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func FindRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	cur := wd
	for {
		if exists(filepath.Join(cur, ".git")) || exists(filepath.Join(cur, "go.mod")) {
			return cur, nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}
	return "", fmt.Errorf("repository root not found from %s", wd)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func SortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
