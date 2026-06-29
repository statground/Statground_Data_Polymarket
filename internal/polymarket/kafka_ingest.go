package polymarket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type predictionKafkaEvent struct {
	EventUUID string `json:"event_uuid"`
	Source    string `json:"source"`
	Host      string `json:"host"`
	UUIDUser  string `json:"uuid_user"`
	IP        string `json:"ip"`
	URL       string `json:"url"`
	EventType string `json:"event_type"`
	Payload   string `json:"payload"`
	CreatedAt string `json:"created_at"`
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func splitIntCSV(raw string) []int {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			continue
		}
		out = append(out, n)
	}
	sort.Ints(out)
	return uniqueInts(out)
}

func uniqueInts(values []int) []int {
	if len(values) == 0 {
		return nil
	}
	out := values[:0]
	var last int
	for i, value := range values {
		if i > 0 && value == last {
			continue
		}
		out = append(out, value)
		last = value
	}
	return out
}

func (i *Ingestor) ValidateKafkaIngest(ctx context.Context) error {
	if len(i.cfg.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is empty")
	}
	if strings.TrimSpace(i.cfg.KafkaTopic) == "" {
		return fmt.Errorf("KAFKA_TOPIC is empty")
	}
	for _, broker := range i.cfg.KafkaBrokers {
		if isLoopbackBrokerEndpoint(broker) {
			return fmt.Errorf("KAFKA_BROKERS must be an externally reachable Kafka bootstrap address, not %q", broker)
		}
	}

	dialer := &kafka.Dialer{
		ClientID: i.cfg.KafkaClientID,
		Timeout:  10 * time.Second,
		DialFunc: kafkaAdvertisedBrokerDialFunc(i.cfg.KafkaBrokers, 10*time.Second),
	}
	if strings.TrimSpace(i.cfg.KafkaUsername) != "" || strings.TrimSpace(i.cfg.KafkaPassword) != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: i.cfg.KafkaUsername, Password: i.cfg.KafkaPassword}
	}

	probeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(probeCtx, "tcp", i.cfg.KafkaBrokers[0])
	if err != nil {
		return fmt.Errorf("kafka preflight failed to connect to bootstrap broker %q: %w", i.cfg.KafkaBrokers[0], err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(i.cfg.KafkaTopic)
	if err != nil {
		return fmt.Errorf("kafka preflight failed to read metadata for topic %q: %w", i.cfg.KafkaTopic, err)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafka preflight found zero partitions for topic %q", i.cfg.KafkaTopic)
	}
	if err := validateKafkaAdvertisedLeaders(partitions, i.cfg.KafkaBrokers, "kafka broker metadata"); err != nil {
		return err
	}

	fmt.Printf("[kafka] preflight ok topic=%s partitions=%d bootstrap=%s\n", i.cfg.KafkaTopic, len(partitions), i.cfg.KafkaBrokers[0])
	return nil
}

func isLoopbackBrokerEndpoint(raw string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(raw))
	if err != nil {
		host = strings.TrimSpace(raw)
		if strings.Contains(host, ":") {
			host = strings.Split(host, ":")[0]
		}
	}
	return isLoopbackHost(host)
}

func validateKafkaAdvertisedLeaders(partitions []kafka.Partition, brokers []string, label string) error {
	bootstrap := kafkaBootstrapEndpointSet(brokers)
	nonBootstrapLeaders := 0
	topics := map[string]bool{}
	for _, partition := range partitions {
		leaderHost := strings.TrimSpace(partition.Leader.Host)
		if isLoopbackHost(leaderHost) {
			return fmt.Errorf("%s advertises loopback listener %s:%d for topic=%s partition=%d; fix Kafka server KAFKA_PUBLIC_HOST/KAFKA_ADVERTISED_LISTENERS and force-recreate Kafka_Platform", label, leaderHost, partition.Leader.Port, partition.Topic, partition.ID)
		}
		leaderEndpoint := normalizedKafkaEndpoint(leaderHost, fmt.Sprint(partition.Leader.Port))
		if len(bootstrap) > 0 && !bootstrap[leaderEndpoint] {
			nonBootstrapLeaders++
			topics[partition.Topic] = true
		}
	}
	if nonBootstrapLeaders > 0 {
		fmt.Printf("[kafka] %s metadata has %d non-bootstrap advertised broker entries across %d topic(s); producer will dial via bootstrap rewrite\n", label, nonBootstrapLeaders, len(topics))
	}
	return nil
}

func kafkaBootstrapEndpointSet(brokers []string) map[string]bool {
	endpoints := make(map[string]bool, len(brokers))
	for _, broker := range brokers {
		host, port, ok := splitKafkaEndpoint(broker)
		if ok {
			endpoints[normalizedKafkaEndpoint(host, port)] = true
		}
	}
	return endpoints
}

func splitKafkaEndpoint(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		if strings.Count(raw, ":") != 1 {
			return "", "", false
		}
		parts := strings.SplitN(raw, ":", 2)
		host, port = parts[0], parts[1]
	}
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	return host, port, host != "" && port != ""
}

func normalizedKafkaEndpoint(host, port string) string {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	port = strings.TrimSpace(port)
	return host + ":" + port
}

func kafkaAdvertisedBrokerDialFunc(brokers []string, timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	if len(brokers) != 1 {
		return dialer.DialContext
	}
	bootstrapHost, bootstrapPort, ok := splitKafkaEndpoint(brokers[0])
	if !ok {
		return dialer.DialContext
	}
	bootstrapAddress := net.JoinHostPort(strings.Trim(bootstrapHost, "[]"), bootstrapPort)
	bootstrapEndpoint := normalizedKafkaEndpoint(bootstrapHost, bootstrapPort)
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		target := address
		if host, port, ok := splitKafkaEndpoint(address); ok {
			endpoint := normalizedKafkaEndpoint(host, port)
			if port == bootstrapPort && endpoint != bootstrapEndpoint {
				target = bootstrapAddress
			}
		}
		return dialer.DialContext(ctx, network, target)
	}
}

func isLoopbackHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	switch host {
	case "", "localhost", "127.0.0.1", "::1", "0.0.0.0":
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsUnspecified())
}

func (i *Ingestor) kafkaHost() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "github-actions"
	}
	return host
}

func (i *Ingestor) kafkaWriter() *kafka.Writer {
	return i.kafkaWriterWithBalancer(&kafka.Hash{})
}

func (i *Ingestor) kafkaWriterWithBalancer(balancer kafka.Balancer) *kafka.Writer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(i.cfg.KafkaBrokers...),
		Topic:                  i.cfg.KafkaTopic,
		Balancer:               balancer,
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchSize:              i.cfg.KafkaBatchSize,
		BatchBytes:             int64(i.cfg.KafkaBatchBytes),
		BatchTimeout:           i.cfg.KafkaBatchTimeout,
		WriteTimeout:           i.cfg.KafkaWriteTimeout,
		ReadTimeout:            i.cfg.KafkaWriteTimeout,
		MaxAttempts:            i.cfg.KafkaWriterMaxAttempts,
	}
	transport := &kafka.Transport{
		ClientID: i.cfg.KafkaClientID,
		Dial:     kafkaAdvertisedBrokerDialFunc(i.cfg.KafkaBrokers, 10*time.Second),
	}
	if strings.TrimSpace(i.cfg.KafkaUsername) != "" || strings.TrimSpace(i.cfg.KafkaPassword) != "" {
		transport.SASL = plain.Mechanism{Username: i.cfg.KafkaUsername, Password: i.cfg.KafkaPassword}
	}
	w.Transport = transport
	return w
}

func (i *Ingestor) publishKafkaEvents(ctx context.Context, events []predictionKafkaEvent) error {
	if len(events) == 0 {
		return nil
	}

	chunkSize := maxInt(1, i.cfg.KafkaWriteChunkSize)
	messages := make([]kafka.Message, 0, minInt(chunkSize, len(events)))
	for _, ev := range events {
		body, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		messages = append(messages, kafka.Message{
			Key:   []byte(kafkaEventKey(ev)),
			Value: body,
			Time:  UTCNow(),
		})
		if len(messages) >= chunkSize {
			if err := i.writeKafkaMessagesBounded(ctx, messages); err != nil {
				return err
			}
			messages = messages[:0]
		}
	}
	return i.writeKafkaMessagesBounded(ctx, messages)
}

func (i *Ingestor) writeKafkaMessagesBounded(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}
	err := i.writeKafkaMessagesWithRetry(ctx, messages)
	if err == nil {
		return nil
	}
	if len(messages) > 1 && isKafkaMessageSizeTooLarge(err) {
		mid := len(messages) / 2
		if err := i.writeKafkaMessagesBounded(ctx, messages[:mid]); err != nil {
			return err
		}
		return i.writeKafkaMessagesBounded(ctx, messages[mid:])
	}
	if len(messages) == 1 && isKafkaMessageSizeTooLarge(err) {
		return fmt.Errorf("kafka message too large after shrink key=%s value_bytes=%d: %w", string(messages[0].Key), len(messages[0].Value), err)
	}
	return err
}

func (i *Ingestor) writeKafkaMessagesWithRetry(ctx context.Context, messages []kafka.Message) error {
	pending := messages
	var lastErr error
	maxRetries := maxInt(1, i.cfg.MaxRetries)
	for attempt := 1; attempt <= maxRetries; attempt++ {
		writeCtx := ctx
		cancel := func() {}
		if i.cfg.KafkaWriteTimeout > 0 {
			writeCtx, cancel = context.WithTimeout(ctx, i.cfg.KafkaWriteTimeout+5*time.Second)
		}
		w := i.kafkaWriter()
		err := w.WriteMessages(writeCtx, pending...)
		_ = w.Close()
		cancel()
		if err == nil {
			if attempt > 1 {
				fmt.Printf("[kafka] publish retry succeeded attempt=%d messages=%d\n", attempt, len(pending))
			}
			return nil
		}
		lastErr = err
		if isKafkaMessageSizeTooLarge(err) || ctx.Err() != nil {
			return err
		}
		failed, retryable := retryableFailedMessages(pending, err)
		if len(failed) == 0 {
			return nil
		}
		if i.cfg.KafkaPartitionFallback && shouldUsePartitionFallback(err) {
			if fallbackErr := i.writeMessagesToWritablePartition(ctx, failed); fallbackErr == nil {
				return nil
			} else {
				return fmt.Errorf("kafka publish failed after fixed-partition fallback: %s; original_error=%s", shortKafkaError(fallbackErr), shortKafkaError(err))
			}
		}
		if !retryable || attempt >= maxRetries {
			return err
		}
		sleepFor := RetryBackoff(i.cfg.BaseSleep, attempt)
		fmt.Printf("[kafka retry] failed_messages=%d attempt=%d/%d sleep=%s reason=%s err=%s\n", len(failed), attempt, maxRetries, sleepFor, kafkaRetryReason(err), shortKafkaError(err))
		if err := SleepContext(ctx, sleepFor); err != nil {
			return fmt.Errorf("kafka retry wait stopped: %w; last_error=%s", err, shortKafkaError(lastErr))
		}
		pending = failed
	}
	return lastErr
}

func (i *Ingestor) writeMessagesToWritablePartition(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}
	partitions, err := i.fallbackPartitionIDs(ctx)
	if err != nil {
		return err
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafka partition fallback found zero partitions for topic=%s", i.cfg.KafkaTopic)
	}

	pending := messages
	var lastErr error
	for _, partition := range partitions {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		attemptCtx, cancel := context.WithTimeout(ctx, i.cfg.KafkaFallbackTimeout)
		w := i.kafkaWriterWithBalancer(fixedPartitionBalancer{partition: partition})
		err := w.WriteMessages(attemptCtx, pending...)
		_ = w.Close()
		cancel()
		if err == nil {
			fmt.Printf("[kafka] fixed partition fallback succeeded partition=%d messages=%d\n", partition, len(pending))
			return nil
		}

		lastErr = err
		failed, retryable := retryableFailedMessages(pending, err)
		if len(failed) == 0 {
			return nil
		}
		pending = failed
		fmt.Printf("[kafka] fixed partition fallback failed partition=%d failed_messages=%d reason=%s error=%s\n", partition, len(pending), kafkaRetryReason(err), shortKafkaError(err))
		if !retryable {
			return err
		}
	}
	return fmt.Errorf("kafka fixed partition fallback exhausted partitions=%v failed_messages=%d last_error=%s", partitions, len(pending), shortKafkaError(lastErr))
}

func (i *Ingestor) fallbackPartitionIDs(ctx context.Context) ([]int, error) {
	if len(i.cfg.KafkaFallbackPartitions) > 0 {
		out := append([]int(nil), i.cfg.KafkaFallbackPartitions...)
		sort.Ints(out)
		return uniqueInts(out), nil
	}

	dialer := &kafka.Dialer{
		ClientID: i.cfg.KafkaClientID,
		Timeout:  10 * time.Second,
		DialFunc: kafkaAdvertisedBrokerDialFunc(i.cfg.KafkaBrokers, 10*time.Second),
	}
	if strings.TrimSpace(i.cfg.KafkaUsername) != "" || strings.TrimSpace(i.cfg.KafkaPassword) != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: i.cfg.KafkaUsername, Password: i.cfg.KafkaPassword}
	}

	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(probeCtx, "tcp", i.cfg.KafkaBrokers[0])
	if err != nil {
		return nil, fmt.Errorf("kafka partition fallback failed to connect to bootstrap broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(i.cfg.KafkaTopic)
	if err != nil {
		return nil, fmt.Errorf("kafka partition fallback failed to read metadata for topic %q: %w", i.cfg.KafkaTopic, err)
	}
	out := make([]int, 0, len(partitions))
	for _, partition := range partitions {
		if partition.Topic == i.cfg.KafkaTopic {
			out = append(out, partition.ID)
		}
	}
	sort.Ints(out)
	return uniqueInts(out), nil
}

type fixedPartitionBalancer struct {
	partition int
}

func (b fixedPartitionBalancer) Balance(_ kafka.Message, partitions ...int) int {
	for _, partition := range partitions {
		if partition == b.partition {
			return partition
		}
	}
	if len(partitions) > 0 {
		return partitions[0]
	}
	return b.partition
}

func isKafkaMessageSizeTooLarge(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "message size too large") || strings.Contains(msg, "record too large")
}

func retryableFailedMessages(messages []kafka.Message, err error) ([]kafka.Message, bool) {
	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		if len(writeErrs) != len(messages) {
			return messages, retryableKafkaWriteError(err)
		}
		failed := make([]kafka.Message, 0, writeErrs.Count())
		retryable := true
		for idx, writeErr := range writeErrs {
			if writeErr == nil {
				continue
			}
			failed = append(failed, messages[idx])
			if !retryableKafkaWriteError(writeErr) {
				retryable = false
			}
		}
		return failed, retryable
	}
	return messages, retryableKafkaWriteError(err)
}

func retryableKafkaWriteError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		if writeErrs.Count() == 0 {
			return false
		}
		for _, writeErr := range writeErrs {
			if writeErr != nil && !retryableKafkaWriteError(writeErr) {
				return false
			}
		}
		return true
	}
	var tempErr interface{ Temporary() bool }
	if errors.As(err, &tempErr) && tempErr.Temporary() {
		return true
	}
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return true
	}
	return errors.Is(err, io.EOF) || isRetryableKafkaErrorText(err.Error())
}

func isRetryableKafkaErrorText(message string) bool {
	msg := strings.ToLower(message)
	return strings.Contains(msg, "not leader for partition") ||
		strings.Contains(msg, "partition has no leader") ||
		strings.Contains(msg, "has no leader") ||
		strings.Contains(msg, "leader not available") ||
		strings.Contains(msg, "metadata are likely out of date") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "failed to dial") ||
		strings.Contains(msg, "failed to open connection") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "network is unreachable") ||
		strings.Contains(msg, "temporary failure in name resolution") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "eof")
}

func shouldUsePartitionFallback(err error) bool {
	reason := kafkaRetryReason(err)
	return reason == "leader-metadata-stale" || reason == "leader-not-available"
}

func kafkaRetryReason(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "not leader for partition"),
		strings.Contains(msg, "partition has no leader"),
		strings.Contains(msg, "has no leader"),
		strings.Contains(msg, "metadata are likely out of date"):
		return "leader-metadata-stale"
	case strings.Contains(msg, "leader not available"):
		return "leader-not-available"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "eof"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "failed to dial"),
		strings.Contains(msg, "failed to open connection"),
		strings.Contains(msg, "no route to host"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "temporary failure in name resolution"):
		return "network"
	default:
		return "temporary-kafka-error"
	}
}

func shortKafkaError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.Join(strings.Fields(err.Error()), " ")
	return truncateString(msg, 280)
}

func truncateString(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

func (i *Ingestor) PublishCheckpoint(ctx context.Context, checkpoint map[string]string) error {
	if len(checkpoint) == 0 {
		return nil
	}
	now := UTCNow()
	checkpointUUID, err := UUIDv7()
	if err != nil {
		return err
	}
	payload := map[string]any{
		"checkpoint_uuid": checkpointUUID,
		"service":         "polymarket",
		"source":          i.cfg.ProducerSource,
		"checkpoint":      checkpoint,
		"updated_at":      FormatISO8601UTCMicro(now),
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	ev := predictionKafkaEvent{
		EventUUID: checkpointUUID,
		Source:    i.cfg.ProducerSource,
		Host:      i.kafkaHost(),
		UUIDUser:  "",
		IP:        i.cfg.ProducerIP,
		URL:       strings.TrimRight(i.cfg.PolyBase, "/") + "/checkpoint",
		EventType: "polymarket.crawl_checkpoint.v1",
		Payload:   string(payloadJSON),
		CreatedAt: FormatISO8601UTCMicro(now),
	}
	ev, err = i.ensureKafkaEventSize("checkpoint", ev)
	if err != nil {
		return err
	}
	return i.publishKafkaEvents(ctx, []predictionKafkaEvent{ev})
}

func kafkaEventKey(ev predictionKafkaEvent) string {
	if strings.TrimSpace(ev.URL) != "" {
		return ev.EventType + ":" + ev.URL
	}
	return ev.EventType + ":" + ev.EventUUID
}

func (i *Ingestor) rowEvent(entity string, row map[string]any) (predictionKafkaEvent, error) {
	payload := cloneRowForKafka(entity, row)
	payload = limitKafkaPayloadArrays(payload, i.cfg.KafkaMaxArrayItems)
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return predictionKafkaEvent{}, err
	}
	eventUUID := SafeString(row["raw_key"])
	if strings.TrimSpace(eventUUID) == "" {
		eventUUID, err = UUIDv7()
		if err != nil {
			return predictionKafkaEvent{}, err
		}
	}
	createdAt := firstNonEmpty(SafeString(row["collected_at_utc"]), FormatISO8601UTCMicro(UTCNow()))
	ev := predictionKafkaEvent{
		EventUUID: eventUUID,
		Source:    i.cfg.ProducerSource,
		Host:      i.kafkaHost(),
		UUIDUser:  "",
		IP:        i.cfg.ProducerIP,
		URL:       i.rowEventURL(entity, row),
		EventType: polymarketEventType(entity),
		Payload:   string(payloadJSON),
		CreatedAt: createdAt,
	}
	return i.ensureKafkaEventSize(entity, ev)
}

func (i *Ingestor) ensureKafkaEventSize(entity string, ev predictionKafkaEvent) (predictionKafkaEvent, error) {
	maxBytes := i.cfg.KafkaMaxMessageBytes
	if maxBytes <= 0 {
		return ev, nil
	}
	before := kafkaEventWireBytes(ev)
	if before <= maxBytes {
		return ev, nil
	}

	payload := map[string]any{}
	if err := json.Unmarshal([]byte(ev.Payload), &payload); err != nil {
		return predictionKafkaEvent{}, err
	}

	payload = shrinkKafkaPayload(payload, "raw_json_omitted_because_kafka_message_exceeded_configured_limit")
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return predictionKafkaEvent{}, err
	}
	ev.Payload = string(payloadJSON)
	after := kafkaEventWireBytes(ev)

	if after > maxBytes {
		payload = hardTrimKafkaPayload(payload)
		payload = limitKafkaPayloadArrays(payload, minInt(i.cfg.KafkaMaxArrayItems, 64))
		payloadJSON, err = json.Marshal(payload)
		if err != nil {
			return predictionKafkaEvent{}, err
		}
		ev.Payload = string(payloadJSON)
		after = kafkaEventWireBytes(ev)
	}

	if after > maxBytes {
		payload = emergencyTrimKafkaPayload(payload)
		payloadJSON, err = json.Marshal(payload)
		if err != nil {
			return predictionKafkaEvent{}, err
		}
		ev.Payload = string(payloadJSON)
		after = kafkaEventWireBytes(ev)
	}

	if after > maxBytes {
		return predictionKafkaEvent{}, fmt.Errorf("kafka event remains too large entity=%s key=%s bytes_before=%d bytes_after=%d max_bytes=%d", entity, kafkaEventKey(ev), before, after, maxBytes)
	}

	fmt.Printf("[kafka] oversized payload shrunk entity=%s key=%s bytes_before=%d bytes_after=%d max_bytes=%d\n",
		entity, kafkaEventKey(ev), before, after, maxBytes)
	return ev, nil
}

func kafkaEventWireBytes(ev predictionKafkaEvent) int {
	body, err := json.Marshal(ev)
	if err != nil {
		return 0
	}
	return len(body)
}

func shrinkKafkaPayload(payload map[string]any, policy string) map[string]any {
	out := make(map[string]any, len(payload)+4)
	for k, v := range payload {
		out[k] = v
	}
	raw := SafeString(out["raw_json"])
	if raw != "" {
		out["raw_json_original_bytes"] = len([]byte(raw))
	}
	out["raw_json"] = "{}"
	out["raw_json_policy"] = policy
	return out
}

func hardTrimKafkaPayload(payload map[string]any) map[string]any {
	out := make(map[string]any, len(payload)+4)
	for k, v := range payload {
		out[k] = v
	}
	for _, key := range []string{"description", "question", "title"} {
		if s := SafeString(out[key]); len([]byte(s)) > 8192 {
			out[key] = TrimBody(s, 8192)
			out[key+"_truncated"] = true
		}
	}
	out["payload_policy"] = "large_text_fields_trimmed_after_raw_json_omission"
	return out
}

func emergencyTrimKafkaPayload(payload map[string]any) map[string]any {
	out := make(map[string]any, len(payload)+8)
	keep := []string{
		"event_id", "market_id", "series_id", "raw_key", "collected_at_utc", "created_at_utc", "updated_at_utc",
		"slug", "ticker", "title", "question", "condition_id", "question_id",
		"active", "approved", "archived", "closed", "restricted", "neg_risk",
		"start_date_utc", "end_date_utc", "closed_time_utc", "creation_date_utc",
		"best_ask", "best_bid", "last_trade_price", "spread", "liquidity", "volume", "volume_24h",
		"recurrence", "series_type", "series_slug", "resolved_by", "resolution_source",
	}
	for _, key := range keep {
		if value, ok := payload[key]; ok {
			out[key] = value
		}
	}
	for _, key := range []string{"title", "question"} {
		if s := SafeString(out[key]); len([]byte(s)) > 2048 {
			out[key] = TrimBody(s, 2048)
			out[key+"_truncated"] = true
		}
	}
	out["series_ids"] = []uint64{}
	out["market_ids"] = []uint64{}
	out["event_ids"] = []uint64{}
	out["outcomes"] = []string{}
	out["outcome_prices"] = []string{}
	out["clob_token_ids"] = []string{}
	out["raw_json"] = "{}"
	out["payload_policy"] = "emergency_minimal_payload_after_kafka_message_size_limit"
	return out
}

func limitKafkaPayloadArrays(payload map[string]any, maxItems int) map[string]any {
	if payload == nil {
		return payload
	}
	out := make(map[string]any, len(payload)+12)
	for k, v := range payload {
		out[k] = v
	}
	for _, key := range []string{"series_ids", "market_ids", "event_ids", "outcomes", "outcome_prices", "clob_token_ids"} {
		value, ok := out[key]
		if !ok {
			continue
		}
		trimmed, originalCount, didTrim := trimJSONCompatibleArray(value, maxItems)
		if !didTrim {
			continue
		}
		out[key] = trimmed
		out[key+"_original_count"] = originalCount
		out[key+"_truncated"] = true
		out["payload_policy"] = "large_array_fields_trimmed_for_kafka_message_size"
	}
	return out
}

func trimJSONCompatibleArray(value any, maxItems int) (any, int, bool) {
	if maxItems < 0 {
		maxItems = 0
	}
	switch arr := value.(type) {
	case []uint64:
		if len(arr) <= maxItems {
			return value, len(arr), false
		}
		out := append([]uint64(nil), arr[:maxItems]...)
		return out, len(arr), true
	case []string:
		if len(arr) <= maxItems {
			return value, len(arr), false
		}
		out := append([]string(nil), arr[:maxItems]...)
		return out, len(arr), true
	case []any:
		if len(arr) <= maxItems {
			return value, len(arr), false
		}
		out := append([]any(nil), arr[:maxItems]...)
		return out, len(arr), true
	default:
		return value, 0, false
	}
}

func cloneRowForKafka(entity string, row map[string]any) map[string]any {
	columns := baseInsertColumns[entity]
	if len(columns) == 0 {
		out := make(map[string]any, len(row))
		for k, v := range row {
			out[k] = v
		}
		return out
	}
	out := make(map[string]any, len(columns))
	for _, col := range columns {
		out[col] = row[col]
	}
	return out
}

func (i *Ingestor) rowEventURL(entity string, row map[string]any) string {
	id := entityIDString(entity, row)
	if id == "" {
		return i.cfg.Endpoint(entity)
	}
	return strings.TrimRight(i.cfg.Endpoint(entity), "/") + "/" + id
}

func polymarketEventType(entity string) string {
	switch entity {
	case "events":
		return "polymarket.event_snapshot_raw.v1"
	case "markets":
		return "polymarket.market_snapshot_raw.v1"
	case "series":
		return "polymarket.series_snapshot_raw.v1"
	default:
		return "polymarket.unknown_snapshot_raw.v1"
	}
}

func entityIDString(entity string, row map[string]any) string {
	switch entity {
	case "events":
		return SafeString(row["event_id"])
	case "markets":
		return SafeString(row["market_id"])
	case "series":
		return SafeString(row["series_id"])
	default:
		return ""
	}
}

func (i *Ingestor) FlushEntityRows(ctx context.Context, entity string, buffer *[]map[string]any, force bool) error {
	if buffer == nil || len(*buffer) == 0 {
		return nil
	}
	batchSize := i.cfg.InsertBatchSizeForEntity(entity)
	for len(*buffer) >= batchSize || (force && len(*buffer) > 0) {
		take := batchSize
		if len(*buffer) < take {
			take = len(*buffer)
		}
		batch := append([]map[string]any(nil), (*buffer)[:take]...)
		*buffer = (*buffer)[take:]
		events := make([]predictionKafkaEvent, 0, len(batch))
		for _, row := range batch {
			ev, err := i.rowEvent(entity, row)
			if err != nil {
				return err
			}
			events = append(events, ev)
		}
		if err := i.publishKafkaEvents(ctx, events); err != nil {
			return err
		}
		fmt.Printf("[kafka] entity=%s published=%d topic=%s\n", entity, len(events), i.cfg.KafkaTopic)
	}
	return nil
}
