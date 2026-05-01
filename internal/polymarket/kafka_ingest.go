package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
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
	for _, partition := range partitions {
		leaderHost := strings.TrimSpace(partition.Leader.Host)
		if isLoopbackHost(leaderHost) {
			return fmt.Errorf("kafka broker metadata advertises loopback listener %s:%d for topic=%s partition=%d", leaderHost, partition.Leader.Port, partition.Topic, partition.ID)
		}
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
	w := &kafka.Writer{
		Addr:                   kafka.TCP(i.cfg.KafkaBrokers...),
		Topic:                  i.cfg.KafkaTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchSize:              i.cfg.KafkaBatchSize,
		BatchTimeout:           i.cfg.KafkaBatchTimeout,
	}
	if strings.TrimSpace(i.cfg.KafkaClientID) != "" || strings.TrimSpace(i.cfg.KafkaUsername) != "" {
		transport := &kafka.Transport{ClientID: i.cfg.KafkaClientID}
		if strings.TrimSpace(i.cfg.KafkaUsername) != "" || strings.TrimSpace(i.cfg.KafkaPassword) != "" {
			transport.SASL = plain.Mechanism{Username: i.cfg.KafkaUsername, Password: i.cfg.KafkaPassword}
		}
		w.Transport = transport
	}
	return w
}

func (i *Ingestor) publishKafkaEvents(ctx context.Context, events []predictionKafkaEvent) error {
	if len(events) == 0 {
		return nil
	}
	w := i.kafkaWriter()
	defer w.Close()

	messages := make([]kafka.Message, 0, len(events))
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
	}
	return w.WriteMessages(ctx, messages...)
}

func kafkaEventKey(ev predictionKafkaEvent) string {
	if strings.TrimSpace(ev.URL) != "" {
		return ev.EventType + ":" + ev.URL
	}
	return ev.EventType + ":" + ev.EventUUID
}

func (i *Ingestor) rowEvent(entity string, row map[string]any) (predictionKafkaEvent, error) {
	payload := cloneRowForKafka(entity, row)
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
	return predictionKafkaEvent{
		EventUUID: eventUUID,
		Source:    i.cfg.ProducerSource,
		Host:      i.kafkaHost(),
		UUIDUser:  "",
		IP:        i.cfg.ProducerIP,
		URL:       i.rowEventURL(entity, row),
		EventType: polymarketEventType(entity),
		Payload:   string(payloadJSON),
		CreatedAt: createdAt,
	}, nil
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
