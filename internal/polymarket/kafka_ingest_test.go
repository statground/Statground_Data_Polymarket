package polymarket

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestFixedPartitionBalancerUsesRequestedPartition(t *testing.T) {
	balancer := fixedPartitionBalancer{partition: 7}
	if got := balancer.Balance(kafka.Message{}, 0, 3, 7, 9); got != 7 {
		t.Fatalf("fixed partition = %d, want 7", got)
	}
	if got := balancer.Balance(kafka.Message{}, 1, 2, 3); got != 1 {
		t.Fatalf("fallback partition = %d, want first available partition", got)
	}
}

func TestSplitIntCSVSortsAndDeduplicates(t *testing.T) {
	got := splitIntCSV("5, 1, bad, -1, 5, 3")
	want := []int{1, 3, 5}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d: %#v", len(got), len(want), got)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("splitIntCSV[%d] = %d, want %d", idx, got[idx], want[idx])
		}
	}
}

func TestShouldUsePartitionFallbackForLeaderMetadataErrors(t *testing.T) {
	err := errors.New(`kafka.(*Client).Produce: fetch request error: topic partition has no leader (topic="prediction.events" partition=2)`)
	if !shouldUsePartitionFallback(err) {
		t.Fatal("expected leader metadata error to use fixed partition fallback")
	}
	if shouldUsePartitionFallback(errors.New("connection reset by peer")) {
		t.Fatal("network errors should use normal retry path")
	}
}

func TestRetryableKafkaWriteErrorTreatsAttemptDeadlineAsRetryable(t *testing.T) {
	if !retryableKafkaWriteErrorForContext(context.Background(), context.DeadlineExceeded) {
		t.Fatal("attempt-scoped write deadline should be retryable while parent context is alive")
	}

	parent, cancel := context.WithCancel(context.Background())
	cancel()
	if retryableKafkaWriteErrorForContext(parent, context.DeadlineExceeded) {
		t.Fatal("deadline should not be retryable after parent context is already stopped")
	}
}

func TestRetryableFailedMessagesForContextKeepsDeadlineFailedSubset(t *testing.T) {
	messages := []kafka.Message{
		{Key: []byte("ok")},
		{Key: []byte("retry")},
	}
	err := kafka.WriteErrors{nil, context.DeadlineExceeded}

	failed, retryable := retryableFailedMessagesForContext(context.Background(), messages, err)
	if !retryable {
		t.Fatal("expected attempt-scoped deadline in write errors to be retryable")
	}
	if len(failed) != 1 || string(failed[0].Key) != "retry" {
		t.Fatalf("failed messages = %#v, want only retry key", failed)
	}
}
