package polymarket

import "strings"

func ingestCursorKey(entity string) string {
	return "__ingest_resume_cursor_" + entity
}

func ingestCursorCheckpointKey(entity string) string {
	return "__ingest_resume_checkpoint_" + entity
}

func ingestCursorOrderKey(entity string) string {
	return "__ingest_resume_order_" + entity
}

func ingestBestSeenKey(entity string) string {
	return "__ingest_resume_best_seen_" + entity
}

func canResumeIngestCursor(state map[string]string, entity string, checkpoint string, order string) bool {
	if state == nil {
		return false
	}
	cursor := strings.TrimSpace(state[ingestCursorKey(entity)])
	if cursor == "" {
		return false
	}
	if state[ingestCursorCheckpointKey(entity)] != checkpoint {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(state[ingestCursorOrderKey(entity)]), strings.TrimSpace(order)) {
		return false
	}
	return true
}

func setIngestResumeState(state map[string]string, entity string, checkpoint string, order string, cursor string, bestSeen string) {
	if state == nil {
		return
	}
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		clearIngestResumeState(state, entity)
		return
	}
	state[ingestCursorKey(entity)] = cursor
	state[ingestCursorCheckpointKey(entity)] = checkpoint
	state[ingestCursorOrderKey(entity)] = order
	if strings.TrimSpace(bestSeen) != "" {
		state[ingestBestSeenKey(entity)] = bestSeen
	}
}

func clearIngestResumeState(state map[string]string, entity string) {
	if state == nil {
		return
	}
	delete(state, ingestCursorKey(entity))
	delete(state, ingestCursorCheckpointKey(entity))
	delete(state, ingestCursorOrderKey(entity))
	delete(state, ingestBestSeenKey(entity))
}
