package polymarket

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type StateStore interface {
	LoadCheckpoint(ctx context.Context) (map[string]string, error)
	SaveCheckpoint(ctx context.Context, checkpoint map[string]string) error
}

type GitHubStateStore struct {
	cfg    *Config
	client *HTTPJSONClient
}

type LocalFileStateStore struct {
	cfg *Config
}

func NewStateStore(cfg *Config) StateStore {
	if strings.TrimSpace(cfg.GHToken) != "" {
		return &GitHubStateStore{
			cfg:    cfg,
			client: NewHTTPJSONClient(30*time.Second, 10*time.Second, "statground-polymarket-state"),
		}
	}
	return &LocalFileStateStore{cfg: cfg}
}

type githubContentResponse struct {
	SHA     string `json:"sha"`
	Content string `json:"content"`
}

func (s *GitHubStateStore) LoadCheckpoint(ctx context.Context) (map[string]string, error) {
	_, content, err := s.getContent(ctx, s.cfg.CheckpointPath)
	if err != nil {
		return nil, err
	}
	if len(content) == 0 {
		return map[string]string{}, nil
	}
	var checkpoint map[string]string
	if err := json.Unmarshal(content, &checkpoint); err != nil {
		return map[string]string{}, nil
	}
	return checkpoint, nil
}

func (s *GitHubStateStore) SaveCheckpoint(ctx context.Context, checkpoint map[string]string) error {
	ts := UTCNow().Format("2006-01-02 15:04:05 UTC")
	payload, err := marshalCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	return s.putContent(ctx, s.cfg.CheckpointPath, payload, fmt.Sprintf("Update Polymarket checkpoint (%s)", ts))
}

func (s *GitHubStateStore) getContent(ctx context.Context, path string) (string, []byte, error) {
	rawURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/contents/%s", s.cfg.Org, s.cfg.OrchestratorRepo, path)
	headers := map[string]string{"Accept": "application/vnd.github+json"}
	if s.cfg.GHToken != "" {
		headers["Authorization"] = "Bearer " + s.cfg.GHToken
	}
	data, _, err := s.client.DoJSON(ctx, http.MethodGet, rawURL, map[string]string{"ref": s.cfg.DefaultBranch}, headers, nil)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "status=404") {
			return "", nil, nil
		}
		return "", nil, err
	}
	raw := StringifyJSON(data)
	var resp githubContentResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return "", nil, err
	}
	cleaned := strings.ReplaceAll(resp.Content, "\n", "")
	if cleaned == "" {
		return resp.SHA, nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(cleaned)
	if err != nil {
		return "", nil, err
	}
	return resp.SHA, decoded, nil
}

func (s *GitHubStateStore) putContent(ctx context.Context, path string, content []byte, message string) error {
	sha, _, err := s.getContent(ctx, path)
	if err != nil {
		return err
	}

	bodyMap := map[string]any{
		"message": message,
		"content": base64.StdEncoding.EncodeToString(content),
		"branch":  s.cfg.DefaultBranch,
	}
	if sha != "" {
		bodyMap["sha"] = sha
	}
	rawBody, err := json.Marshal(bodyMap)
	if err != nil {
		return err
	}

	rawURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/contents/%s", s.cfg.Org, s.cfg.OrchestratorRepo, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, rawURL, bytes.NewReader(rawBody))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.GHToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.GHToken)
	}

	resp, err := s.client.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to put %s: status=%d body=%s", path, resp.StatusCode, TrimBody(string(responseBody), 500))
	}
	return nil
}

func (s *LocalFileStateStore) LoadCheckpoint(ctx context.Context) (map[string]string, error) {
	_ = ctx
	path := s.cfg.CheckpointAbsPath()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, err
	}
	var checkpoint map[string]string
	if err := json.Unmarshal(raw, &checkpoint); err != nil {
		return map[string]string{}, nil
	}
	return checkpoint, nil
}

func (s *LocalFileStateStore) SaveCheckpoint(ctx context.Context, checkpoint map[string]string) error {
	_ = ctx
	path := s.cfg.CheckpointAbsPath()
	payload, err := marshalCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	return WriteFileAtomic(path, payload)
}

func marshalCheckpoint(checkpoint map[string]string) ([]byte, error) {
	keys := SortedKeys(checkpoint)
	buf := &bytes.Buffer{}
	buf.WriteString("{\n")
	for idx, key := range keys {
		keyJSON, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		valJSON, err := json.Marshal(checkpoint[key])
		if err != nil {
			return nil, err
		}
		buf.WriteString("  ")
		buf.Write(keyJSON)
		buf.WriteString(": ")
		buf.Write(valJSON)
		if idx < len(keys)-1 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

func EnsureStatePath(cfg *Config) error {
	path := cfg.CheckpointAbsPath()
	return EnsureDir(filepath.Dir(path))
}
