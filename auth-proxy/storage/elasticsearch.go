package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ElasticsearchStorage struct {
	client  *http.Client
	baseURL string
}

func NewElasticsearchStorage(baseURL string) *ElasticsearchStorage {
	return &ElasticsearchStorage{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		baseURL: baseURL,
	}
}

func (es *ElasticsearchStorage) StoreLogs(ctx context.Context, accountID string, logs []map[string]interface{}) error {
	if len(logs) == 0 {
		return nil
	}

	var bulkBody bytes.Buffer
	indexName := fmt.Sprintf("account-%s-logs", accountID)
	timestamp := time.Now().Format(time.RFC3339)

	for _, logEntry := range logs {
		logEntry["account_id"] = accountID
		logEntry["@timestamp"] = timestamp

		action := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
			},
		}
		actionJSON, _ := json.Marshal(action)
		bulkBody.Write(actionJSON)
		bulkBody.WriteByte('\n')

		logJSON, _ := json.Marshal(logEntry)
		bulkBody.Write(logJSON)
		bulkBody.WriteByte('\n')
	}

	url := fmt.Sprintf("%s/_bulk", es.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, &bulkBody)
	if err != nil {
		return fmt.Errorf("failed to create bulk request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := es.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send bulk request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("elasticsearch bulk API returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
