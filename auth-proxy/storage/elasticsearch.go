package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	indexName := fmt.Sprintf("account-%s-logs-%s", accountID, time.Now().Format("2006.01.02"))
	timestamp := time.Now().Format(time.RFC3339)

	for _, logEntry := range logs {
		logEntry["account_id"] = accountID
		logEntry["@timestamp"] = timestamp

		action := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
			},
		}
		actionJSON, err := json.Marshal(action)
		if err != nil {
			log.Printf("Failed to marshal bulk action: %v", err)
			continue
		}
		bulkBody.Write(actionJSON)
		bulkBody.WriteByte('\n')

		logJSON, err := json.Marshal(logEntry)
		if err != nil {
			log.Printf("Failed to marshal log entry: %v", err)
			continue
		}
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

	var bulkResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&bulkResp); err != nil {
		log.Printf("Failed to parse bulk response: %v", err)
		return nil
	}

	if errors, ok := bulkResp["errors"].(bool); ok && errors {
		log.Printf("Bulk operation completed with some errors")
	}

	return nil
}
