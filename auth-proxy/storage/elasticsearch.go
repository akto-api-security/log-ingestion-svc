package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type ElasticsearchStorage struct {
	elasticsearchClient *elasticsearch.Client
}

func NewElasticsearchStorage(elasticsearchClient *elasticsearch.Client) *ElasticsearchStorage {
	return &ElasticsearchStorage{
		elasticsearchClient: elasticsearchClient,
	}
}

func (elasticsearchStorage *ElasticsearchStorage) StoreLogs(ctx context.Context, tokenAccountID string, logs []map[string]interface{}) error {
	if len(logs) == 0 {
		return nil
	}

	var bulkRequestBody bytes.Buffer
	timestamp := time.Now().Format(time.RFC3339)

	// Track unique data stream names to validate existence once
	dataStreamSet := make(map[string]struct{})

	for _, logEntry := range logs {
		// Extract account ID from log entry
		logAccountID := extractAccountIdFromLog(logEntry)

		// Choose effective account according to rules
		effectiveAccountID := chooseEffectiveAccountID(logAccountID, tokenAccountID)

		// Add timestamp and account IDs to log entry
		logEntry["token_accountId"] = tokenAccountID
		logEntry["@timestamp"] = timestamp

		// Data stream name
		indexName := fmt.Sprintf("logs-account-%s", effectiveAccountID)
		dataStreamSet[indexName] = struct{}{}

		// Create bulk action
		action := map[string]interface{}{
			// Use create op; will fail if not creatable/allowed
			"create": map[string]interface{}{
				"_index": indexName,
			},
		}
		actionJSON, _ := json.Marshal(action)
		bulkRequestBody.Write(actionJSON)
		bulkRequestBody.WriteByte('\n')

		logJSON, _ := json.Marshal(logEntry)
		bulkRequestBody.Write(logJSON)
		bulkRequestBody.WriteByte('\n')
	}

	// Send bulk request to Elasticsearch using esapi
	bulkRequest := esapi.BulkRequest{
		Body: &bulkRequestBody,
	}

	bulkResponse, err := bulkRequest.Do(ctx, elasticsearchStorage.elasticsearchClient)
	if err != nil {
		return fmt.Errorf("failed to execute bulk request: %w", err)
	}
	defer bulkResponse.Body.Close()

	if bulkResponse.IsError() {
		bodyBytes, _ := io.ReadAll(bulkResponse.Body)
		// Just log the server message (e.g., data stream not found) and return a concise error
		log.Printf("bulk indexing failed: status=%s body=%s", bulkResponse.Status(), string(bodyBytes))
		return fmt.Errorf("bulk request failed: %s", bulkResponse.Status())
	}

	// Check for individual item errors in bulk response
	var bulkResponseMap map[string]interface{}
	if err := json.NewDecoder(bulkResponse.Body).Decode(&bulkResponseMap); err != nil {
		log.Printf("warning: failed to decode bulk response: %v", err)
	} else if errors, ok := bulkResponseMap["errors"].(bool); ok && errors {
		log.Printf("warning: some bulk items failed, check response: %+v", bulkResponseMap)
	}

	return nil
}

// extractAccountIdFromLog extracts account ID from log entry - handles string or number types
func extractAccountIdFromLog(logEntry map[string]interface{}) string {
	if v, ok := logEntry["log_account_id"].(string); ok {
		return v
	}
	return ""
}

// chooseEffectiveAccountID applies selection rules to determine the effective account ID
// Rules:
// 1) if logAccountID == "1000000" -> use tokenAccountID
// 2) else if logAccountID == tokenAccountID -> use tokenAccountID
// 3) else if logAccountID != "" -> use logAccountID
// 4) else use tokenAccountID
func chooseEffectiveAccountID(logAccountID, tokenAccountID string) string {
	if logAccountID == "1000000" || logAccountID == "" {
		return tokenAccountID
	}
	if logAccountID != tokenAccountID {
		return logAccountID
	}
	return tokenAccountID
}
