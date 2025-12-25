package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type ElasticsearchStorage struct {
	elasticsearchClient *elasticsearch.Client
	indexer             esutil.BulkIndexer
}

// NewElasticsearchStorage creates a storage backed by esutil.BulkIndexer which handles batching and concurrency internally.
// Reference : https://pkg.go.dev/github.com/elastic/go-elasticsearch/v8/esutil#NewBulkIndexer
func NewElasticsearchStorage(elasticsearchClient *elasticsearch.Client) *ElasticsearchStorage {
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        elasticsearchClient,
		NumWorkers:    runtime.NumCPU(),
		FlushBytes:    5 << 20, // 5MB
		FlushInterval: 2 * time.Second,
	})
	if err != nil {
		log.Fatalf("failed to create bulk indexer: %v", err)
	}

	return &ElasticsearchStorage{
		elasticsearchClient: elasticsearchClient,
		indexer:             bi,
	}
}

func (es *ElasticsearchStorage) StoreLogs(ctx context.Context, tokenAccountID string, logs []map[string]interface{}) error {
	timestamp := time.Now().Format(time.RFC3339)
	marshalErrCount := 0

	for _, logEntry := range logs {
		logAccountID := extractAccountIdFromLog(logEntry)
		containerName := extractContainerName(logEntry)

		// Log the received log entry before attempting to marshal/index it.
		// This helps debug what arrives at the server prior to ES insertion.
		if tokenAccountID == "1756844701" {
			log.Printf("received log: token_account=%s extracted_account=%s container=%s entry=%+v", tokenAccountID, logAccountID, containerName, logEntry)
		}

		logEntry["token_accountId"] = tokenAccountID
		logEntry["@timestamp"] = timestamp

		indexName := buildIndexName(containerName)

		body, err := json.Marshal(logEntry)
		if err != nil {
			// Count marshal failures and continue processing other logs.
			log.Printf("warning: failed to marshal log entry: %v", err)
			marshalErrCount++
			continue
		}

		// Make a copy of the marshaled body so the OnSuccess closure can reference
		// the exact bytes that were enqueued (the original slice may be reused).
		bodyCopy := make([]byte, len(body))
		copy(bodyCopy, body)

		item := esutil.BulkIndexerItem{
			Action: "create",
			Index:  indexName,
			Body:   bytes.NewReader(bodyCopy),
			OnSuccess: func(callbackCtx context.Context, item esutil.BulkIndexerItem, resp esutil.BulkIndexerResponseItem) {
				// Log the successfully indexed document (index, status and the document body)
				if tokenAccountID == "1756844701" {
					log.Printf("successfully indexed log to container index %s", item.Index)
					log.Printf("Success : Log inserted - index=%s status=%d doc=%s", item.Index, resp.Status, string(bodyCopy))
				}

			},
			OnFailure: func(callbackCtx context.Context, item esutil.BulkIndexerItem, resp esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					log.Printf("bulk indexer failure (err): %v", err)
				} else {
					// resp contains status and error body
					if resp.Error.Type != "" {
						log.Printf("bulk indexer item failed: index=%s status=%d error=%+v", item.Index, resp.Status, resp.Error)
					}
				}

				log.Printf("Failure : Log not inserted - index=%s status=%d doc=%s", item.Index, resp.Status, string(bodyCopy))
			},
		}

		if err := es.indexer.Add(ctx, item); err != nil {
			log.Printf("warning: bulk indexer Add error: %v", err)
			return err
		}
	}

	if marshalErrCount > 0 {
		return fmt.Errorf("%d log entries failed to marshal", marshalErrCount)
	}

	return nil
}

func (es *ElasticsearchStorage) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := es.indexer.Close(ctx); err != nil {
		return fmt.Errorf("failed to close bulk indexer: %w", err)
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

// extractContainerName extracts the container name from the log entry
func extractContainerName(logEntry map[string]interface{}) string {
	// Try top-level container_name first (Docker logs metadata)
	if v, ok := logEntry["container_name"].(string); ok && v != "" {
		return v
	}
	// Try nested kubernetes.container_name (K8s logs metadata)
	if k8s, ok := logEntry["kubernetes"].(map[string]interface{}); ok {
		if v, ok := k8s["container_name"].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// buildIndexName creates the Elasticsearch index name from container name
// Uses logs-<type>-<container> pattern to match existing logs-*-* template
func buildIndexName(containerName string) string {
	if containerName != "" {
		// Sanitize container name for ES index naming (lowercase, replace invalid chars)
		// ES index names must be lowercase and not contain: \, /, *, ?, ", <, >, |, ` ` (space), ,, #
		sanitized := sanitizeIndexName(containerName)
		return fmt.Sprintf("logs-containers-%s", sanitized)
	}
	// Default index if no container name found
	return "logs-containers-default"
}

// sanitizeIndexName ensures the name is valid for Elasticsearch indices
func sanitizeIndexName(name string) string {
	// Convert to lowercase and replace invalid characters with hyphens
	result := ""
	for _, ch := range name {
		switch {
		case ch >= 'a' && ch <= 'z':
			result += string(ch)
		case ch >= 'A' && ch <= 'Z':
			result += string(ch - 'A' + 'a')
		case ch >= '0' && ch <= '9':
			result += string(ch)
		case ch == '-' || ch == '_' || ch == '.':
			result += string(ch)
		default:
			result += "-"
		}
	}
	return result
}
