package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type logItem struct {
	tokenAccountID string
	logEntry       map[string]interface{}
}

type ElasticsearchStorage struct {
	elasticsearchClient *elasticsearch.Client
	logQueue            chan logItem
	flushInterval       time.Duration
	batchSize           int
	wg                  sync.WaitGroup
	ctx                 context.Context
	cancel              context.CancelFunc
}

func NewElasticsearchStorage(elasticsearchClient *elasticsearch.Client) *ElasticsearchStorage {
	ctx, cancel := context.WithCancel(context.Background())
	es := &ElasticsearchStorage{
		elasticsearchClient: elasticsearchClient,
		logQueue:            make(chan logItem, 10000),
		flushInterval:       2 * time.Second,
		batchSize:           500,
		ctx:                 ctx,
		cancel:              cancel,
	}
	es.wg.Add(1)
	go es.bulkWriter()
	return es
}

func (es *ElasticsearchStorage) StoreLogs(ctx context.Context, tokenAccountID string, logs []map[string]interface{}) error {
	for _, logEntry := range logs {
		select {
		case es.logQueue <- logItem{tokenAccountID: tokenAccountID, logEntry: logEntry}:
		case <-ctx.Done():
			return ctx.Err()
		default:
			log.Printf("warning: log queue full, dropping log")
		}
	}
	return nil
}

func (es *ElasticsearchStorage) bulkWriter() {
	defer es.wg.Done()
	ticker := time.NewTicker(es.flushInterval) // Timer that fires every 2 seconds
	defer ticker.Stop()

	var batch []logItem // Accumulator slice

	for {
		select {
		case item := <-es.logQueue: // Received a log from channel
			batch = append(batch, item)     // Add to batch
			if len(batch) >= es.batchSize { // Batch full (500 logs)?
				es.wg.Add(1)            // Track the flush goroutine
				go es.flushAsync(batch) // ASYNC: Launch goroutine to write to ES
				batch = nil             // Reset batch (important: don't reuse slice)
			}
		case <-ticker.C: // 2 seconds elapsed
			if len(batch) > 0 { // Have logs to flush?
				es.wg.Add(1)            // Track the flush goroutine
				go es.flushAsync(batch) // ASYNC: Launch goroutine to write to ES
				batch = nil             // Reset batch
			}
		case <-es.ctx.Done(): // Shutdown signal received
			if len(batch) > 0 { // Flush any remaining logs
				es.wg.Add(1)            // Track the flush goroutine
				go es.flushAsync(batch) // ASYNC: Launch goroutine to write to ES
			}
			return // Exit goroutine
		}
	}
}

func (es *ElasticsearchStorage) flushAsync(batch []logItem) {
	defer es.wg.Done()
	es.flush(batch)
}

func (es *ElasticsearchStorage) flush(batch []logItem) {
	if len(batch) == 0 {
		return
	}

	var buf bytes.Buffer
	timestamp := time.Now().Format(time.RFC3339)

	for _, item := range batch {
		logAccountID := extractAccountIdFromLog(item.logEntry)
		effectiveAccountID := chooseEffectiveAccountID(logAccountID, item.tokenAccountID)

		item.logEntry["token_accountId"] = item.tokenAccountID
		item.logEntry["@timestamp"] = timestamp

		indexName := fmt.Sprintf("logs-account-%s", effectiveAccountID)

		action := map[string]interface{}{
			"create": map[string]interface{}{
				"_index": indexName,
			},
		}
		actionJSON, _ := json.Marshal(action)
		buf.Write(actionJSON)
		buf.WriteByte('\n')

		logJSON, _ := json.Marshal(item.logEntry)
		buf.Write(logJSON)
		buf.WriteByte('\n')
	}

	req := esapi.BulkRequest{Body: &buf}
	if err := es.executeBulk(req); err != nil {
		log.Printf("bulk write failed: %v", err)
	}
}

func (es *ElasticsearchStorage) executeBulk(req esapi.BulkRequest) error {
	resp, err := req.Do(es.ctx, es.elasticsearchClient)
	if err != nil {
		return fmt.Errorf("bulk request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("bulk indexing failed: status=%s body=%s", resp.Status(), string(bodyBytes))
		return fmt.Errorf("bulk request failed: %s", resp.Status())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("warning: failed to decode bulk response: %v", err)
	} else if errors, ok := result["errors"].(bool); ok && errors {
		log.Printf("warning: some bulk items failed: %+v", result)
	}

	return nil
}

func (es *ElasticsearchStorage) Close() error {
	es.cancel()
	es.wg.Wait()
	close(es.logQueue)
	return nil
}

// extractAccountIdFromLog extracts account ID from log entry - handles string or number types
func extractAccountIdFromLog(logEntry map[string]interface{}) string {
	if v, ok := logEntry["log_account_id"].(string); ok {
		return v
	}
	return ""
}

func chooseEffectiveAccountID(logAccountID, tokenAccountID string) string {
	if logAccountID == "1000000" || logAccountID == "" {
		return tokenAccountID
	}
	if logAccountID != tokenAccountID {
		return logAccountID
	}
	return tokenAccountID
}
