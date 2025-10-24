package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type ElasticsearchStorage struct {
	client      *http.Client
	baseURL     string
	batchChan   chan *logBatch
	batchSize   int
	flushTicker *time.Ticker
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

type logBatch struct {
	accountID string
	logs      []map[string]interface{}
}

func NewElasticsearchStorage(baseURL string) *ElasticsearchStorage {
	ctx, cancel := context.WithCancel(context.Background())
	es := &ElasticsearchStorage{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		baseURL:     baseURL,
		batchChan:   make(chan *logBatch, 1000), // Buffer for 1000 batches
		batchSize:   100,                        // Batch 100 logs together
		flushTicker: time.NewTicker(1 * time.Second),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Start background workers
	for i := 0; i < 5; i++ { // 5 concurrent workers
		es.wg.Add(1)
		go es.worker()
	}

	es.wg.Add(1)
	go es.batcher()

	return es
}

func (es *ElasticsearchStorage) worker() {
	defer es.wg.Done()
	for {
		select {
		case <-es.ctx.Done():
			return
		case batch := <-es.batchChan:
			if batch != nil {
				if err := es.bulkWrite(batch.accountID, batch.logs); err != nil {
					log.Printf("Failed to write batch: %v", err)
				}
			}
		}
	}
}

var (
	pendingBatches = make(map[string][]map[string]interface{})
	batchMutex     sync.Mutex
)

func (es *ElasticsearchStorage) batcher() {
	defer es.wg.Done()
	for {
		select {
		case <-es.ctx.Done():
			es.flushAll()
			return
		case <-es.flushTicker.C:
			es.flushAll()
		}
	}
}

func (es *ElasticsearchStorage) flushAll() {
	batchMutex.Lock()
	defer batchMutex.Unlock()

	for accountID, logs := range pendingBatches {
		if len(logs) > 0 {
			select {
			case es.batchChan <- &logBatch{accountID: accountID, logs: logs}:
				delete(pendingBatches, accountID)
			default:
				log.Printf("Batch channel full, dropping batch for account %s", accountID)
			}
		}
	}
}

func (es *ElasticsearchStorage) StoreLogs(ctx context.Context, accountID string, logs []map[string]interface{}) error {
	if len(logs) == 0 {
		return nil
	}

	batchMutex.Lock()
	defer batchMutex.Unlock()

	// Add logs to pending batch
	pendingBatches[accountID] = append(pendingBatches[accountID], logs...)

	// Flush if batch size reached
	if len(pendingBatches[accountID]) >= es.batchSize {
		batch := pendingBatches[accountID]
		delete(pendingBatches, accountID)

		// Send async (non-blocking)
		select {
		case es.batchChan <- &logBatch{accountID: accountID, logs: batch}:
		default:
			log.Printf("Batch channel full, queueing for next flush")
			pendingBatches[accountID] = batch
		}
	}

	return nil
}

func (es *ElasticsearchStorage) bulkWrite(accountID string, logs []map[string]interface{}) error {
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
		actionJSON, _ := json.Marshal(action)
		bulkBody.Write(actionJSON)
		bulkBody.WriteByte('\n')

		logJSON, _ := json.Marshal(logEntry)
		bulkBody.Write(logJSON)
		bulkBody.WriteByte('\n')
	}

	url := fmt.Sprintf("%s/_bulk", es.baseURL)
	req, err := http.NewRequestWithContext(es.ctx, "POST", url, &bulkBody)
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

	log.Printf("Successfully wrote %d logs for account %s", len(logs), accountID)
	return nil
}

func (es *ElasticsearchStorage) Close() error {
	es.cancel()
	es.flushTicker.Stop()
	es.wg.Wait()
	close(es.batchChan)
	return nil
}
