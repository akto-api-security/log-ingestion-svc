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
	client    *http.Client
	baseURL   string
	kibanaURL string
}

func NewElasticsearchStorage(baseURL, kibanaURL string) *ElasticsearchStorage {
	return &ElasticsearchStorage{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		baseURL:   baseURL,
		kibanaURL: kibanaURL,
	}
}

func (es *ElasticsearchStorage) StoreLogs(ctx context.Context, accountID string, logs []map[string]interface{}) error {
	if len(logs) == 0 {
		return nil
	}

	var bulkBody bytes.Buffer
	timestamp := time.Now().Format(time.RFC3339)

	// collect unique datastream names to ensure existence
	dsSet := make(map[string]struct{})

	for _, logEntry := range logs {
		// extract account id from log (parser) - could be string or number
		logAccount := extractAccountIdFromLog(logEntry)

		// apply selection rules to choose the effective account ID
		effectiveAccount := chooseAccountId(logAccount, accountID)

		// add both columns requested
		logEntry["token_accountId"] = accountID
		logEntry["log_accountId"] = logAccount
		logEntry["@timestamp"] = timestamp

		// datastream name derived from effective account selection
		dsName := fmt.Sprintf("account-%s-logs", effectiveAccount)
		dsSet[dsName] = struct{}{}

		action := map[string]interface{}{
			"create": map[string]interface{}{
				"_index": dsName,
			},
		}
		actionJSON, _ := json.Marshal(action)
		bulkBody.Write(actionJSON)
		bulkBody.WriteByte('\n')

		logJSON, _ := json.Marshal(logEntry)
		bulkBody.Write(logJSON)
		bulkBody.WriteByte('\n')
	}

	// Create datastreams if they don't exist.
	for ds := range dsSet {
		if err := es.ensureDataStreamExists(ctx, ds); err != nil {
			return err
		}
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

func (es *ElasticsearchStorage) createKibanaIndexPattern(ctx context.Context, dataStreamName string) {
	// Kibana index pattern ID and name will be the datastream name
	indexPatternID := dataStreamName

	// Create the index pattern via Kibana API
	kibanaURL := fmt.Sprintf("%s/api/saved_objects/index-pattern/%s", es.kibanaURL, indexPatternID)

	indexPattern := map[string]interface{}{
		"attributes": map[string]interface{}{
			"title":         dataStreamName,
			"timeFieldName": "@timestamp",
		},
	}

	patternJSON, err := json.Marshal(indexPattern)
	if err != nil {
		log.Printf("failed to marshal Kibana index pattern for %s: %v", dataStreamName, err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, "POST", kibanaURL, bytes.NewReader(patternJSON))
	if err != nil {
		log.Printf("failed to create Kibana index pattern request for %s: %v", dataStreamName, err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("kbn-xsrf", "true")

	resp, err := es.client.Do(req)
	if err != nil {
		log.Printf("failed to create Kibana index pattern %s: %v", dataStreamName, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// Kibana index pattern created successfully
	} else if resp.StatusCode == 409 {
		// Already exists, that's fine
	} else {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Failed to create Kibana index pattern %s (status %d): could not read response body: %v", dataStreamName, resp.StatusCode, err)
		} else {
			log.Printf("Failed to create Kibana index pattern %s (status %d): %s", dataStreamName, resp.StatusCode, string(body))
		}
	}
}

// extractAccountIdFromLog extracts account ID from log entry - could be string or number
func extractAccountIdFromLog(logEntry map[string]interface{}) string {
	var logAccount string
	if v, ok := logEntry["account_id"]; ok && v != nil {
		switch t := v.(type) {
		case string:
			logAccount = t
		case float64:
			// JSON numbers are float64
			logAccount = fmt.Sprintf("%.0f", t)
		case int:
			logAccount = fmt.Sprintf("%d", t)
		case int64:
			logAccount = fmt.Sprintf("%d", t)
		default:
			logAccount = fmt.Sprintf("%v", t)
		}
	}
	return logAccount
}

// chooseAccountId applies selection rules to choose the effective account ID:
// 1) if logAccount == "1000000" -> use tokenAccount (go to datastream with token account)
// 2) else if logAccount == tokenAccount -> use tokenAccount (either is fine)
// 3) else -> use logAccount
func chooseAccountId(logAccount, tokenAccount string) string {
	effectiveAccount := tokenAccount // Default to token account
	if logAccount != "" {
		if logAccount == "1000000" {
			// Rule 1: If log account is 1000000, use token account
			effectiveAccount = tokenAccount
		} else if logAccount == tokenAccount {
			// Rule 2: If they match, use either (we'll use token)
			effectiveAccount = tokenAccount
		} else {
			// Rule 3: If they differ and log is not 1000000, use log account
			effectiveAccount = logAccount
		}
	}
	return effectiveAccount
}

// ensureDataStreamExists ensures that the datastream and its dependencies exist
func (es *ElasticsearchStorage) ensureDataStreamExists(ctx context.Context, ds string) error {
	// First, ensure index template exists for this datastream pattern
	templateName := fmt.Sprintf("%s-template", ds)
	templateExists, err := es.checkIndexTemplateExists(ctx, templateName)
	if err != nil {
		return fmt.Errorf("failed to check index template: %w", err)
	}

	if !templateExists {
		if err := es.createIndexTemplate(ctx, templateName, ds); err != nil {
			return fmt.Errorf("failed to create index template: %w", err)
		}
	}

	// Now create the datastream if it doesn't exist
	exists, err := es.checkDataStreamExists(ctx, ds)
	if err != nil {
		return fmt.Errorf("failed to check data stream: %w", err)
	}
	if !exists {
		if err := es.createDataStream(ctx, ds); err != nil {
			return fmt.Errorf("failed to create data stream: %w", err)
		}
	}

	// Always try to create Kibana index pattern (even if datastream already exists)
	es.createKibanaIndexPattern(ctx, ds)

	return nil
}

// checkIndexTemplateExists checks if an index template exists
func (es *ElasticsearchStorage) checkIndexTemplateExists(ctx context.Context, templateName string) (bool, error) {
	templateURL := fmt.Sprintf("%s/_index_template/%s", es.baseURL, templateName)
	checkReq, err := http.NewRequestWithContext(ctx, "HEAD", templateURL, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create request to check index template: %w", err)
	}

	checkResp, err := es.client.Do(checkReq)
	if err != nil {
		return false, fmt.Errorf("failed to execute request to check index template: %w", err)
	}
	if checkResp != nil {
		defer checkResp.Body.Close()
	}

	// 404 means template doesn't exist
	return checkResp != nil && checkResp.StatusCode != 404, nil
}

// createIndexTemplate creates an index template for the datastream
func (es *ElasticsearchStorage) createIndexTemplate(ctx context.Context, templateName, ds string) error {
	templateURL := fmt.Sprintf("%s/_index_template/%s", es.baseURL, templateName)
	templateJSON := []byte(fmt.Sprintf(elasticsearchTemplateJSON, ds))

	templateReq, err := http.NewRequestWithContext(ctx, "PUT", templateURL, bytes.NewReader(templateJSON))
	if err != nil {
		return fmt.Errorf("failed to create template request for %s: %w", ds, err)
	}
	templateReq.Header.Set("Content-Type", "application/json")

	templateResp, err := es.client.Do(templateReq)
	if err != nil {
		return fmt.Errorf("failed to create template %s: %w", templateName, err)
	}
	defer templateResp.Body.Close()

	if templateResp.StatusCode >= 200 && templateResp.StatusCode < 300 {
		// Template created successfully
		return nil
	}

	body, err := io.ReadAll(templateResp.Body)
	if err != nil {
		return fmt.Errorf("failed to create template %s (status %d): could not read response body: %w", templateName, templateResp.StatusCode, err)
	}
	return fmt.Errorf("failed to create template %s (status %d): %s", templateName, templateResp.StatusCode, string(body))
}

// checkDataStreamExists checks if a datastream exists
func (es *ElasticsearchStorage) checkDataStreamExists(ctx context.Context, ds string) (bool, error) {
	checkURL := fmt.Sprintf("%s/_data_stream/%s", es.baseURL, ds)
	checkReq, err := http.NewRequestWithContext(ctx, "HEAD", checkURL, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create request to check data stream: %w", err)
	}

	checkResp, err := es.client.Do(checkReq)
	if err != nil {
		return false, fmt.Errorf("failed to execute request to check data stream: %w", err)
	}
	if checkResp != nil {
		defer checkResp.Body.Close()
	}

	// 404 means datastream doesn't exist
	return checkResp != nil && checkResp.StatusCode != 404, nil
}

// createDataStream creates a datastream if it doesn't exist
func (es *ElasticsearchStorage) createDataStream(ctx context.Context, ds string) error {
	putURL := fmt.Sprintf("%s/_data_stream/%s", es.baseURL, ds)
	req, err := http.NewRequestWithContext(ctx, "PUT", putURL, nil)
	if err != nil {
		log.Printf("failed to create data stream request for %s: %v", ds, err)
		return nil // non-fatal
	}

	resp, err := es.client.Do(req)
	if err != nil {
		log.Printf("failed to create data stream %s: %v", ds, err)
		return nil // non-fatal
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// Data stream created successfully
		return nil
	}

	if resp.StatusCode == 400 {
		// 400 means already exists, that's fine
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to create data stream %s (status %d): could not read response body: %v", ds, resp.StatusCode, err)
		return nil
	}
	log.Printf("Failed to create data stream %s (status %d): %s", ds, resp.StatusCode, string(body))
	return nil
}
