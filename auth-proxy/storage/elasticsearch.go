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

		// apply selection rules:
		// 1) if logAccount == "1000000" -> use 1000000 (go to datastream 1000000)
		// 2) else if logAccount == tokenAccount -> use token/log (either is fine)
		// 3) else -> use logAccount
		effectiveAccount := accountID // Default to token account
		if logAccount != "" {
			if logAccount == "1000000" {
				// Rule 1: If log account is 1000000, use it
				effectiveAccount = "1000000"
			} else if logAccount == accountID {
				// Rule 2: If they match, use either (we'll use token)
				effectiveAccount = accountID
			} else {
				// Rule 3: If they differ and log is not 1000000, use log account
				effectiveAccount = logAccount
			}
		}

		// add both columns requested
		logEntry["token_accountId"] = accountID
		logEntry["log_accountId"] = logAccount
		logEntry["@timestamp"] = timestamp

		// datastream name derived from effective account selection
		dsName := fmt.Sprintf("account-%s-logs", effectiveAccount)
		dsSet[dsName] = struct{}{}

		// For datastreams, must use "create" operation, not "index"
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
		// First, ensure index template exists for this datastream pattern
		templateName := fmt.Sprintf("%s-template", ds)
		templateURL := fmt.Sprintf("%s/_index_template/%s", es.baseURL, templateName)

		// Check if template exists
		checkReq, err := http.NewRequestWithContext(ctx, "HEAD", templateURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create request to check index template: %w", err)
		}
		checkResp, err := es.client.Do(checkReq)
		if err != nil {
			return fmt.Errorf("failed to execute request to check index template: %w", err)
		}
		if checkResp != nil {
			checkResp.Body.Close()
		}

		// Create template if it doesn't exist (404)
		if checkResp == nil || checkResp.StatusCode == 404 {
			template := map[string]interface{}{
				"index_patterns": []string{ds},
				"data_stream":    map[string]interface{}{},
				"template": map[string]interface{}{
					"mappings": map[string]interface{}{
						"properties": map[string]interface{}{
							"@timestamp": map[string]interface{}{
								"type": "date",
							},
							"message": map[string]interface{}{
								"type": "text",
								"fields": map[string]interface{}{
									"keyword": map[string]interface{}{
										"type":         "keyword",
										"ignore_above": 256,
									},
								},
							},
							"log": map[string]interface{}{
								"type": "text",
							},
							"log_message": map[string]interface{}{
								"type": "text",
							},
							"token_accountId": map[string]interface{}{
								"type": "keyword",
							},
							"log_accountId": map[string]interface{}{
								"type": "keyword",
							},
							"account_id": map[string]interface{}{
								"type": "keyword",
							},
							"container_name": map[string]interface{}{
								"type": "keyword",
							},
							"container_id": map[string]interface{}{
								"type": "keyword",
							},
							"source": map[string]interface{}{
								"type": "keyword",
							},
							"date": map[string]interface{}{
								"type": "long",
							},
						},
					},
				},
			}
			templateJSON, err := json.Marshal(template)
			if err != nil {
				log.Printf("failed to marshal template for %s: %v", templateName, err)
				continue
			}
			templateReq, err := http.NewRequestWithContext(ctx, "PUT", templateURL, bytes.NewReader(templateJSON))
			if err != nil {
				log.Printf("failed to create template request for %s: %v", ds, err)
				continue
			}
			templateReq.Header.Set("Content-Type", "application/json")
			templateResp, err := es.client.Do(templateReq)
			if err != nil {
				log.Printf("failed to create template %s: %v", templateName, err)
				continue
			}
			if templateResp.StatusCode >= 200 && templateResp.StatusCode < 300 {
				// Template created successfully
			} else {
				body, err := io.ReadAll(templateResp.Body)
				if err != nil {
					log.Printf("Failed to create template %s (status %d): could not read response body: %v", templateName, templateResp.StatusCode, err)
				} else {
					log.Printf("Failed to create template %s (status %d): %s", templateName, templateResp.StatusCode, string(body))
				}
			}
			templateResp.Body.Close()
		}

		// Now create the datastream
		putURL := fmt.Sprintf("%s/_data_stream/%s", es.baseURL, ds)
		req, err := http.NewRequestWithContext(ctx, "PUT", putURL, nil)
		if err != nil {
			// non-fatal
			log.Printf("failed to create data stream request for %s: %v", ds, err)
			continue
		}
		resp, err := es.client.Do(req)
		if err != nil {
			log.Printf("failed to create data stream %s: %v", ds, err)
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Data stream created successfully
		} else if resp.StatusCode != 400 { // 400 means already exists
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Failed to create data stream %s (status %d): could not read response body: %v", ds, resp.StatusCode, err)
			} else {
				log.Printf("Failed to create data stream %s (status %d): %s", ds, resp.StatusCode, string(body))
			}
		}
		resp.Body.Close()

		// Always try to create Kibana index pattern (even if datastream already exists)
		es.createKibanaIndexPattern(ctx, ds)
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
