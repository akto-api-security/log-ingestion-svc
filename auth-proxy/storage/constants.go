package storage

const elasticsearchTemplateJSON = `{
  "index_patterns": ["%s"],
  "data_stream": {},
  "template": {
    "mappings": {
      "properties": {
        "@timestamp": {"type": "date"},
        "message": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
        "token_accountId": {"type": "keyword"},
        "log_accountId": {"type": "keyword"},
        "account_id": {"type": "keyword"},
        "container_name": {"type": "keyword"},
        "container_id": {"type": "keyword"},
        "source": {"type": "keyword"},
        "date": {"type": "long"}
      }
    }
  }
}`
