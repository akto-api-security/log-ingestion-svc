package storage

import "context"

type LogStorage interface {
	StoreLogs(ctx context.Context, accountID string, logs []map[string]interface{}) error
}
