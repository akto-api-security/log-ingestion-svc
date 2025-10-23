package auth

import "context"

type Validator interface {
	Validate(ctx context.Context, token string) (*Claims, error)
}
