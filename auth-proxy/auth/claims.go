package auth

import "fmt"

type Claims struct {
	AccountID int64  `json:"accountId"`
	Issuer    string `json:"iss,omitempty"`
	Subject   string `json:"sub,omitempty"`
	IssuedAt  int64  `json:"iat,omitempty"`
	ExpiresAt int64  `json:"exp,omitempty"`
}

func (c *Claims) GetAccountID() string {
	if c.AccountID != 0 {
		return fmt.Sprintf("%d", c.AccountID)
	}
	return ""
}
