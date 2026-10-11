package events

import (
	"strconv"
	"time"
)

const (
	EntryTypePosted = "entry.posted"
)

// Payload for the outbox entry event
type EntryPosted struct {
	AccountID       int64     `json:"account_id"`
	TransactionID   int64     `json:"transaction_id"`
	TransactionType string    `json:"transaction_type"`
	Amount          int64     `json:"amount"`
	Currency        string    `json:"currency"`
	CounterpartyID  int64     `json:"counterparty_id"`
	ReversalOf      *int64    `json:"reversal_of,omitempty"`
	OccuredAt       time.Time `json:"occured_at"`
}

// Key returns the Kafka partition key: all events of one account
// go to the same partition and keep their order.
func (e EntryPosted) Key() string {
	return strconv.FormatInt(e.AccountID, 10)
}
