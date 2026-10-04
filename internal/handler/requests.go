package handler

import (
	"time"

	"github.com/Velgorion/equilibra/internal/service"
)

type transferRequest struct {
	FromAccountID int64 `json:"from_account_id"`
	ToAccountID   int64 `json:"to_account_id"`
	Amount        int64 `json:"amount"`
}

type depositRequest struct {
	AccountID int64  `json:"account_id"`
	Amount    int64  `json:"amount"`
	Source    string `json:"source"`
}

type withdrawRequest struct {
	AccountID   int64  `json:"account_id"`
	Amount      int64  `json:"amount"`
	Destination string `json:"destination"`
}

type transactionResponse struct {
	TransactionID int64     `json:"transaction_id"`
	SourceID      int64     `json:"source_id"`
	DestinationID int64     `json:"destination_id"`
	Amount        int64     `json:"amount"`
	Status        string    `json:"status"`
	Type          string    `json:"type"`
	ReversalOf    *int64    `json:"reversal_of,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type AccountBalanceResponse struct {
	AccountID int64  `json:"account_id"`
	Currency  string `json:"currency"`
	Balance   int64  `json:"balance"`
}

type statementEntryResponse struct {
	EntryID        int64     `json:"entry_id"`
	TransactionID  int64     `json:"transaction_id"`
	Amount         int64     `json:"amount"`
	CounterpartyID int64     `json:"counterparty_id,omitempty"`
	Type           string    `json:"type"`
	ReversalOf     *int64    `json:"reversal_of,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type AccountHistoryResponse struct {
	Transactions []statementEntryResponse `json:"transactions"`
	NextCursor   int64                    `json:"next_cursor,omitempty"`
}

func newResponse(r *service.Result) transactionResponse {
	return transactionResponse{
		TransactionID: r.TransactionID,
		SourceID:      r.SourceID,
		DestinationID: r.DestinationID,
		Amount:        r.Amount,
		Status:        r.Status,
		Type:          r.Type,
		ReversalOf:    r.ReversalOf,
		CreatedAt:     r.CreatedAt,
	}
}

func newBalanceResponse(b *service.AccountBalance) AccountBalanceResponse {
	return AccountBalanceResponse{
		AccountID: b.AccountID,
		Currency:  b.Currency,
		Balance:   b.Balance,
	}
}

func newAccountHistoryResponse(s *service.Statement) AccountHistoryResponse {
	entries := make([]statementEntryResponse, 0, len(s.Entries))

	for _, e := range s.Entries {
		counterpartyID := e.CounterpartyID

		// Omit the counterpartyID field if the counterparty
		// isn't of the user type ("BANK" or "ATM")
		if e.Type != service.TxTypeTransfer {
			counterpartyID = 0
		}

		entries = append(entries, statementEntryResponse{
			EntryID:        e.EntryID,
			TransactionID:  e.TransactionID,
			Amount:         e.Amount,
			CounterpartyID: counterpartyID,
			Type:           e.Type,
			ReversalOf:     e.ReversalOf,
			CreatedAt:      e.CreatedAt,
		})
	}

	return AccountHistoryResponse{
		Transactions: entries,
		NextCursor:   s.NextCursor,
	}
}

func (in transferRequest) validate(v *validator) {
	v.Check(in.FromAccountID > 0, "from_account_id", "must be provided")
	v.Check(in.ToAccountID > 0, "to_account_id", "must be provided")
	v.Check(in.FromAccountID != in.ToAccountID, "to_account_id", "account id's must be distinct")
	v.Check(in.Amount > 0, "amount", "must be greater than zero")
}

func (in depositRequest) validate(v *validator) {
	v.Check(in.AccountID > 0, "account_id", "must be provided")
	v.Check(in.Amount > 0, "amount", "must be greater than zero")
	v.Check(in.Source == "ATM" || in.Source == "BANK", "source", "must be ATM or BANK")
}

func (in withdrawRequest) validate(v *validator) {
	v.Check(in.AccountID > 0, "account_id", "must be provided")
	v.Check(in.Amount > 0, "amount", "must be greater than zero")
	v.Check(in.Destination == "ATM" || in.Destination == "BANK", "destination", "must be ATM or BANK")
}
