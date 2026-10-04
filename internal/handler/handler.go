package handler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Velgorion/equilibra/internal/service"
	"github.com/go-chi/chi/v5"
)

type Service interface {
	Transfer(ctx context.Context, fromAccountID, toAccountID int64,
		amount int64, idempotencyKey string) (*service.Result, error)

	Deposit(ctx context.Context, destinationID int64,
		amount int64, source string, idempotencyKey string) (*service.Result, error)

	Withdraw(ctx context.Context, sourceID int64,
		amount int64, destination string, idempotencyKey string) (*service.Result, error)

	Reverse(ctx context.Context, transactionID int64,
		idempotencyKey string) (*service.Result, error)

	AccountBalance(ctx context.Context, accountID int64) (*service.AccountBalance, error)

	AccountTransactionsHistory(ctx context.Context, accountID int64,
		limit, before int64) (service.Statement, error)
}

// operationTimeout bounds a single money operation
const operationTimeout = 10 * time.Second

// BuildInfo is what the health endpoint reports about this instance
type BuildInfo struct {
	Env     string
	Version string
}

type API struct {
	service Service
	logger  *slog.Logger
	build   BuildInfo
}

func New(s Service, build BuildInfo, l *slog.Logger) *API {
	return &API{
		service: s,
		logger:  l,
		build:   build,
	}
}

func (a *API) Routes() http.Handler {
	r := chi.NewRouter()

	r.NotFound(a.routeNotFoundResponse)
	r.MethodNotAllowed(a.methodNotAllowedResponse)

	r.Route("/v1", func(r chi.Router) {
		r.Get("/healthcheck", a.healthcheckHandler)
		r.Get("/accounts/{id}/balance", a.accountBalanceHandler)
		r.Get("/accounts/{id}/transactions", a.accountHistoryHandler)
		r.Post("/transfers", a.transferHandler)
		r.Post("/deposits", a.depositHandler)
		r.Post("/withdrawals", a.withdrawHandler)
		r.Post("/transactions/{id}/reversal", a.reverseHandler)
	})

	return r
}

func (a *API) healthcheckHandler(w http.ResponseWriter, r *http.Request) {
	env := envelope{
		"status": "available",
		"system_info": map[string]string{
			"environment": a.build.Env,
			"version":     a.build.Version,
		},
	}

	if err := a.writeJSON(w, http.StatusOK, env, nil); err != nil {
		a.serverErrorResponse(w, r, err)
	}
}

func (a *API) transferHandler(w http.ResponseWriter, r *http.Request) {
	key, err := idempotencyKey(r)
	if err != nil {
		a.badRequestResponse(w, r, err)
		return
	}

	var input transferRequest
	err = a.readJSON(w, r, &input)
	if err != nil {
		a.badRequestResponse(w, r, err)
		return
	}

	v := newValidator()

	if input.validate(v); !v.Valid() {
		a.failedValidationResponse(w, r, v.Errors)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), operationTimeout)
	defer cancel()

	res, err := a.service.Transfer(ctx, input.FromAccountID, input.ToAccountID, input.Amount, key)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	a.writeTransaction(w, r, res)
}

func (a *API) depositHandler(w http.ResponseWriter, r *http.Request) {
	key, err := idempotencyKey(r)
	if err != nil {
		a.badRequestResponse(w, r, err)
		return
	}

	var input depositRequest
	err = a.readJSON(w, r, &input)
	if err != nil {
		a.badRequestResponse(w, r, err)
		return
	}

	v := newValidator()

	if input.validate(v); !v.Valid() {
		a.failedValidationResponse(w, r, v.Errors)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), operationTimeout)
	defer cancel()

	res, err := a.service.Deposit(ctx, input.AccountID, input.Amount, input.Source, key)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	a.writeTransaction(w, r, res)
}

func (a *API) withdrawHandler(w http.ResponseWriter, r *http.Request) {
	key, err := idempotencyKey(r)
	if err != nil {
		a.badRequestResponse(w, r, err)
		return
	}

	var input withdrawRequest
	err = a.readJSON(w, r, &input)
	if err != nil {
		a.badRequestResponse(w, r, err)
		return
	}

	v := newValidator()

	if input.validate(v); !v.Valid() {
		a.failedValidationResponse(w, r, v.Errors)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), operationTimeout)
	defer cancel()

	res, err := a.service.Withdraw(ctx, input.AccountID, input.Amount, input.Destination, key)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	a.writeTransaction(w, r, res)
}

func (a *API) reverseHandler(w http.ResponseWriter, r *http.Request) {
	key, err := idempotencyKey(r)
	if err != nil {
		a.badRequestResponse(w, r, err)
		return
	}

	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		a.notFoundResponse(w, r, errors.New("invalid id parameter"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), operationTimeout)
	defer cancel()

	res, err := a.service.Reverse(ctx, id, key)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	a.writeTransaction(w, r, res)
}

func (a *API) accountBalanceHandler(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		a.notFoundResponse(w, r, errors.New("invalid id parameter"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), operationTimeout)
	defer cancel()

	b, err := a.service.AccountBalance(ctx, id)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	err = a.writeJSON(w, http.StatusOK, envelope{"account": newBalanceResponse(b)}, nil)
	if err != nil {
		a.serverErrorResponse(w, r, err)
		return
	}
}

func (a *API) accountHistoryHandler(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		a.notFoundResponse(w, r, errors.New("invalid id parameter"))
		return
	}

	v := newValidator()

	qs := r.URL.Query()

	limit := a.readInt64(qs, "limit", 10, v)
	before := a.readInt64(qs, "before", 0, v)

	v.Check(limit > 0, "limit", "must be greater than zero")
	v.Check(limit <= service.MaxLimit, "limit",
		fmt.Sprintf("must not exceed %d", service.MaxLimit))
	v.Check(before >= 0, "before", "must not be negative")

	if !v.Valid() {
		a.failedValidationResponse(w, r, v.Errors)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), operationTimeout)
	defer cancel()

	history, err := a.service.AccountTransactionsHistory(ctx, id, limit, before)
	if err != nil {
		a.handleError(w, r, err)
		return
	}

	err = a.writeJSON(w, http.StatusOK, envelope{"history": newAccountHistoryResponse(&history)}, nil)
	if err != nil {
		a.serverErrorResponse(w, r, err)
		return
	}
}
