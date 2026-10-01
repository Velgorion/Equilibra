package handler

import (
	"context"
	"errors"
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
