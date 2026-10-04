package handler

import (
	"errors"
	"fmt"
	"net/http"

	svc "github.com/Velgorion/equilibra/internal/service"
)

func (a *API) logError(r *http.Request, err error) {
	var (
		method = r.Method
		uri    = r.URL.RequestURI()
	)

	a.logger.Error(err.Error(), "method", method, "uri", uri)
}

func (a *API) errorResponse(w http.ResponseWriter, r *http.Request, status int, message any) {
	env := envelope{"error": message}

	err := a.writeJSON(w, status, env, nil)
	if err != nil {
		a.logError(r, err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// handleError maps a domain error to an HTTP status
func (a *API) handleError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, svc.ErrInvalidAmount),
		errors.Is(err, svc.ErrSameAccount):
		a.badRequestResponse(w, r, err) // 400

	case errors.Is(err, svc.ErrSourceAccountNotFound),
		errors.Is(err, svc.ErrDestinationAccountNotFound),
		errors.Is(err, svc.ErrSystemAccountNotFound),
		errors.Is(err, svc.ErrTransactionNotFound),
		errors.Is(err, svc.ErrAccountNotFound):
		a.notFoundResponse(w, r, err) // 404

	case errors.Is(err, svc.ErrNotEnough),
		errors.Is(err, svc.ErrCurrencyMismatch),
		errors.Is(err, svc.ErrSystemAccountNotAllowed),
		errors.Is(err, svc.ErrInvalidWithdrawSource),
		errors.Is(err, svc.ErrInvalidDepositDestination):
		a.unprocessableEntityResponse(w, r, err) // 422

	case errors.Is(err, svc.ErrTransactionsMismatch),
		errors.Is(err, svc.ErrReverseIncompletedTx),
		errors.Is(err, svc.ErrReverseReversalTx):
		a.conflictResponse(w, r, err) // 409

	default:
		a.serverErrorResponse(w, r, err) // 500
	}
}

func (a *API) serverErrorResponse(w http.ResponseWriter, r *http.Request, err error) {
	a.logError(r, err)
	message := "the server encountered a problem and could not process your request"
	a.errorResponse(w, r, http.StatusInternalServerError, message)
}

func (a *API) badRequestResponse(w http.ResponseWriter, r *http.Request, err error) {
	a.errorResponse(w, r, http.StatusBadRequest, err.Error())
}

func (a *API) notFoundResponse(w http.ResponseWriter, r *http.Request, err error) {
	a.errorResponse(w, r, http.StatusNotFound, err.Error())
}

func (a *API) unprocessableEntityResponse(w http.ResponseWriter, r *http.Request, err error) {
	a.errorResponse(w, r, http.StatusUnprocessableEntity, err.Error())
}

func (a *API) conflictResponse(w http.ResponseWriter, r *http.Request, err error) {
	a.errorResponse(w, r, http.StatusConflict, err.Error())
}

func (a *API) failedValidationResponse(w http.ResponseWriter, r *http.Request, errors map[string]string) {
	a.errorResponse(w, r, http.StatusBadRequest, errors)
}

func (a *API) routeNotFoundResponse(w http.ResponseWriter, r *http.Request) {
	message := fmt.Sprintf("the requested resource could not be found: %s", r.URL.RequestURI())
	a.errorResponse(w, r, http.StatusNotFound, message)
}

func (a *API) methodNotAllowedResponse(w http.ResponseWriter, r *http.Request) {
	message := fmt.Sprintf("the %s method is not supported for this resource: %s", r.Method, r.URL.RequestURI())
	a.errorResponse(w, r, http.StatusMethodNotAllowed, message)
}
