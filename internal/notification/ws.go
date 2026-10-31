package notification

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
)

const (
	pingInterval = 25 * time.Second
	pingTimeout  = 5 * time.Second
	writeTimeout = 10 * time.Second
)

// WSHandler serves client's requests, ungrades
// connection to a WebSocket and send notifications to clients.
type WSHandler struct {
	hub    *Hub
	logger *slog.Logger
}

// NewWSHandler creates a WSHandler
func NewWSHandler(hub *Hub, logger *slog.Logger) *WSHandler {
	return &WSHandler{
		hub:    hub,
		logger: logger,
	}
}

// ServeHTTP accepts client's requests and sends messages to the connection
// over WebSocket.
//
// At the same time client cannot send messages to the server
// otherwise the connection will be closed.
func (h *WSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Get 'account_id' from URL values
	val := r.URL.Query().Get("account_id")
	accountID, err := strconv.ParseInt(val, 10, 64)
	if err != nil || accountID <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Upgrate connection to a WebSocket
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"localhost:*"}})
	if err != nil {
		h.logger.Error("accept websocket client", "account_id", accountID, "err", err)
		return
	}
	defer conn.CloseNow()

	// Register the client under accountID in the Hub
	cl := h.hub.Register(accountID)
	defer h.hub.Unregister(accountID, websocket.StatusNormalClosure, cl)

	ctx := conn.CloseRead(r.Context())

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {

		// If we got message for our client, write it to the connection over WebSocket
		case msg := <-cl.send:
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)

			err = conn.Write(wctx, websocket.MessageText, msg)
			if err != nil {
				h.logger.Info("write notification", "account_id", accountID, "err", err)
				cancel()
				return
			}
			cancel()

		// Every 'pingTimeout' seconds ping the client for ensure he's responsive
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, pingTimeout)
			err = conn.Ping(pctx)
			if err != nil {
				h.logger.Info("send ping", "account_id", accountID, "err", err)
				cancel()
				return
			}
			cancel()

		case <-cl.done:
			conn.Close(cl.reason, "")
			return

		case <-ctx.Done():
			return
		}
	}
}
