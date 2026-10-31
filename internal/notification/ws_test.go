package notification

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestWSHandler(t *testing.T) {
	hub := NewHub()
	msg := []byte("hello")

	srv := httptest.NewTestServer(t, NewWSHandler(hub, slog.New(slog.DiscardHandler)))

	conn := dialAccount(t, srv, 10)

	waitForClients(t, hub, 10, 1)

	hub.Send(10, msg)

	data, err := readMessage(t, conn)
	require.NoError(t, err)
	require.Equal(t, msg, data)
}

func TestWSHandlerWrongAccountID(t *testing.T) {
	hub := NewHub()

	srv := httptest.NewTestServer(t, NewWSHandler(hub, slog.New(slog.DiscardHandler)))

	// Send request with the incorrect accountID = 'abc'
	_, resp, err := websocket.Dial(t.Context(), "ws://example.com/?account_id=abc",
		&websocket.DialOptions{HTTPClient: srv.Client()})
	require.Error(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestWSHandlerDisconnect(t *testing.T) {
	hub := NewHub()
	srv := httptest.NewTestServer(t, NewWSHandler(hub, slog.New(slog.DiscardHandler)))

	conn := dialAccount(t, srv, 10)

	waitForClients(t, hub, 10, 1)

	hub.CloseAll()

	_, err := readMessage(t, conn)
	require.Error(t, err)
	require.Equal(t, websocket.StatusGoingAway, websocket.CloseStatus(err))
}

func TestWSHandlerClientLeaves(t *testing.T) {
	hub := NewHub()
	srv := httptest.NewTestServer(t, NewWSHandler(hub, slog.New(slog.DiscardHandler)))

	conn := dialAccount(t, srv, 10)
	waitForClients(t, hub, 10, 1)

	// the browser tab is closed: the handler has to notice it and leave the hub
	require.NoError(t, conn.Close(websocket.StatusNormalClosure, ""))
}

func TestWSHandlerEveryConnectionOfAccountGetsMessage(t *testing.T) {
	hub := NewHub()
	srv := httptest.NewTestServer(t, NewWSHandler(hub, slog.New(slog.DiscardHandler)))

	// the same user with the app open in two places, and another user
	phone := dialAccount(t, srv, 10)
	laptop := dialAccount(t, srv, 10)
	other := dialAccount(t, srv, 11)

	waitForClients(t, hub, 10, 2)
	waitForClients(t, hub, 11, 1)

	msg := []byte("you received 500")
	hub.Send(10, msg)

	for _, conn := range []*websocket.Conn{phone, laptop} {
		data, err := readMessage(t, conn)
		require.NoError(t, err)
		require.Equal(t, msg, data)
	}

	// the other account gets nothing: its read only ends by the timeout
	_, err := readMessage(t, other)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWSHandlerRejectsBadAccountID(t *testing.T) {
	hub := NewHub()
	srv := httptest.NewTestServer(t, NewWSHandler(hub, slog.New(slog.DiscardHandler)))

	for _, query := range []string{"", "?account_id=", "?account_id=0", "?account_id=-5"} {
		t.Run(query, func(t *testing.T) {
			_, resp, err := websocket.Dial(t.Context(), "ws://example.com/"+query,
				&websocket.DialOptions{HTTPClient: srv.Client()})
			require.Error(t, err)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}

	require.Empty(t, hub.clients)
}

// dialAccount opens a WebSocket to the test server for accountID and closes it
// when the test ends.
func dialAccount(t *testing.T, srv *httptest.Server, accountID int64) *websocket.Conn {
	t.Helper()

	url := fmt.Sprintf("ws://example.com/?account_id=%d", accountID)
	conn, _, err := websocket.Dial(t.Context(), url, &websocket.DialOptions{HTTPClient: srv.Client()})
	require.NoError(t, err)

	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

// waitForClients waits until the hub holds n clients of accountID
func waitForClients(t *testing.T, hub *Hub, accountID int64, n int) {
	t.Helper()

	require.Eventually(t, func() bool {
		hub.mu.RLock()
		defer hub.mu.RUnlock()
		return len(hub.clients[accountID]) == n
	}, time.Second, 5*time.Millisecond)
}

// readMessage reads one message with a deadline, so a missing message fails
// the test.
func readMessage(t *testing.T, conn *websocket.Conn) ([]byte, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	_, data, err := conn.Read(ctx)
	return data, err
}
