package notification

import (
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func TestHub(t *testing.T) {
	const (
		AliceID = 10
	)

	msg := []byte("hello")

	hub := NewHub()
	cl1 := hub.Register(AliceID)
	_, ok := hub.clients[AliceID]
	require.True(t, ok)

	cl2 := hub.Register(AliceID)
	require.Equal(t, 2, len(hub.clients[AliceID]))

	hub.Send(AliceID, msg)
	for cl := range hub.clients[AliceID] {
		require.Equal(t, msg, <-cl.send)
	}

	hub.Unregister(AliceID, websocket.StatusGoingAway, cl1)
	require.Equal(t, 1, len(hub.clients[AliceID]))

	_, ok = <-cl1.done
	require.False(t, ok)
	require.Equal(t, websocket.StatusGoingAway, cl1.reason)

	hub.Unregister(AliceID, websocket.StatusGoingAway, cl2)
	_, ok = hub.clients[AliceID]
	require.False(t, ok)

	_, ok = <-cl2.done
	require.False(t, ok)
	require.Equal(t, websocket.StatusGoingAway, cl2.reason)

	// there will be no panic, thanks to client.CloseOnce
	hub.CloseAll()
}

func TestHubSendsOnlyToTheAccount(t *testing.T) {
	const (
		AliceID = 10
		BobID   = 11
	)

	hub := NewHub()
	alice := hub.Register(AliceID)
	bob := hub.Register(BobID)

	msg := []byte("you received 500")
	hub.Send(BobID, msg)

	require.Equal(t, msg, <-bob.send)

	// a notification about one account must never reach the owner of another
	require.Empty(t, alice.send)
}

func TestHubDisconnectsSlowClient(t *testing.T) {
	const AliceID = 10

	hub := NewHub()
	slow := hub.Register(AliceID)

	// the client never reads, so its queue fills up
	for range cap(slow.send) {
		hub.Send(AliceID, []byte("msg"))
	}
	require.Len(t, slow.send, cap(slow.send))

	// one more message must not block Send: the client is dropped instead
	hub.Send(AliceID, []byte("one too many"))

	require.NotContains(t, hub.clients, int64(AliceID))

	_, open := <-slow.done
	require.False(t, open)
	require.Equal(t, websocket.StatusPolicyViolation, slow.reason)
}

func TestHubCloseAll(t *testing.T) {
	hub := NewHub()
	alice := hub.Register(10)
	bob := hub.Register(11)

	hub.CloseAll()

	for _, cl := range []*client{alice, bob} {
		_, open := <-cl.done
		require.False(t, open)
		require.Equal(t, websocket.StatusGoingAway, cl.reason)
	}
}

func TestHubConcurrently(t *testing.T) {
	const (
		accounts = 5
		workers  = 50
		rounds   = 200
	)

	hub := NewHub()
	var wg sync.WaitGroup
	msg := []byte("hello")

	for w := range workers {
		wg.Go(func() {
			accountID := int64(w % accounts)

			for range rounds {
				cl := hub.Register(accountID)
				hub.Send(accountID, msg)
				hub.Send(rand.Int64N(accounts), msg)
				hub.Unregister(accountID, websocket.StatusGoingAway, cl)
			}
		})
	}

	wg.Wait()

	// Has to be empty after all the work
	require.Empty(t, hub.clients)
	require.Zero(t, hub.Connections())
}

func TestHubConnectionsAfterSlowClientLeaves(t *testing.T) {
	hub := NewHub()
	slow := hub.Register(10)
	hub.Register(11)
	require.Equal(t, 2, hub.Connections())

	// the slow client's queue fills up, and one more message drops it
	for range cap(slow.send) + 1 {
		hub.Send(10, []byte("msg"))
	}

	// then its handler leaves and unregisters it a second time, as ws.go does
	hub.Unregister(10, websocket.StatusNormalClosure, slow)

	require.Equal(t, 1, hub.Connections())
}
