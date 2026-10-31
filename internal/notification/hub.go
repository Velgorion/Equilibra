package notification

import (
	"sync"

	"github.com/coder/websocket"
)

type client struct {
	send chan []byte // chan for sending messages to the client

	// close done channel when the client
	// needs to be disconnected
	done      chan struct{}
	closeOnce sync.Once // guarantees that done chan will be closed just once
	reason    websocket.StatusCode
}

type Hub struct {
	mu      sync.RWMutex
	clients map[int64]map[*client]struct{} // accountID -> set of the clients
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[int64]map[*client]struct{}),
	}
}

func (h *Hub) Register(accountID int64) *client {
	h.mu.Lock()
	defer h.mu.Unlock()

	cl := &client{
		send: make(chan []byte, 16),
		done: make(chan struct{}),
	}

	if _, ok := h.clients[accountID]; !ok {
		h.clients[accountID] = make(map[*client]struct{})
	}

	// Add the client to the set of clients with the same accountID
	h.clients[accountID][cl] = struct{}{}

	return cl
}

func (h *Hub) Unregister(accountID int64, reason websocket.StatusCode, cl *client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.clients[accountID], cl)

	if len(h.clients[accountID]) == 0 {
		delete(h.clients, accountID)
	}

	// disconnect the client by closing the channel
	cl.disconnect(reason)
}

func (h *Hub) Send(accountID int64, msg []byte) {
	h.mu.RLock()

	// copy clients into a slice so we can work with them without mutex
	accountClients := make([]*client, 0, len(h.clients[accountID]))
	for cl := range h.clients[accountID] {
		accountClients = append(accountClients, cl)
	}

	h.mu.RUnlock()

	for _, cl := range accountClients {
		select {
		case cl.send <- msg:
		case <-cl.done:

		// don't wait if the client is slow
		default:
			h.Unregister(accountID, websocket.StatusPolicyViolation, cl)
		}
	}
}

func (h *Hub) CloseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, clients := range h.clients {
		for cl := range clients {
			cl.disconnect(websocket.StatusGoingAway)
		}
	}
}

func (c *client) disconnect(reason websocket.StatusCode) {
	c.closeOnce.Do(func() {
		c.reason = reason
		close(c.done)
	})
}
