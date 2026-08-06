package main

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/l3hu4l1/chatroom/protocol"
	protocolv1 "github.com/l3hu4l1/chatroom/protocol/v1"
)

type client struct {
	id        uint64
	conn      net.Conn
	outbound  chan *protocolv1.WireMessage
	done      chan struct{}
	closeOnce sync.Once
}

type chatHub struct {
	mu      sync.RWMutex
	clients map[uint64]*client
	nextID  uint64
}

type serverMetrics struct {
	connects             uint64
	disconnects          uint64
	broadcasts           uint64
	queueFullDisconnects uint64
}

var hub = &chatHub{clients: make(map[uint64]*client)}

const outboundQueueSize = 64

var errClientClosed = errors.New("client closed")
var errOutboundQueueFull = errors.New("client outbound queue full")

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server is listening on port 8080...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		client := hub.add(conn)
		go handleConnection(client)
	}
}

func (h *chatHub) add(conn net.Conn) *client {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.nextID++
	c := &client{
		id:       h.nextID,
		conn:     conn,
		outbound: make(chan *protocolv1.WireMessage, outboundQueueSize),
		done:     make(chan struct{}),
	}
	h.clients[c.id] = c
	c.startWriter()
	return c
}

func (h *chatHub) remove(id uint64) {
	h.mu.Lock()
	c, ok := h.clients[id]
	if ok {
		delete(h.clients, id)
	}
	h.mu.Unlock()

	if ok {
		c.close()
	}
}

func (h *chatHub) snapshotExcept(skipID uint64) []*client {
	h.mu.RLock()
	defer h.mu.RUnlock()

	clients := make([]*client, 0, len(h.clients))
	for id, c := range h.clients {
		if id == skipID {
			continue
		}
		clients = append(clients, c)
	}
	return clients
}

func (c *client) startWriter() {
	go func() {
		for message := range c.outbound {
			if err := protocol.WriteWireMessage(c.conn, message); err != nil {
				c.close()
				return
			}
		}
	}()
}

func (c *client) enqueue(message *protocolv1.WireMessage) (err error) {
	select {
	case <-c.done:
		return errClientClosed
	default:
	}

	defer func() {
		if recover() != nil {
			err = errClientClosed
		}
	}()

	select {
	case <-c.done:
		return errClientClosed
	case c.outbound <- message:
		return nil
	default:
		return errOutboundQueueFull
	}
}

func (c *client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		close(c.outbound)
		_ = c.conn.Close()
	})
}

func handleConnection(c *client) {
	defer hub.remove(c.id)

	var nickname string
	for {
		message, err := protocol.ReadWireMessage(c.conn)
		if err != nil {
			fmt.Println("Connection closed:", err)
			return
		}

		if message.GetVersion() != protocol.ProtocolVersion {
			if err := c.enqueue(&protocolv1.WireMessage{
				Version: protocol.ProtocolVersion,
				Body: &protocolv1.WireMessage_Error{
					Error: &protocolv1.ProtocolError{Code: 400, Message: "unsupported protocol version"},
				},
			}); err != nil {
				fmt.Println("Error sending protocol version error:", err)
			}
			return
		}

		switch body := message.GetBody().(type) {
		case *protocolv1.WireMessage_ClientHello:
			nickname = body.ClientHello.GetNickname()
			if nickname == "" {
				if err := c.enqueue(&protocolv1.WireMessage{
					Version: protocol.ProtocolVersion,
					Body: &protocolv1.WireMessage_Error{
						Error: &protocolv1.ProtocolError{Code: 400, Message: "nickname cannot be empty"},
					},
				}); err != nil {
					fmt.Println("Error sending nickname validation error:", err)
				}
				return
			}

			if err := c.enqueue(&protocolv1.WireMessage{
				Version: protocol.ProtocolVersion,
				Body: &protocolv1.WireMessage_ServerWelcome{
					ServerWelcome: &protocolv1.ServerWelcome{ConnectionId: c.id, Nickname: nickname},
				},
			}); err != nil {
				fmt.Println("Error sending welcome:", err)
				return
			}

			fmt.Printf("%s connected\n", nickname)

		case *protocolv1.WireMessage_ClientMessage:
			if nickname == "" {
				if err := c.enqueue(&protocolv1.WireMessage{
					Version: protocol.ProtocolVersion,
					Body: &protocolv1.WireMessage_Error{
						Error: &protocolv1.ProtocolError{Code: 400, Message: "hello required before chat messages"},
					},
				}); err != nil {
					fmt.Println("Error sending pre-hello validation error:", err)
					return
				}
				continue
			}

			text := body.ClientMessage.GetText()
			if text == "" {
				continue
			}

			broadcast := &protocolv1.WireMessage{
				Version: protocol.ProtocolVersion,
				Body: &protocolv1.WireMessage_ServerEvent{
					ServerEvent: &protocolv1.ServerEvent{
						Sender:       nickname,
						Text:         text,
						SentAtUnixMs: uint64(time.Now().UnixMilli()),
					},
				},
			}

			for _, target := range hub.snapshotExcept(c.id) {
				if err := target.enqueue(broadcast); err != nil {
					if errors.Is(err, errOutboundQueueFull) {
						atomic.AddUint64(&metrics.queueFullDisconnects, 1)
						fmt.Println("Disconnecting slow client due to full outbound queue:", target.id)
					} else {
						fmt.Println("Error writing to connection:", err)
					}
					hub.remove(target.id)
				}
			}

			fmt.Printf("%s: %s\n", nickname, text)

		case *protocolv1.WireMessage_Ping:
			if err := c.enqueue(&protocolv1.WireMessage{
				Version: protocol.ProtocolVersion,
				Body: &protocolv1.WireMessage_Pong{
					Pong: &protocolv1.Pong{SentAtUnixMs: body.Ping.GetSentAtUnixMs()},
				},
			}); err != nil {
				fmt.Println("Error sending pong:", err)
				return
			}

		default:
			if err := c.enqueue(&protocolv1.WireMessage{
				Version: protocol.ProtocolVersion,
				Body: &protocolv1.WireMessage_Error{
					Error: &protocolv1.ProtocolError{Code: 400, Message: "unsupported message type"},
				},
			}); err != nil {
				fmt.Println("Error sending unsupported message type error:", err)
				return
			}
		}
	}
}
