package main

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/l3hu4l1/chatroom/protocol"
)

type client struct {
	id      uint64
	conn    net.Conn
	writeMu sync.Mutex
}

type chatHub struct {
	mu      sync.RWMutex
	clients map[uint64]*client
	nextID  uint64
}

var hub = &chatHub{clients: make(map[uint64]*client)}

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
	c := &client{id: h.nextID, conn: conn}
	h.clients[c.id] = c
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
		_ = c.conn.Close()
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

func (c *client) write(message *protocol.WireMessage) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return protocol.WriteWireMessage(c.conn, message)
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
			_ = c.write(&protocol.WireMessage{
				Version: protocol.ProtocolVersion,
				Body: &protocol.WireMessage_Error{
					Error: &protocol.ProtocolError{Code: 400, Message: "unsupported protocol version"},
				},
			})
			return
		}

		switch body := message.GetBody().(type) {
		case *protocol.WireMessage_ClientHello:
			nickname = body.ClientHello.GetNickname()
			if nickname == "" {
				_ = c.write(&protocol.WireMessage{
					Version: protocol.ProtocolVersion,
					Body: &protocol.WireMessage_Error{
						Error: &protocol.ProtocolError{Code: 400, Message: "nickname cannot be empty"},
					},
				})
				return
			}

			if err := c.write(&protocol.WireMessage{
				Version: protocol.ProtocolVersion,
				Body: &protocol.WireMessage_ServerWelcome{
					ServerWelcome: &protocol.ServerWelcome{ConnectionId: c.id, Nickname: nickname},
				},
			}); err != nil {
				fmt.Println("Error sending welcome:", err)
				return
			}

			fmt.Printf("%s connected\n", nickname)

		case *protocol.WireMessage_ClientMessage:
			if nickname == "" {
				_ = c.write(&protocol.WireMessage{
					Version: protocol.ProtocolVersion,
					Body: &protocol.WireMessage_Error{
						Error: &protocol.ProtocolError{Code: 400, Message: "hello required before chat messages"},
					},
				})
				continue
			}

			text := body.ClientMessage.GetText()
			if text == "" {
				continue
			}

			broadcast := &protocol.WireMessage{
				Version: protocol.ProtocolVersion,
				Body: &protocol.WireMessage_ServerEvent{
					ServerEvent: &protocol.ServerEvent{
						Sender:       nickname,
						Text:         text,
						SentAtUnixMs: uint64(time.Now().UnixMilli()),
					},
				},
			}

			for _, target := range hub.snapshotExcept(c.id) {
				if err := target.write(broadcast); err != nil {
					fmt.Println("Error writing to connection:", err)
					hub.remove(target.id)
				}
			}

			fmt.Printf("%s: %s\n", nickname, text)

		case *protocol.WireMessage_Ping:
			if err := c.write(&protocol.WireMessage{
				Version: protocol.ProtocolVersion,
				Body: &protocol.WireMessage_Pong{
					Pong: &protocol.Pong{SentAtUnixMs: body.Ping.GetSentAtUnixMs()},
				},
			}); err != nil {
				fmt.Println("Error sending pong:", err)
				return
			}

		default:
			_ = c.write(&protocol.WireMessage{
				Version: protocol.ProtocolVersion,
				Body: &protocol.WireMessage_Error{
					Error: &protocol.ProtocolError{Code: 400, Message: "unsupported message type"},
				},
			})
		}
	}
}
