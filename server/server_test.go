package main

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/l3hu4l1/chatroom/protocol"
	protocolv1 "github.com/l3hu4l1/chatroom/protocol/v1"
)

func resetHubForTest() {
	hub = &chatHub{clients: make(map[uint64]*client)}
}

func waitForCondition(t *testing.T, deadline time.Duration, condition func() bool) {
	t.Helper()

	stop := time.Now().Add(deadline)
	for time.Now().Before(stop) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition was not met before timeout")
}

func sendHello(t *testing.T, conn net.Conn, nickname string) {
	t.Helper()

	if err := protocol.WriteWireMessage(conn, &protocolv1.WireMessage{
		Version: protocol.ProtocolVersion,
		Body: &protocolv1.WireMessage_ClientHello{
			ClientHello: &protocolv1.ClientHello{Nickname: nickname},
		},
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	message, err := protocol.ReadWireMessage(conn)
	if err != nil {
		t.Fatalf("read welcome: %v", err)
	}

	welcome := message.GetServerWelcome()
	if welcome == nil {
		t.Fatalf("expected server welcome, got %#v", message.GetBody())
	}
	if got := welcome.GetNickname(); got != nickname {
		t.Fatalf("welcome nickname = %q, want %q", got, nickname)
	}
}

func TestHandleConnectionRemovesDisconnectedClient(t *testing.T) {
	resetHubForTest()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	client := hub.add(serverConn)
	go handleConnection(client)

	sendHello(t, clientConn, "alice")
	if err := clientConn.Close(); err != nil {
		t.Fatalf("close client conn: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		hub.mu.RLock()
		defer hub.mu.RUnlock()
		return len(hub.clients) == 0
	})
}

func TestBroadcastFansOutToOtherClients(t *testing.T) {
	resetHubForTest()

	senderServer, senderClient := net.Pipe()
	receiverOneServer, receiverOneClient := net.Pipe()
	receiverTwoServer, receiverTwoClient := net.Pipe()
	defer senderClient.Close()
	defer receiverOneClient.Close()
	defer receiverTwoClient.Close()

	sender := hub.add(senderServer)
	receiverOne := hub.add(receiverOneServer)
	receiverTwo := hub.add(receiverTwoServer)

	go handleConnection(sender)
	go handleConnection(receiverOne)
	go handleConnection(receiverTwo)

	sendHello(t, senderClient, "sender")
	sendHello(t, receiverOneClient, "receiver-one")
	sendHello(t, receiverTwoClient, "receiver-two")

	if err := protocol.WriteWireMessage(senderClient, &protocolv1.WireMessage{
		Version: protocol.ProtocolVersion,
		Body: &protocolv1.WireMessage_ClientMessage{
			ClientMessage: &protocolv1.ClientMessage{Text: "hello everyone"},
		},
	}); err != nil {
		t.Fatalf("write chat message: %v", err)
	}

	messageOne, err := protocol.ReadWireMessage(receiverOneClient)
	if err != nil {
		t.Fatalf("read receiver one message: %v", err)
	}
	eventOne := messageOne.GetServerEvent()
	if eventOne == nil {
		t.Fatalf("receiver one expected server event, got %#v", messageOne.GetBody())
	}
	if got := eventOne.GetSender(); got != "sender" {
		t.Fatalf("receiver one sender = %q, want %q", got, "sender")
	}
	if got := eventOne.GetText(); got != "hello everyone" {
		t.Fatalf("receiver one text = %q, want %q", got, "hello everyone")
	}

	messageTwo, err := protocol.ReadWireMessage(receiverTwoClient)
	if err != nil {
		t.Fatalf("read receiver two message: %v", err)
	}
	eventTwo := messageTwo.GetServerEvent()
	if eventTwo == nil {
		t.Fatalf("receiver two expected server event, got %#v", messageTwo.GetBody())
	}
	if got := eventTwo.GetSender(); got != "sender" {
		t.Fatalf("receiver two sender = %q, want %q", got, "sender")
	}
	if got := eventTwo.GetText(); got != "hello everyone" {
		t.Fatalf("receiver two text = %q, want %q", got, "hello everyone")
	}
}

func TestEnqueueFailsAfterClientRemoval(t *testing.T) {
	resetHubForTest()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	client := hub.add(serverConn)
	hub.remove(client.id)

	err := client.enqueue(&protocolv1.WireMessage{
		Version: protocol.ProtocolVersion,
		Body: &protocolv1.WireMessage_Ping{
			Ping: &protocolv1.Ping{SentAtUnixMs: 1},
		},
	})
	if err == nil {
		t.Fatal("expected enqueue to fail after client removal")
	}
}

func TestEnqueueReturnsQueueFullWhenOutboundBufferIsFull(t *testing.T) {
	c := &client{
		id:       1,
		outbound: make(chan *protocolv1.WireMessage, 1),
		done:     make(chan struct{}),
	}

	first := &protocolv1.WireMessage{Version: protocol.ProtocolVersion}
	second := &protocolv1.WireMessage{Version: protocol.ProtocolVersion}

	if err := c.enqueue(first); err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}

	err := c.enqueue(second)
	if err == nil {
		t.Fatal("expected queue-full error on second enqueue")
	}
	if !errors.Is(err, errOutboundQueueFull) {
		t.Fatalf("error = %v, want errOutboundQueueFull", err)
	}
}

func TestBroadcastRemovesSlowClientWhenQueueIsFull(t *testing.T) {
	resetHubForTest()

	senderServer, senderClient := net.Pipe()
	slowServer, slowClient := net.Pipe()
	fastServer, fastClient := net.Pipe()
	defer senderClient.Close()
	defer slowClient.Close()
	defer fastClient.Close()

	sender := hub.add(senderServer)
	slow := hub.add(slowServer)
	fast := hub.add(fastServer)

	go handleConnection(sender)
	go handleConnection(fast)

	sendHello(t, senderClient, "sender")
	sendHello(t, fastClient, "fast")

	// Fill slow client's queue so the next broadcast enqueue hits queue-full.
	for {
		err := slow.enqueue(&protocolv1.WireMessage{
			Version: protocol.ProtocolVersion,
			Body: &protocolv1.WireMessage_ServerEvent{
				ServerEvent: &protocolv1.ServerEvent{Sender: "server", Text: "backpressure"},
			},
		})
		if err == nil {
			continue
		}
		if errors.Is(err, errOutboundQueueFull) {
			break
		}
		t.Fatalf("unexpected enqueue error while filling slow queue: %v", err)
	}

	if err := protocol.WriteWireMessage(senderClient, &protocolv1.WireMessage{
		Version: protocol.ProtocolVersion,
		Body: &protocolv1.WireMessage_ClientMessage{
			ClientMessage: &protocolv1.ClientMessage{Text: "hello healthy client"},
		},
	}); err != nil {
		t.Fatalf("write chat message: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		hub.mu.RLock()
		defer hub.mu.RUnlock()
		_, slowPresent := hub.clients[slow.id]
		_, fastPresent := hub.clients[fast.id]
		return !slowPresent && fastPresent
	})

	message, err := protocol.ReadWireMessage(fastClient)
	if err != nil {
		t.Fatalf("read fast client message: %v", err)
	}
	event := message.GetServerEvent()
	if event == nil {
		t.Fatalf("expected server event for fast client, got %#v", message.GetBody())
	}
	if got := event.GetText(); got != "hello healthy client" {
		t.Fatalf("fast client text = %q, want %q", got, "hello healthy client")
	}

	if got := atomic.LoadUint64(&metrics.queueFullDisconnects); got == 0 {
		t.Fatal("expected queue-full disconnect metric to increment")
	}
}
