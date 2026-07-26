package main

import (
	"net"
	"testing"
	"time"

	"github.com/l3hu4l1/chatroom/protocol"
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

	if err := protocol.WriteWireMessage(conn, &protocol.WireMessage{
		Version: protocol.ProtocolVersion,
		Body: &protocol.WireMessage_ClientHello{
			ClientHello: &protocol.ClientHello{Nickname: nickname},
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

	if err := protocol.WriteWireMessage(senderClient, &protocol.WireMessage{
		Version: protocol.ProtocolVersion,
		Body: &protocol.WireMessage_ClientMessage{
			ClientMessage: &protocol.ClientMessage{Text: "hello everyone"},
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
