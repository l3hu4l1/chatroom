package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/l3hu4l1/chatroom/protocol"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer conn.Close()

	stdin := bufio.NewReader(os.Stdin)

	var nickname string
	fmt.Print("Enter your nickname: ")
	nickname, err = stdin.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading nickname:", err)
		return
	}
	nickname = strings.TrimSpace(nickname)
	if nickname == "" {
		fmt.Println("Nickname cannot be empty")
		return
	}
	fmt.Println("Welcome,", nickname)

	if err := protocol.WriteWireMessage(conn, &protocol.WireMessage{
		Version: protocol.ProtocolVersion,
		Body: &protocol.WireMessage_ClientHello{
			ClientHello: &protocol.ClientHello{Nickname: nickname},
		},
	}); err != nil {
		fmt.Println("Failed to send hello:", err)
		return
	}

	go handleConnection(conn)

	for {
		fmt.Print("Enter message: ")
		msg, err := stdin.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading message:", err)
			return
		}
		msg = strings.TrimSpace(msg)
		if msg == "" {
			continue
		}
		if msg == "quit" {
			fmt.Println("See you next time!")
			return
		}

		data := &protocol.WireMessage{
			Version: protocol.ProtocolVersion,
			Body: &protocol.WireMessage_ClientMessage{
				ClientMessage: &protocol.ClientMessage{Text: msg},
			},
		}

		if err := protocol.WriteWireMessage(conn, data); err != nil {
			fmt.Println("Failed to send message:", err)
			return
		}
	}
}

func handleConnection(conn net.Conn) {
	for {
		message, err := protocol.ReadWireMessage(conn)
		if err != nil {
			fmt.Println("Connection closed:", err)
			return
		}

		switch body := message.GetBody().(type) {
		case *protocol.WireMessage_ServerWelcome:
			fmt.Println("Server:", body.ServerWelcome.GetNickname(), "connected with id", body.ServerWelcome.GetConnectionId())
		case *protocol.WireMessage_ServerEvent:
			fmt.Println("Received message from", body.ServerEvent.GetSender()+":", body.ServerEvent.GetText())
		case *protocol.WireMessage_Error:
			fmt.Println("Server error:", body.Error.GetMessage())
		case *protocol.WireMessage_Pong:
			fmt.Println("Received pong at", body.Pong.GetSentAtUnixMs())
		default:
			fmt.Println("Received unsupported message")
		}
	}
}
