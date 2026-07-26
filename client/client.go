package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/l3hu4l1/chatroom/protocol"
	protocolv1 "github.com/l3hu4l1/chatroom/protocol/v1"
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

	if err := protocol.WriteWireMessage(conn, &protocolv1.WireMessage{
		Version: protocol.ProtocolVersion,
		Body: &protocolv1.WireMessage_ClientHello{
			ClientHello: &protocolv1.ClientHello{Nickname: nickname},
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

		data := &protocolv1.WireMessage{
			Version: protocol.ProtocolVersion,
			Body: &protocolv1.WireMessage_ClientMessage{
				ClientMessage: &protocolv1.ClientMessage{Text: msg},
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
		case *protocolv1.WireMessage_ServerWelcome:
			fmt.Println("Server:", body.ServerWelcome.GetNickname(), "connected with id", body.ServerWelcome.GetConnectionId())
		case *protocolv1.WireMessage_ServerEvent:
			fmt.Println("Received message from", body.ServerEvent.GetSender()+":", body.ServerEvent.GetText())
		case *protocolv1.WireMessage_Error:
			fmt.Println("Server error:", body.Error.GetMessage())
		case *protocolv1.WireMessage_Pong:
			fmt.Println("Received pong at", body.Pong.GetSentAtUnixMs())
		default:
			fmt.Println("Received unsupported message")
		}
	}
}
