package main

import (
	"fmt"
	"log"
	"net"

	"github.com/l3hu4l1/chatroom/protocol"
	"google.golang.org/protobuf/proto"
)

func main() {
	conn, err := net.Dial("tcp", ":8080")
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer conn.Close()

	var nickname string
	fmt.Print("Enter your nickname: ")
	fmt.Scanln(&nickname)
	fmt.Println("Welcome,", nickname)
	go handleConnection(conn)

	for {
		var msg string
		fmt.Print("Enter message: ")
		fmt.Scanln(&msg)

		data := &protocol.ConnServer{
			Nickname: nickname,
			Msg:      msg,
		}

		dataBytes, err := proto.Marshal(data)
		if err != nil {
			log.Fatal("Failed to marshal message:", err)
		}

		_, err = conn.Write(dataBytes)
		if err != nil {
			log.Fatal("Failed to send message:", err)
		}

		if msg == "quit" {
			fmt.Println("See you next time!")
			return
		}
	}
}

func handleConnection(conn net.Conn) {
	for {
		msg := make([]byte, 255)
		n, err := conn.Read(msg)
		if n <= 0 || err != nil {
			break
		}

		data := &protocol.ConnServer{}
		err = proto.Unmarshal(msg[:n], data)
		if err != nil {
			log.Fatal("Failed to unmarshal message:", err)
		}

		fmt.Println("Received message from", data.GetNickname() + ":", data.GetMsg())
	}
}