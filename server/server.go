package main

import (
	"fmt"
	"net"
	"github.com/l3hu4l1/chatroom/protocol"
    "google.golang.org/protobuf/proto"
)

var connections = make(map[int]net.Conn)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error starting server:", err)
	}
	defer listener.Close()

	fmt.Println("Server is listening on port 8080...")

	key := 1
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
		}

		connections[key] = conn
		key++

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	for {
		msg := make([]byte, 255)
		n, err := conn.Read(msg)
		if n <= 0 || err != nil {
			continue
		}

		data := &protocol.ConnServer{}
        err = proto.Unmarshal(msg[:n], data)
        if err != nil {
            fmt.Println("Failed to unmarshal message:", err)
            continue
        }

		fmt.Printf("%s: %s\n", data.GetNickname(), data.GetMsg())

		for _, c := range connections {
			if c != conn {
				_, err := c.Write(msg[:n])
				if err != nil {
					fmt.Println("Error writing to connection:", err)
				}
			}
		}
	}
}