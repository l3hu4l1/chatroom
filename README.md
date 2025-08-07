# chatroom
a cli app written in Go, simulating a live chat environment

## usage
run the client and server executables separately

## overview
### client.go
handles server connection, sending/receiving messages, user interaction.

### server.go
handles multiple clients,  message broadcasting, chat logic.

### /protocol
contains protobuf files which allow efficient message schema and serialization

### concurrency
each client has its own goroutine for simultaneous connection and message broadcasting