package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"google.golang.org/protobuf/proto"
)

type chunkedReader struct {
	data  []byte
	chunk int
	index int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.index >= len(r.data) {
		return 0, io.EOF
	}

	limit := r.index + r.chunk
	if limit > len(r.data) {
		limit = len(r.data)
	}
	if limit-r.index > len(p) {
		limit = r.index + len(p)
	}

	n := copy(p, r.data[r.index:limit])
	r.index += n
	return n, nil
}

func TestReadWireMessageHandlesPartialReads(t *testing.T) {
	original := &WireMessage{
		Version: ProtocolVersion,
		Body: &WireMessage_ClientMessage{
			ClientMessage: &ClientMessage{Text: "hello world", Room: "general"},
		},
	}

	payload, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)

	message, err := ReadWireMessage(&chunkedReader{data: frame, chunk: 1})
	if err != nil {
		t.Fatalf("read wire message: %v", err)
	}

	if got := message.GetVersion(); got != ProtocolVersion {
		t.Fatalf("version = %d, want %d", got, ProtocolVersion)
	}

	clientMessage := message.GetClientMessage()
	if clientMessage == nil {
		t.Fatalf("client message is nil")
	}
	if got := clientMessage.GetText(); got != "hello world" {
		t.Fatalf("text = %q, want %q", got, "hello world")
	}
	if got := clientMessage.GetRoom(); got != "general" {
		t.Fatalf("room = %q, want %q", got, "general")
	}
}

func TestReadWireMessageRejectsOversizedFrame(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], MaxFrameSize+1)

	message, err := ReadWireMessage(bytes.NewReader(header[:]))
	if err == nil {
		t.Fatalf("expected error for oversized frame, got message %#v", message)
	}
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("error = %v, want ErrFrameTooLarge", err)
	}
}
