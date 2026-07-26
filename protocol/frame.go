package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

const (
	ProtocolVersion uint32 = 1
	MaxFrameSize           = 1 << 20
)

var ErrFrameTooLarge = errors.New("protocol frame too large")

func WriteWireMessage(w io.Writer, message *WireMessage) error {
	payload, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	if len(payload) == 0 {
		return fmt.Errorf("%w: empty payload", ErrFrameTooLarge)
	}
	if len(payload) > MaxFrameSize {
		return fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(payload))
	}

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))

	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	_, err = w.Write(payload)
	return err
}

func ReadWireMessage(r io.Reader) (*WireMessage, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	frameSize := binary.BigEndian.Uint32(header[:])
	if frameSize == 0 {
		return nil, fmt.Errorf("empty protocol frame")
	}
	if frameSize > MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, frameSize)
	}

payload := make([]byte, int(frameSize))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	message := &WireMessage{}
	if err := proto.Unmarshal(payload, message); err != nil {
		return nil, err
	}

	return message, nil
}
