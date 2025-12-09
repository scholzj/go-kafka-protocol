package controlledshutdown

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ControlledShutdownRequestApiKey        = 7
	ControlledShutdownRequestHeaderVersion = 1
)

// ControlledShutdownRequest represents a request message.
type ControlledShutdownRequest struct {
}

// Encode encodes a ControlledShutdownRequest to a byte slice for the given version.
func (m *ControlledShutdownRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ControlledShutdownRequest from a byte slice for the given version.
func (m *ControlledShutdownRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ControlledShutdownRequest to an io.Writer for the given version.
func (m *ControlledShutdownRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}

// Read reads a ControlledShutdownRequest from an io.Reader for the given version.
func (m *ControlledShutdownRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}
