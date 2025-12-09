package controlledshutdown

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ControlledShutdownResponseApiKey        = 7
	ControlledShutdownResponseHeaderVersion = 1
)

// ControlledShutdownResponse represents a response message.
type ControlledShutdownResponse struct {
}

// Encode encodes a ControlledShutdownResponse to a byte slice for the given version.
func (m *ControlledShutdownResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ControlledShutdownResponse from a byte slice for the given version.
func (m *ControlledShutdownResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ControlledShutdownResponse to an io.Writer for the given version.
func (m *ControlledShutdownResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}

// Read reads a ControlledShutdownResponse from an io.Reader for the given version.
func (m *ControlledShutdownResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}
