package leaderandisr

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	LeaderAndIsrResponseApiKey        = 4
	LeaderAndIsrResponseHeaderVersion = 1
)

// LeaderAndIsrResponse represents a response message.
type LeaderAndIsrResponse struct {
}

// Encode encodes a LeaderAndIsrResponse to a byte slice for the given version.
func (m *LeaderAndIsrResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a LeaderAndIsrResponse from a byte slice for the given version.
func (m *LeaderAndIsrResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a LeaderAndIsrResponse to an io.Writer for the given version.
func (m *LeaderAndIsrResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}

// Read reads a LeaderAndIsrResponse from an io.Reader for the given version.
func (m *LeaderAndIsrResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}
