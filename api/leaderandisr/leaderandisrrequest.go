package leaderandisr

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	LeaderAndIsrRequestApiKey        = 4
	LeaderAndIsrRequestHeaderVersion = 1
)

// LeaderAndIsrRequest represents a request message.
type LeaderAndIsrRequest struct {
}

// Encode encodes a LeaderAndIsrRequest to a byte slice for the given version.
func (m *LeaderAndIsrRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a LeaderAndIsrRequest from a byte slice for the given version.
func (m *LeaderAndIsrRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a LeaderAndIsrRequest to an io.Writer for the given version.
func (m *LeaderAndIsrRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}

// Read reads a LeaderAndIsrRequest from an io.Reader for the given version.
func (m *LeaderAndIsrRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}
