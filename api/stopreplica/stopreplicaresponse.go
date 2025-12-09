package stopreplica

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	StopReplicaResponseApiKey        = 5
	StopReplicaResponseHeaderVersion = 1
)

// StopReplicaResponse represents a response message.
type StopReplicaResponse struct {
}

// Encode encodes a StopReplicaResponse to a byte slice for the given version.
func (m *StopReplicaResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a StopReplicaResponse from a byte slice for the given version.
func (m *StopReplicaResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a StopReplicaResponse to an io.Writer for the given version.
func (m *StopReplicaResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}

// Read reads a StopReplicaResponse from an io.Reader for the given version.
func (m *StopReplicaResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}
