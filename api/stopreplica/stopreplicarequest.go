package stopreplica

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	StopReplicaRequestApiKey        = 5
	StopReplicaRequestHeaderVersion = 1
)

// StopReplicaRequest represents a request message.
type StopReplicaRequest struct {
}

// Encode encodes a StopReplicaRequest to a byte slice for the given version.
func (m *StopReplicaRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a StopReplicaRequest from a byte slice for the given version.
func (m *StopReplicaRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a StopReplicaRequest to an io.Writer for the given version.
func (m *StopReplicaRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}

// Read reads a StopReplicaRequest from an io.Reader for the given version.
func (m *StopReplicaRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}
