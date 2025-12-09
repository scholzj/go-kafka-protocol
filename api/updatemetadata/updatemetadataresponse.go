package updatemetadata

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	UpdateMetadataResponseApiKey        = 6
	UpdateMetadataResponseHeaderVersion = 1
)

// UpdateMetadataResponse represents a response message.
type UpdateMetadataResponse struct {
}

// Encode encodes a UpdateMetadataResponse to a byte slice for the given version.
func (m *UpdateMetadataResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a UpdateMetadataResponse from a byte slice for the given version.
func (m *UpdateMetadataResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a UpdateMetadataResponse to an io.Writer for the given version.
func (m *UpdateMetadataResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}

// Read reads a UpdateMetadataResponse from an io.Reader for the given version.
func (m *UpdateMetadataResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}
