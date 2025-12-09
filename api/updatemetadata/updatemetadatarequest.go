package updatemetadata

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	UpdateMetadataRequestApiKey        = 6
	UpdateMetadataRequestHeaderVersion = 1
)

// UpdateMetadataRequest represents a request message.
type UpdateMetadataRequest struct {
}

// Encode encodes a UpdateMetadataRequest to a byte slice for the given version.
func (m *UpdateMetadataRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a UpdateMetadataRequest from a byte slice for the given version.
func (m *UpdateMetadataRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a UpdateMetadataRequest to an io.Writer for the given version.
func (m *UpdateMetadataRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}

// Read reads a UpdateMetadataRequest from an io.Reader for the given version.
func (m *UpdateMetadataRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	return nil
}
