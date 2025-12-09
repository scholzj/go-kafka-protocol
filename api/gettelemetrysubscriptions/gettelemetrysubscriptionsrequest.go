package gettelemetrysubscriptions

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	GetTelemetrySubscriptionsRequestApiKey        = 71
	GetTelemetrySubscriptionsRequestHeaderVersion = 1
)

// GetTelemetrySubscriptionsRequest represents a request message.
type GetTelemetrySubscriptionsRequest struct {
	// Unique id for this client instance, must be set to 0 on the first request.
	ClientInstanceId uuid.UUID `json:"clientinstanceid" versions:"0-999"`
}

// Encode encodes a GetTelemetrySubscriptionsRequest to a byte slice for the given version.
func (m *GetTelemetrySubscriptionsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a GetTelemetrySubscriptionsRequest from a byte slice for the given version.
func (m *GetTelemetrySubscriptionsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a GetTelemetrySubscriptionsRequest to an io.Writer for the given version.
func (m *GetTelemetrySubscriptionsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ClientInstanceId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteUUID(w, m.ClientInstanceId); err != nil {
			return err
		}
	}
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a GetTelemetrySubscriptionsRequest from an io.Reader for the given version.
func (m *GetTelemetrySubscriptionsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ClientInstanceId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadUUID(r)
		if err != nil {
			return err
		}
		m.ClientInstanceId = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for GetTelemetrySubscriptionsRequest.
func (m *GetTelemetrySubscriptionsRequest) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	// Write tagged fields count
	if err := protocol.WriteVaruint32(w, uint32(taggedFieldsCount)); err != nil {
		return err
	}

	// Write tagged fields data
	if taggedFieldsCount > 0 {
		if _, err := w.Write(taggedFieldsBuf.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

// readTaggedFields reads tagged fields for GetTelemetrySubscriptionsRequest.
func (m *GetTelemetrySubscriptionsRequest) readTaggedFields(r io.Reader, version int16) error {
	// Read tagged fields count
	count, err := protocol.ReadVaruint32(r)
	if err != nil {
		return err
	}

	if count == 0 {
		return nil
	}

	// Read tagged fields
	for i := uint32(0); i < count; i++ {
		tag, err := protocol.ReadVaruint32(r)
		if err != nil {
			return err
		}

		switch tag {
		default:
			// Unknown tag, skip it
			// Read and discard the field data
			// For now, we'll need to know the type to skip properly
			// This is a simplified implementation
		}
	}

	return nil
}
