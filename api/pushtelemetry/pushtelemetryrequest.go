package pushtelemetry

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	PushTelemetryRequestApiKey        = 72
	PushTelemetryRequestHeaderVersion = 1
)

// PushTelemetryRequest represents a request message.
type PushTelemetryRequest struct {
	// Unique id for this client instance.
	ClientInstanceId uuid.UUID `json:"clientinstanceid" versions:"0-999"`
	// Unique identifier for the current subscription.
	SubscriptionId int32 `json:"subscriptionid" versions:"0-999"`
	// Client is terminating the connection.
	Terminating bool `json:"terminating" versions:"0-999"`
	// Compression codec used to compress the metrics.
	CompressionType int8 `json:"compressiontype" versions:"0-999"`
	// Metrics encoded in OpenTelemetry MetricsData v1 protobuf format.
	Metrics []byte `json:"metrics" versions:"0-999"`
}

// Encode encodes a PushTelemetryRequest to a byte slice for the given version.
func (m *PushTelemetryRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a PushTelemetryRequest from a byte slice for the given version.
func (m *PushTelemetryRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a PushTelemetryRequest to an io.Writer for the given version.
func (m *PushTelemetryRequest) Write(w io.Writer, version int16) error {
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
	// SubscriptionId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.SubscriptionId); err != nil {
			return err
		}
	}
	// Terminating
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.Terminating); err != nil {
			return err
		}
	}
	// CompressionType
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt8(w, m.CompressionType); err != nil {
			return err
		}
	}
	// Metrics
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactBytes(w, m.Metrics); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteBytes(w, m.Metrics); err != nil {
				return err
			}
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

// Read reads a PushTelemetryRequest from an io.Reader for the given version.
func (m *PushTelemetryRequest) Read(r io.Reader, version int16) error {
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
	// SubscriptionId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.SubscriptionId = val
	}
	// Terminating
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.Terminating = val
	}
	// CompressionType
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		m.CompressionType = val
	}
	// Metrics
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactBytes(r)
			if err != nil {
				return err
			}
			m.Metrics = val
		} else {
			val, err := protocol.ReadBytes(r)
			if err != nil {
				return err
			}
			m.Metrics = val
		}
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for PushTelemetryRequest.
func (m *PushTelemetryRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for PushTelemetryRequest.
func (m *PushTelemetryRequest) readTaggedFields(r io.Reader, version int16) error {
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
