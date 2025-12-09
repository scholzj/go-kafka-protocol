package gettelemetrysubscriptions

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	GetTelemetrySubscriptionsResponseApiKey        = 71
	GetTelemetrySubscriptionsResponseHeaderVersion = 1
)

// GetTelemetrySubscriptionsResponse represents a response message.
type GetTelemetrySubscriptionsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// Assigned client instance id if ClientInstanceId was 0 in the request, else 0.
	ClientInstanceId uuid.UUID `json:"clientinstanceid" versions:"0-999"`
	// Unique identifier for the current subscription set for this client instance.
	SubscriptionId int32 `json:"subscriptionid" versions:"0-999"`
	// Compression types that broker accepts for the PushTelemetryRequest.
	AcceptedCompressionTypes []int8 `json:"acceptedcompressiontypes" versions:"0-999"`
	// Configured push interval, which is the lowest configured interval in the current subscription set.
	PushIntervalMs int32 `json:"pushintervalms" versions:"0-999"`
	// The maximum bytes of binary data the broker accepts in PushTelemetryRequest.
	TelemetryMaxBytes int32 `json:"telemetrymaxbytes" versions:"0-999"`
	// Flag to indicate monotonic/counter metrics are to be emitted as deltas or cumulative values.
	DeltaTemporality bool `json:"deltatemporality" versions:"0-999"`
	// Requested metrics prefix string match. Empty array: No metrics subscribed, Array[0] empty string: All metrics subscribed.
	RequestedMetrics []string `json:"requestedmetrics" versions:"0-999"`
}

// Encode encodes a GetTelemetrySubscriptionsResponse to a byte slice for the given version.
func (m *GetTelemetrySubscriptionsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a GetTelemetrySubscriptionsResponse from a byte slice for the given version.
func (m *GetTelemetrySubscriptionsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a GetTelemetrySubscriptionsResponse to an io.Writer for the given version.
func (m *GetTelemetrySubscriptionsResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// ErrorCode
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
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
	// AcceptedCompressionTypes
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactInt8Array(w, m.AcceptedCompressionTypes); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt8Array(w, m.AcceptedCompressionTypes); err != nil {
				return err
			}
		}
	}
	// PushIntervalMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.PushIntervalMs); err != nil {
			return err
		}
	}
	// TelemetryMaxBytes
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TelemetryMaxBytes); err != nil {
			return err
		}
	}
	// DeltaTemporality
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.DeltaTemporality); err != nil {
			return err
		}
	}
	// RequestedMetrics
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactStringArray(w, m.RequestedMetrics); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteStringArray(w, m.RequestedMetrics); err != nil {
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

// Read reads a GetTelemetrySubscriptionsResponse from an io.Reader for the given version.
func (m *GetTelemetrySubscriptionsResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// ErrorCode
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
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
	// AcceptedCompressionTypes
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactInt8Array(r)
			if err != nil {
				return err
			}
			m.AcceptedCompressionTypes = val
		} else {
			val, err := protocol.ReadInt8Array(r)
			if err != nil {
				return err
			}
			m.AcceptedCompressionTypes = val
		}
	}
	// PushIntervalMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.PushIntervalMs = val
	}
	// TelemetryMaxBytes
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.TelemetryMaxBytes = val
	}
	// DeltaTemporality
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.DeltaTemporality = val
	}
	// RequestedMetrics
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactStringArray(r)
			if err != nil {
				return err
			}
			m.RequestedMetrics = val
		} else {
			val, err := protocol.ReadStringArray(r)
			if err != nil {
				return err
			}
			m.RequestedMetrics = val
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

// writeTaggedFields writes tagged fields for GetTelemetrySubscriptionsResponse.
func (m *GetTelemetrySubscriptionsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for GetTelemetrySubscriptionsResponse.
func (m *GetTelemetrySubscriptionsResponse) readTaggedFields(r io.Reader, version int16) error {
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
