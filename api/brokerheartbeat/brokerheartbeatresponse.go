package brokerheartbeat

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	BrokerHeartbeatResponseApiKey        = 63
	BrokerHeartbeatResponseHeaderVersion = 1
)

// BrokerHeartbeatResponse represents a response message.
type BrokerHeartbeatResponse struct {
	// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// True if the broker has approximately caught up with the latest metadata.
	IsCaughtUp bool `json:"iscaughtup" versions:"0-999"`
	// True if the broker is fenced.
	IsFenced bool `json:"isfenced" versions:"0-999"`
	// True if the broker should proceed with its shutdown.
	ShouldShutDown bool `json:"shouldshutdown" versions:"0-999"`
}

// Encode encodes a BrokerHeartbeatResponse to a byte slice for the given version.
func (m *BrokerHeartbeatResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a BrokerHeartbeatResponse from a byte slice for the given version.
func (m *BrokerHeartbeatResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a BrokerHeartbeatResponse to an io.Writer for the given version.
func (m *BrokerHeartbeatResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
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
	// IsCaughtUp
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.IsCaughtUp); err != nil {
			return err
		}
	}
	// IsFenced
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.IsFenced); err != nil {
			return err
		}
	}
	// ShouldShutDown
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.ShouldShutDown); err != nil {
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

// Read reads a BrokerHeartbeatResponse from an io.Reader for the given version.
func (m *BrokerHeartbeatResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
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
	// IsCaughtUp
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IsCaughtUp = val
	}
	// IsFenced
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IsFenced = val
	}
	// ShouldShutDown
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.ShouldShutDown = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for BrokerHeartbeatResponse.
func (m *BrokerHeartbeatResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for BrokerHeartbeatResponse.
func (m *BrokerHeartbeatResponse) readTaggedFields(r io.Reader, version int16) error {
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
