package syncgroup

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	SyncGroupResponseApiKey        = 14
	SyncGroupResponseHeaderVersion = 1
)

// SyncGroupResponse represents a response message.
type SyncGroupResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"1-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The group protocol type.
	ProtocolType *string `json:"protocoltype" versions:"5-999"`
	// The group protocol name.
	ProtocolName *string `json:"protocolname" versions:"5-999"`
	// The member assignment.
	Assignment []byte `json:"assignment" versions:"0-999"`
}

// Encode encodes a SyncGroupResponse to a byte slice for the given version.
func (m *SyncGroupResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a SyncGroupResponse from a byte slice for the given version.
func (m *SyncGroupResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a SyncGroupResponse to an io.Writer for the given version.
func (m *SyncGroupResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
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
	// ProtocolType
	if version >= 5 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ProtocolType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ProtocolType); err != nil {
				return err
			}
		}
	}
	// ProtocolName
	if version >= 5 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ProtocolName); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ProtocolName); err != nil {
				return err
			}
		}
	}
	// Assignment
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactBytes(w, m.Assignment); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteBytes(w, m.Assignment); err != nil {
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

// Read reads a SyncGroupResponse from an io.Reader for the given version.
func (m *SyncGroupResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
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
	// ProtocolType
	if version >= 5 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ProtocolType = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ProtocolType = val
		}
	}
	// ProtocolName
	if version >= 5 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ProtocolName = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ProtocolName = val
		}
	}
	// Assignment
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactBytes(r)
			if err != nil {
				return err
			}
			m.Assignment = val
		} else {
			val, err := protocol.ReadBytes(r)
			if err != nil {
				return err
			}
			m.Assignment = val
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

// writeTaggedFields writes tagged fields for SyncGroupResponse.
func (m *SyncGroupResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for SyncGroupResponse.
func (m *SyncGroupResponse) readTaggedFields(r io.Reader, version int16) error {
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
