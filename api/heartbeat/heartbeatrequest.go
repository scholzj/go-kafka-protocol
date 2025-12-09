package heartbeat

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	HeartbeatRequestApiKey        = 12
	HeartbeatRequestHeaderVersion = 1
)

// HeartbeatRequest represents a request message.
type HeartbeatRequest struct {
	// The group id.
	GroupId string `json:"groupid" versions:"0-999"`
	// The generation of the group.
	GenerationId int32 `json:"generationid" versions:"0-999"`
	// The member ID.
	MemberId string `json:"memberid" versions:"0-999"`
	// The unique identifier of the consumer instance provided by end user.
	GroupInstanceId *string `json:"groupinstanceid" versions:"3-999"`
}

// Encode encodes a HeartbeatRequest to a byte slice for the given version.
func (m *HeartbeatRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a HeartbeatRequest from a byte slice for the given version.
func (m *HeartbeatRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a HeartbeatRequest to an io.Writer for the given version.
func (m *HeartbeatRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.GroupId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.GroupId); err != nil {
				return err
			}
		}
	}
	// GenerationId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.GenerationId); err != nil {
			return err
		}
	}
	// MemberId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.MemberId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.MemberId); err != nil {
				return err
			}
		}
	}
	// GroupInstanceId
	if version >= 3 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.GroupInstanceId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.GroupInstanceId); err != nil {
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

// Read reads a HeartbeatRequest from an io.Reader for the given version.
func (m *HeartbeatRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		}
	}
	// GenerationId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.GenerationId = val
	}
	// MemberId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.MemberId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.MemberId = val
		}
	}
	// GroupInstanceId
	if version >= 3 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.GroupInstanceId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.GroupInstanceId = val
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

// writeTaggedFields writes tagged fields for HeartbeatRequest.
func (m *HeartbeatRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for HeartbeatRequest.
func (m *HeartbeatRequest) readTaggedFields(r io.Reader, version int16) error {
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
