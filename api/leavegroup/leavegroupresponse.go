package leavegroup

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	LeaveGroupResponseApiKey        = 13
	LeaveGroupResponseHeaderVersion = 1
)

// LeaveGroupResponse represents a response message.
type LeaveGroupResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"1-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// List of leaving member responses.
	Members []LeaveGroupResponseMemberResponse `json:"members" versions:"3-999"`
}

// Encode encodes a LeaveGroupResponse to a byte slice for the given version.
func (m *LeaveGroupResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a LeaveGroupResponse from a byte slice for the given version.
func (m *LeaveGroupResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a LeaveGroupResponse to an io.Writer for the given version.
func (m *LeaveGroupResponse) Write(w io.Writer, version int16) error {
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
	// Members
	if version >= 3 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Members) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Members))); err != nil {
				return err
			}
		}
		for i := range m.Members {
			// MemberId
			if version >= 3 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Members[i].MemberId); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Members[i].MemberId); err != nil {
						return err
					}
				}
			}
			// GroupInstanceId
			if version >= 3 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Members[i].GroupInstanceId); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Members[i].GroupInstanceId); err != nil {
						return err
					}
				}
			}
			// ErrorCode
			if version >= 3 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Members[i].ErrorCode); err != nil {
					return err
				}
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

// Read reads a LeaveGroupResponse from an io.Reader for the given version.
func (m *LeaveGroupResponse) Read(r io.Reader, version int16) error {
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
	// Members
	if version >= 3 && version <= 999 {
		var length int32
		if isFlexible {
			var lengthUint uint32
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint < 1 {
				return errors.New("invalid compact array length")
			}
			length = int32(lengthUint - 1)
			m.Members = make([]LeaveGroupResponseMemberResponse, length)
			for i := int32(0); i < length; i++ {
				// MemberId
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Members[i].MemberId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Members[i].MemberId = val
					}
				}
				// GroupInstanceId
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Members[i].GroupInstanceId = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Members[i].GroupInstanceId = val
					}
				}
				// ErrorCode
				if version >= 3 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Members[i].ErrorCode = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Members = make([]LeaveGroupResponseMemberResponse, length)
			for i := int32(0); i < length; i++ {
				// MemberId
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Members[i].MemberId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Members[i].MemberId = val
					}
				}
				// GroupInstanceId
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Members[i].GroupInstanceId = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Members[i].GroupInstanceId = val
					}
				}
				// ErrorCode
				if version >= 3 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Members[i].ErrorCode = val
				}
			}
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

// LeaveGroupResponseMemberResponse represents List of leaving member responses..
type LeaveGroupResponseMemberResponse struct {
	// The member ID to remove from the group.
	MemberId string `json:"memberid" versions:"3-999"`
	// The group instance ID to remove from the group.
	GroupInstanceId *string `json:"groupinstanceid" versions:"3-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"3-999"`
}

// writeTaggedFields writes tagged fields for LeaveGroupResponse.
func (m *LeaveGroupResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for LeaveGroupResponse.
func (m *LeaveGroupResponse) readTaggedFields(r io.Reader, version int16) error {
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
