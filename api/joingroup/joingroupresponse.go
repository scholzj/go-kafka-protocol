package joingroup

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	JoinGroupResponseApiKey        = 11
	JoinGroupResponseHeaderVersion = 1
)

// JoinGroupResponse represents a response message.
type JoinGroupResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"2-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The generation ID of the group.
	GenerationId int32 `json:"generationid" versions:"0-999"`
	// The group protocol name.
	ProtocolType *string `json:"protocoltype" versions:"7-999"`
	// The group protocol selected by the coordinator.
	ProtocolName *string `json:"protocolname" versions:"0-999"`
	// The leader of the group.
	Leader string `json:"leader" versions:"0-999"`
	// True if the leader must skip running the assignment.
	SkipAssignment bool `json:"skipassignment" versions:"9-999"`
	// The member ID assigned by the group coordinator.
	MemberId string `json:"memberid" versions:"0-999"`
	// The group members.
	Members []JoinGroupResponseJoinGroupResponseMember `json:"members" versions:"0-999"`
}

// Encode encodes a JoinGroupResponse to a byte slice for the given version.
func (m *JoinGroupResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a JoinGroupResponse from a byte slice for the given version.
func (m *JoinGroupResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a JoinGroupResponse to an io.Writer for the given version.
func (m *JoinGroupResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 9 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 2 && version <= 999 {
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
	// GenerationId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.GenerationId); err != nil {
			return err
		}
	}
	// ProtocolType
	if version >= 7 && version <= 999 {
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
	if version >= 0 && version <= 999 {
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
	// Leader
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.Leader); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.Leader); err != nil {
				return err
			}
		}
	}
	// SkipAssignment
	if version >= 9 && version <= 999 {
		if err := protocol.WriteBool(w, m.SkipAssignment); err != nil {
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
	// Members
	if version >= 0 && version <= 999 {
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
			if version >= 0 && version <= 999 {
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
			if version >= 5 && version <= 999 {
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
			// Metadata
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactBytes(w, m.Members[i].Metadata); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteBytes(w, m.Members[i].Metadata); err != nil {
						return err
					}
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

// Read reads a JoinGroupResponse from an io.Reader for the given version.
func (m *JoinGroupResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 9 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 2 && version <= 999 {
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
	// GenerationId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.GenerationId = val
	}
	// ProtocolType
	if version >= 7 && version <= 999 {
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
	if version >= 0 && version <= 999 {
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
	// Leader
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.Leader = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.Leader = val
		}
	}
	// SkipAssignment
	if version >= 9 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.SkipAssignment = val
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
	// Members
	if version >= 0 && version <= 999 {
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
			m.Members = make([]JoinGroupResponseJoinGroupResponseMember, length)
			for i := int32(0); i < length; i++ {
				// MemberId
				if version >= 0 && version <= 999 {
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
				if version >= 5 && version <= 999 {
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
				// Metadata
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						m.Members[i].Metadata = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						m.Members[i].Metadata = val
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Members = make([]JoinGroupResponseJoinGroupResponseMember, length)
			for i := int32(0); i < length; i++ {
				// MemberId
				if version >= 0 && version <= 999 {
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
				if version >= 5 && version <= 999 {
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
				// Metadata
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						m.Members[i].Metadata = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						m.Members[i].Metadata = val
					}
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

// JoinGroupResponseJoinGroupResponseMember represents The group members..
type JoinGroupResponseJoinGroupResponseMember struct {
	// The group member ID.
	MemberId string `json:"memberid" versions:"0-999"`
	// The unique identifier of the consumer instance provided by end user.
	GroupInstanceId *string `json:"groupinstanceid" versions:"5-999"`
	// The group member metadata.
	Metadata []byte `json:"metadata" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for JoinGroupResponse.
func (m *JoinGroupResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for JoinGroupResponse.
func (m *JoinGroupResponse) readTaggedFields(r io.Reader, version int16) error {
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
