package syncgroup

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	SyncGroupRequestApiKey        = 14
	SyncGroupRequestHeaderVersion = 1
)

// SyncGroupRequest represents a request message.
type SyncGroupRequest struct {
	// The unique group identifier.
	GroupId string `json:"groupid" versions:"0-999"`
	// The generation of the group.
	GenerationId int32 `json:"generationid" versions:"0-999"`
	// The member ID assigned by the group.
	MemberId string `json:"memberid" versions:"0-999"`
	// The unique identifier of the consumer instance provided by end user.
	GroupInstanceId *string `json:"groupinstanceid" versions:"3-999"`
	// The group protocol type.
	ProtocolType *string `json:"protocoltype" versions:"5-999"`
	// The group protocol name.
	ProtocolName *string `json:"protocolname" versions:"5-999"`
	// Each assignment.
	Assignments []SyncGroupRequestSyncGroupRequestAssignment `json:"assignments" versions:"0-999"`
}

// Encode encodes a SyncGroupRequest to a byte slice for the given version.
func (m *SyncGroupRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a SyncGroupRequest from a byte slice for the given version.
func (m *SyncGroupRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a SyncGroupRequest to an io.Writer for the given version.
func (m *SyncGroupRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 5 {
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
	// Assignments
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Assignments) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Assignments))); err != nil {
				return err
			}
		}
		for i := range m.Assignments {
			// MemberId
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Assignments[i].MemberId); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Assignments[i].MemberId); err != nil {
						return err
					}
				}
			}
			// Assignment
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactBytes(w, m.Assignments[i].Assignment); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteBytes(w, m.Assignments[i].Assignment); err != nil {
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

// Read reads a SyncGroupRequest from an io.Reader for the given version.
func (m *SyncGroupRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 5 {
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
	// Assignments
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
			m.Assignments = make([]SyncGroupRequestSyncGroupRequestAssignment, length)
			for i := int32(0); i < length; i++ {
				// MemberId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Assignments[i].MemberId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Assignments[i].MemberId = val
					}
				}
				// Assignment
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						m.Assignments[i].Assignment = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						m.Assignments[i].Assignment = val
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Assignments = make([]SyncGroupRequestSyncGroupRequestAssignment, length)
			for i := int32(0); i < length; i++ {
				// MemberId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Assignments[i].MemberId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Assignments[i].MemberId = val
					}
				}
				// Assignment
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						m.Assignments[i].Assignment = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						m.Assignments[i].Assignment = val
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

// SyncGroupRequestSyncGroupRequestAssignment represents Each assignment..
type SyncGroupRequestSyncGroupRequestAssignment struct {
	// The ID of the member to assign.
	MemberId string `json:"memberid" versions:"0-999"`
	// The member assignment.
	Assignment []byte `json:"assignment" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for SyncGroupRequest.
func (m *SyncGroupRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for SyncGroupRequest.
func (m *SyncGroupRequest) readTaggedFields(r io.Reader, version int16) error {
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
