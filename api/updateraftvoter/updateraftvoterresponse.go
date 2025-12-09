package updateraftvoter

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	UpdateRaftVoterResponseApiKey        = 82
	UpdateRaftVoterResponseHeaderVersion = 1
)

// UpdateRaftVoterResponse represents a response message.
type UpdateRaftVoterResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// Details of the current Raft cluster leader.
	CurrentLeader UpdateRaftVoterResponseCurrentLeader `json:"currentleader" versions:"0-999" tag:"0"`
}

// Encode encodes a UpdateRaftVoterResponse to a byte slice for the given version.
func (m *UpdateRaftVoterResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a UpdateRaftVoterResponse from a byte slice for the given version.
func (m *UpdateRaftVoterResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a UpdateRaftVoterResponse to an io.Writer for the given version.
func (m *UpdateRaftVoterResponse) Write(w io.Writer, version int16) error {
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
	// CurrentLeader
	if version >= 0 && version <= 999 {
	}
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a UpdateRaftVoterResponse from an io.Reader for the given version.
func (m *UpdateRaftVoterResponse) Read(r io.Reader, version int16) error {
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
	// CurrentLeader
	if version >= 0 && version <= 999 {
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// UpdateRaftVoterResponseCurrentLeader represents Details of the current Raft cluster leader..
type UpdateRaftVoterResponseCurrentLeader struct {
	// The replica id of the current leader or -1 if the leader is unknown.
	LeaderId int32 `json:"leaderid" versions:"0-999"`
	// The latest known leader epoch.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// The node's hostname.
	Host string `json:"host" versions:"0-999"`
	// The node's port.
	Port int32 `json:"port" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for UpdateRaftVoterResponseCurrentLeader.
func (m *UpdateRaftVoterResponseCurrentLeader) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for UpdateRaftVoterResponseCurrentLeader.
func (m *UpdateRaftVoterResponseCurrentLeader) readTaggedFields(r io.Reader, version int16) error {
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
		}
	}

	return nil
}

// writeTaggedFields writes tagged fields for UpdateRaftVoterResponse.
func (m *UpdateRaftVoterResponse) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	isFlexible := version >= 0

	// CurrentLeader (tag 0)
	if version >= 0 {
		if true {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(0)); err != nil {
				return err
			}
			// LeaderId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.CurrentLeader.LeaderId); err != nil {
					return err
				}
			}
			// LeaderEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.CurrentLeader.LeaderEpoch); err != nil {
					return err
				}
			}
			// Host
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.CurrentLeader.Host); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.CurrentLeader.Host); err != nil {
						return err
					}
				}
			}
			// Port
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.CurrentLeader.Port); err != nil {
					return err
				}
			}
			taggedFieldsCount++
		}
	}

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

// readTaggedFields reads tagged fields for UpdateRaftVoterResponse.
func (m *UpdateRaftVoterResponse) readTaggedFields(r io.Reader, version int16) error {
	isFlexible := version >= 0

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
		case 0: // CurrentLeader
			if version >= 0 {
				// LeaderId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.CurrentLeader.LeaderId = val
				}
				// LeaderEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.CurrentLeader.LeaderEpoch = val
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.CurrentLeader.Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.CurrentLeader.Host = val
					}
				}
				// Port
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.CurrentLeader.Port = val
				}
			}
		default:
			// Unknown tag, skip it
			// Read and discard the field data
			// For now, we'll need to know the type to skip properly
			// This is a simplified implementation
		}
	}

	return nil
}
