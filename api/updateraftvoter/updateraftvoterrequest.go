package updateraftvoter

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	UpdateRaftVoterRequestApiKey        = 82
	UpdateRaftVoterRequestHeaderVersion = 1
)

// UpdateRaftVoterRequest represents a request message.
type UpdateRaftVoterRequest struct {
	// The cluster id.
	ClusterId *string `json:"clusterid" versions:"0-999"`
	// The current leader epoch of the partition, -1 for unknown leader epoch.
	CurrentLeaderEpoch int32 `json:"currentleaderepoch" versions:"0-999"`
	// The replica id of the voter getting updated in the topic partition.
	VoterId int32 `json:"voterid" versions:"0-999"`
	// The directory id of the voter getting updated in the topic partition.
	VoterDirectoryId uuid.UUID `json:"voterdirectoryid" versions:"0-999"`
	// The endpoint that can be used to communicate with the leader.
	Listeners []UpdateRaftVoterRequestListener `json:"listeners" versions:"0-999"`
	// The range of versions of the protocol that the replica supports.
	KRaftVersionFeature UpdateRaftVoterRequestKRaftVersionFeature `json:"kraftversionfeature" versions:"0-999"`
}

// Encode encodes a UpdateRaftVoterRequest to a byte slice for the given version.
func (m *UpdateRaftVoterRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a UpdateRaftVoterRequest from a byte slice for the given version.
func (m *UpdateRaftVoterRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a UpdateRaftVoterRequest to an io.Writer for the given version.
func (m *UpdateRaftVoterRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ClusterId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ClusterId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ClusterId); err != nil {
				return err
			}
		}
	}
	// CurrentLeaderEpoch
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.CurrentLeaderEpoch); err != nil {
			return err
		}
	}
	// VoterId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.VoterId); err != nil {
			return err
		}
	}
	// VoterDirectoryId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteUUID(w, m.VoterDirectoryId); err != nil {
			return err
		}
	}
	// Listeners
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Listeners) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Listeners))); err != nil {
				return err
			}
		}
		for i := range m.Listeners {
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Listeners[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Listeners[i].Name); err != nil {
						return err
					}
				}
			}
			// Host
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Listeners[i].Host); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Listeners[i].Host); err != nil {
						return err
					}
				}
			}
			// Port
			if version >= 0 && version <= 999 {
			}
		}
	}
	// KRaftVersionFeature
	if version >= 0 && version <= 999 {
		// MinSupportedVersion
		if version >= 0 && version <= 999 {
			if err := protocol.WriteInt16(w, m.KRaftVersionFeature.MinSupportedVersion); err != nil {
				return err
			}
		}
		// MaxSupportedVersion
		if version >= 0 && version <= 999 {
			if err := protocol.WriteInt16(w, m.KRaftVersionFeature.MaxSupportedVersion); err != nil {
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

// Read reads a UpdateRaftVoterRequest from an io.Reader for the given version.
func (m *UpdateRaftVoterRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ClusterId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		}
	}
	// CurrentLeaderEpoch
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.CurrentLeaderEpoch = val
	}
	// VoterId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.VoterId = val
	}
	// VoterDirectoryId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadUUID(r)
		if err != nil {
			return err
		}
		m.VoterDirectoryId = val
	}
	// Listeners
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
			m.Listeners = make([]UpdateRaftVoterRequestListener, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Name = val
					}
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Host = val
					}
				}
				// Port
				if version >= 0 && version <= 999 {
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Listeners = make([]UpdateRaftVoterRequestListener, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Name = val
					}
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Host = val
					}
				}
				// Port
				if version >= 0 && version <= 999 {
				}
			}
		}
	}
	// KRaftVersionFeature
	if version >= 0 && version <= 999 {
		// MinSupportedVersion
		if version >= 0 && version <= 999 {
			val, err := protocol.ReadInt16(r)
			if err != nil {
				return err
			}
			m.KRaftVersionFeature.MinSupportedVersion = val
		}
		// MaxSupportedVersion
		if version >= 0 && version <= 999 {
			val, err := protocol.ReadInt16(r)
			if err != nil {
				return err
			}
			m.KRaftVersionFeature.MaxSupportedVersion = val
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

// UpdateRaftVoterRequestListener represents The endpoint that can be used to communicate with the leader..
type UpdateRaftVoterRequestListener struct {
	// The name of the endpoint.
	Name string `json:"name" versions:"0-999"`
	// The hostname.
	Host string `json:"host" versions:"0-999"`
	// The port.
	Port uint16 `json:"port" versions:"0-999"`
}

// UpdateRaftVoterRequestKRaftVersionFeature represents The range of versions of the protocol that the replica supports..
type UpdateRaftVoterRequestKRaftVersionFeature struct {
	// The minimum supported KRaft protocol version.
	MinSupportedVersion int16 `json:"minsupportedversion" versions:"0-999"`
	// The maximum supported KRaft protocol version.
	MaxSupportedVersion int16 `json:"maxsupportedversion" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for UpdateRaftVoterRequest.
func (m *UpdateRaftVoterRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for UpdateRaftVoterRequest.
func (m *UpdateRaftVoterRequest) readTaggedFields(r io.Reader, version int16) error {
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
