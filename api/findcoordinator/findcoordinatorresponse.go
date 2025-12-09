package findcoordinator

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	FindCoordinatorResponseApiKey        = 10
	FindCoordinatorResponseHeaderVersion = 1
)

// FindCoordinatorResponse represents a response message.
type FindCoordinatorResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"1-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-3"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"1-3"`
	// The node id.
	NodeId int32 `json:"nodeid" versions:"0-3"`
	// The host name.
	Host string `json:"host" versions:"0-3"`
	// The port.
	Port int32 `json:"port" versions:"0-3"`
	// Each coordinator result in the response.
	Coordinators []FindCoordinatorResponseCoordinator `json:"coordinators" versions:"4-999"`
}

// Encode encodes a FindCoordinatorResponse to a byte slice for the given version.
func (m *FindCoordinatorResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a FindCoordinatorResponse from a byte slice for the given version.
func (m *FindCoordinatorResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a FindCoordinatorResponse to an io.Writer for the given version.
func (m *FindCoordinatorResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// ErrorCode
	if version >= 0 && version <= 3 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// ErrorMessage
	if version >= 1 && version <= 3 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ErrorMessage); err != nil {
				return err
			}
		}
	}
	// NodeId
	if version >= 0 && version <= 3 {
		if err := protocol.WriteInt32(w, m.NodeId); err != nil {
			return err
		}
	}
	// Host
	if version >= 0 && version <= 3 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.Host); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.Host); err != nil {
				return err
			}
		}
	}
	// Port
	if version >= 0 && version <= 3 {
		if err := protocol.WriteInt32(w, m.Port); err != nil {
			return err
		}
	}
	// Coordinators
	if version >= 4 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Coordinators) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Coordinators))); err != nil {
				return err
			}
		}
		for i := range m.Coordinators {
			// Key
			if version >= 4 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Coordinators[i].Key); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Coordinators[i].Key); err != nil {
						return err
					}
				}
			}
			// NodeId
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Coordinators[i].NodeId); err != nil {
					return err
				}
			}
			// Host
			if version >= 4 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Coordinators[i].Host); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Coordinators[i].Host); err != nil {
						return err
					}
				}
			}
			// Port
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Coordinators[i].Port); err != nil {
					return err
				}
			}
			// ErrorCode
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Coordinators[i].ErrorCode); err != nil {
					return err
				}
			}
			// ErrorMessage
			if version >= 4 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Coordinators[i].ErrorMessage); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Coordinators[i].ErrorMessage); err != nil {
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

// Read reads a FindCoordinatorResponse from an io.Reader for the given version.
func (m *FindCoordinatorResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
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
	if version >= 0 && version <= 3 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// ErrorMessage
	if version >= 1 && version <= 3 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ErrorMessage = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ErrorMessage = val
		}
	}
	// NodeId
	if version >= 0 && version <= 3 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.NodeId = val
	}
	// Host
	if version >= 0 && version <= 3 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.Host = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.Host = val
		}
	}
	// Port
	if version >= 0 && version <= 3 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.Port = val
	}
	// Coordinators
	if version >= 4 && version <= 999 {
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
			m.Coordinators = make([]FindCoordinatorResponseCoordinator, length)
			for i := int32(0); i < length; i++ {
				// Key
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].Key = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].Key = val
					}
				}
				// NodeId
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Coordinators[i].NodeId = val
				}
				// Host
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].Host = val
					}
				}
				// Port
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Coordinators[i].Port = val
				}
				// ErrorCode
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Coordinators[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].ErrorMessage = val
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Coordinators = make([]FindCoordinatorResponseCoordinator, length)
			for i := int32(0); i < length; i++ {
				// Key
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].Key = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].Key = val
					}
				}
				// NodeId
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Coordinators[i].NodeId = val
				}
				// Host
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].Host = val
					}
				}
				// Port
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Coordinators[i].Port = val
				}
				// ErrorCode
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Coordinators[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Coordinators[i].ErrorMessage = val
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

// FindCoordinatorResponseCoordinator represents Each coordinator result in the response..
type FindCoordinatorResponseCoordinator struct {
	// The coordinator key.
	Key string `json:"key" versions:"4-999"`
	// The node id.
	NodeId int32 `json:"nodeid" versions:"4-999"`
	// The host name.
	Host string `json:"host" versions:"4-999"`
	// The port.
	Port int32 `json:"port" versions:"4-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"4-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"4-999"`
}

// writeTaggedFields writes tagged fields for FindCoordinatorResponse.
func (m *FindCoordinatorResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for FindCoordinatorResponse.
func (m *FindCoordinatorResponse) readTaggedFields(r io.Reader, version int16) error {
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
