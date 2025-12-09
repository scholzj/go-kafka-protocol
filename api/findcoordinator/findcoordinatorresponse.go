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
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(FindCoordinatorResponseCoordinator)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Key
			if version >= 4 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Key); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Key); err != nil {
						return nil, err
					}
				}
			}
			// NodeId
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.NodeId); err != nil {
					return nil, err
				}
			}
			// Host
			if version >= 4 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Host); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Host); err != nil {
						return nil, err
					}
				}
			}
			// Port
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.Port); err != nil {
					return nil, err
				}
			}
			// ErrorCode
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ErrorCode); err != nil {
					return nil, err
				}
			}
			// ErrorMessage
			if version >= 4 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.ErrorMessage); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.ErrorMessage); err != nil {
						return nil, err
					}
				}
			}
			// Write tagged fields if flexible
			if isFlexible {
				if err := structItem.writeTaggedFields(elemW, version); err != nil {
					return nil, err
				}
			}
			return elemBuf.Bytes(), nil
		}
		items := make([]interface{}, len(m.Coordinators))
		for i := range m.Coordinators {
			items[i] = m.Coordinators[i]
		}
		if isFlexible {
			if err := protocol.WriteCompactArray(w, items, encoder); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, items, encoder); err != nil {
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
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem FindCoordinatorResponseCoordinator
			elemR := bytes.NewReader(data)
			// Key
			if version >= 4 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Key = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Key = val
				}
			}
			// NodeId
			if version >= 4 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.NodeId = val
			}
			// Host
			if version >= 4 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Host = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Host = val
				}
			}
			// Port
			if version >= 4 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.Port = val
			}
			// ErrorCode
			if version >= 4 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ErrorCode = val
			}
			// ErrorMessage
			if version >= 4 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ErrorMessage = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ErrorMessage = val
				}
			}
			// Read tagged fields if flexible
			if isFlexible {
				if err := elem.readTaggedFields(elemR, version); err != nil {
					return nil, 0, err
				}
			}
			consumed := len(data) - elemR.Len()
			return elem, consumed, nil
		}
		if isFlexible {
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint < 1 {
				return errors.New("invalid compact array length")
			}
			length := int32(lengthUint - 1)
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem FindCoordinatorResponseCoordinator
				// Key
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Key = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Key = val
					}
				}
				// NodeId
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.NodeId = val
				}
				// Host
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					}
				}
				// Port
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.Port = val
				}
				// ErrorCode
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// ErrorMessage
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					}
				}
				// Key
				if version >= 4 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
							return err
						}
					}
				}
				// NodeId
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.NodeId); err != nil {
						return err
					}
				}
				// Host
				if version >= 4 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
							return err
						}
					}
				}
				// Port
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.Port); err != nil {
						return err
					}
				}
				// ErrorCode
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// ErrorMessage
				if version >= 4 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeCompactArray
			lengthBytes := protocol.EncodeVaruint32(lengthUint)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.Coordinators = make([]FindCoordinatorResponseCoordinator, len(decoded))
			for i, item := range decoded {
				m.Coordinators[i] = item.(FindCoordinatorResponseCoordinator)
			}
		} else {
			length, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem FindCoordinatorResponseCoordinator
				// Key
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Key = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Key = val
					}
				}
				// NodeId
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.NodeId = val
				}
				// Host
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					}
				}
				// Port
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.Port = val
				}
				// ErrorCode
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// ErrorMessage
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					}
				}
				// Key
				if version >= 4 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
							return err
						}
					}
				}
				// NodeId
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.NodeId); err != nil {
						return err
					}
				}
				// Host
				if version >= 4 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
							return err
						}
					}
				}
				// Port
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.Port); err != nil {
						return err
					}
				}
				// ErrorCode
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// ErrorMessage
				if version >= 4 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeArray
			lengthBytes := protocol.EncodeInt32(length)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.Coordinators = make([]FindCoordinatorResponseCoordinator, len(decoded))
			for i, item := range decoded {
				m.Coordinators[i] = item.(FindCoordinatorResponseCoordinator)
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
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for FindCoordinatorResponseCoordinator.
func (m *FindCoordinatorResponseCoordinator) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for FindCoordinatorResponseCoordinator.
func (m *FindCoordinatorResponseCoordinator) readTaggedFields(r io.Reader, version int16) error {
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
