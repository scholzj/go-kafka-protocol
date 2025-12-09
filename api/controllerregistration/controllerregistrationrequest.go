package controllerregistration

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ControllerRegistrationRequestApiKey        = 70
	ControllerRegistrationRequestHeaderVersion = 1
)

// ControllerRegistrationRequest represents a request message.
type ControllerRegistrationRequest struct {
	// The ID of the controller to register.
	ControllerId int32 `json:"controllerid" versions:"0-999"`
	// The controller incarnation ID, which is unique to each process run.
	IncarnationId uuid.UUID `json:"incarnationid" versions:"0-999"`
	// Set if the required configurations for ZK migration are present.
	ZkMigrationReady bool `json:"zkmigrationready" versions:"0-999"`
	// The listeners of this controller.
	Listeners []ControllerRegistrationRequestListener `json:"listeners" versions:"0-999"`
	// The features on this controller.
	Features []ControllerRegistrationRequestFeature `json:"features" versions:"0-999"`
}

// Encode encodes a ControllerRegistrationRequest to a byte slice for the given version.
func (m *ControllerRegistrationRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ControllerRegistrationRequest from a byte slice for the given version.
func (m *ControllerRegistrationRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ControllerRegistrationRequest to an io.Writer for the given version.
func (m *ControllerRegistrationRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ControllerId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ControllerId); err != nil {
			return err
		}
	}
	// IncarnationId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteUUID(w, m.IncarnationId); err != nil {
			return err
		}
	}
	// ZkMigrationReady
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.ZkMigrationReady); err != nil {
			return err
		}
	}
	// Listeners
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ControllerRegistrationRequestListener)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				}
			}
			// Host
			if version >= 0 && version <= 999 {
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
			if version >= 0 && version <= 999 {
			}
			// SecurityProtocol
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.SecurityProtocol); err != nil {
					return nil, err
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
		items := make([]interface{}, len(m.Listeners))
		for i := range m.Listeners {
			items[i] = m.Listeners[i]
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
	// Features
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ControllerRegistrationRequestFeature)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				}
			}
			// MinSupportedVersion
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.MinSupportedVersion); err != nil {
					return nil, err
				}
			}
			// MaxSupportedVersion
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.MaxSupportedVersion); err != nil {
					return nil, err
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
		items := make([]interface{}, len(m.Features))
		for i := range m.Features {
			items[i] = m.Features[i]
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

// Read reads a ControllerRegistrationRequest from an io.Reader for the given version.
func (m *ControllerRegistrationRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ControllerId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ControllerId = val
	}
	// IncarnationId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadUUID(r)
		if err != nil {
			return err
		}
		m.IncarnationId = val
	}
	// ZkMigrationReady
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.ZkMigrationReady = val
	}
	// Listeners
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ControllerRegistrationRequestListener
			elemR := bytes.NewReader(data)
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				}
			}
			// Host
			if version >= 0 && version <= 999 {
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
			if version >= 0 && version <= 999 {
			}
			// SecurityProtocol
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.SecurityProtocol = val
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
				var tempElem ControllerRegistrationRequestListener
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// Host
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
				}
				// SecurityProtocol
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.SecurityProtocol = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// Host
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
				}
				// SecurityProtocol
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.SecurityProtocol); err != nil {
						return err
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
			m.Listeners = make([]ControllerRegistrationRequestListener, len(decoded))
			for i, item := range decoded {
				m.Listeners[i] = item.(ControllerRegistrationRequestListener)
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
				var tempElem ControllerRegistrationRequestListener
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// Host
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
				}
				// SecurityProtocol
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.SecurityProtocol = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// Host
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
				}
				// SecurityProtocol
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.SecurityProtocol); err != nil {
						return err
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
			m.Listeners = make([]ControllerRegistrationRequestListener, len(decoded))
			for i, item := range decoded {
				m.Listeners[i] = item.(ControllerRegistrationRequestListener)
			}
		}
	}
	// Features
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ControllerRegistrationRequestFeature
			elemR := bytes.NewReader(data)
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				}
			}
			// MinSupportedVersion
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.MinSupportedVersion = val
			}
			// MaxSupportedVersion
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.MaxSupportedVersion = val
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
				var tempElem ControllerRegistrationRequestFeature
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// MinSupportedVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.MinSupportedVersion = val
				}
				// MaxSupportedVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.MaxSupportedVersion = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// MinSupportedVersion
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.MinSupportedVersion); err != nil {
						return err
					}
				}
				// MaxSupportedVersion
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.MaxSupportedVersion); err != nil {
						return err
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
			m.Features = make([]ControllerRegistrationRequestFeature, len(decoded))
			for i, item := range decoded {
				m.Features[i] = item.(ControllerRegistrationRequestFeature)
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
				var tempElem ControllerRegistrationRequestFeature
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// MinSupportedVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.MinSupportedVersion = val
				}
				// MaxSupportedVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.MaxSupportedVersion = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// MinSupportedVersion
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.MinSupportedVersion); err != nil {
						return err
					}
				}
				// MaxSupportedVersion
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.MaxSupportedVersion); err != nil {
						return err
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
			m.Features = make([]ControllerRegistrationRequestFeature, len(decoded))
			for i, item := range decoded {
				m.Features[i] = item.(ControllerRegistrationRequestFeature)
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

// ControllerRegistrationRequestListener represents The listeners of this controller..
type ControllerRegistrationRequestListener struct {
	// The name of the endpoint.
	Name string `json:"name" versions:"0-999"`
	// The hostname.
	Host string `json:"host" versions:"0-999"`
	// The port.
	Port uint16 `json:"port" versions:"0-999"`
	// The security protocol.
	SecurityProtocol int16 `json:"securityprotocol" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ControllerRegistrationRequestListener.
func (m *ControllerRegistrationRequestListener) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ControllerRegistrationRequestListener.
func (m *ControllerRegistrationRequestListener) readTaggedFields(r io.Reader, version int16) error {
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

// ControllerRegistrationRequestFeature represents The features on this controller..
type ControllerRegistrationRequestFeature struct {
	// The feature name.
	Name string `json:"name" versions:"0-999"`
	// The minimum supported feature level.
	MinSupportedVersion int16 `json:"minsupportedversion" versions:"0-999"`
	// The maximum supported feature level.
	MaxSupportedVersion int16 `json:"maxsupportedversion" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ControllerRegistrationRequestFeature.
func (m *ControllerRegistrationRequestFeature) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ControllerRegistrationRequestFeature.
func (m *ControllerRegistrationRequestFeature) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for ControllerRegistrationRequest.
func (m *ControllerRegistrationRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ControllerRegistrationRequest.
func (m *ControllerRegistrationRequest) readTaggedFields(r io.Reader, version int16) error {
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
