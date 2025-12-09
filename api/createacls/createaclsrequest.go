package createacls

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	CreateAclsRequestApiKey        = 30
	CreateAclsRequestHeaderVersion = 1
)

// CreateAclsRequest represents a request message.
type CreateAclsRequest struct {
	// The ACLs that we want to create.
	Creations []CreateAclsRequestAclCreation `json:"creations" versions:"0-999"`
}

// Encode encodes a CreateAclsRequest to a byte slice for the given version.
func (m *CreateAclsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a CreateAclsRequest from a byte slice for the given version.
func (m *CreateAclsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a CreateAclsRequest to an io.Writer for the given version.
func (m *CreateAclsRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Creations
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(CreateAclsRequestAclCreation)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// ResourceType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.ResourceType); err != nil {
					return nil, err
				}
			}
			// ResourceName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.ResourceName); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.ResourceName); err != nil {
						return nil, err
					}
				}
			}
			// ResourcePatternType
			if version >= 1 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.ResourcePatternType); err != nil {
					return nil, err
				}
			}
			// Principal
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Principal); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Principal); err != nil {
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
			// Operation
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.Operation); err != nil {
					return nil, err
				}
			}
			// PermissionType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.PermissionType); err != nil {
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
		items := make([]interface{}, len(m.Creations))
		for i := range m.Creations {
			items[i] = m.Creations[i]
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

// Read reads a CreateAclsRequest from an io.Reader for the given version.
func (m *CreateAclsRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Creations
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem CreateAclsRequestAclCreation
			elemR := bytes.NewReader(data)
			// ResourceType
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt8(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ResourceType = val
			}
			// ResourceName
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ResourceName = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ResourceName = val
				}
			}
			// ResourcePatternType
			if version >= 1 && version <= 999 {
				val, err := protocol.ReadInt8(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ResourcePatternType = val
			}
			// Principal
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Principal = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Principal = val
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
			// Operation
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt8(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.Operation = val
			}
			// PermissionType
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt8(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.PermissionType = val
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
				var tempElem CreateAclsRequestAclCreation
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceName = val
					}
				}
				// ResourcePatternType
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.ResourcePatternType = val
				}
				// Principal
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Principal = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Principal = val
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
				// Operation
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.Operation = val
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.PermissionType = val
				}
				// ResourceType
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.ResourceType); err != nil {
						return err
					}
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.ResourceName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.ResourceName); err != nil {
							return err
						}
					}
				}
				// ResourcePatternType
				if version >= 1 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.ResourcePatternType); err != nil {
						return err
					}
				}
				// Principal
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Principal); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Principal); err != nil {
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
				// Operation
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.Operation); err != nil {
						return err
					}
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.PermissionType); err != nil {
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
			m.Creations = make([]CreateAclsRequestAclCreation, len(decoded))
			for i, item := range decoded {
				m.Creations[i] = item.(CreateAclsRequestAclCreation)
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
				var tempElem CreateAclsRequestAclCreation
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceName = val
					}
				}
				// ResourcePatternType
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.ResourcePatternType = val
				}
				// Principal
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Principal = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Principal = val
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
				// Operation
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.Operation = val
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.PermissionType = val
				}
				// ResourceType
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.ResourceType); err != nil {
						return err
					}
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.ResourceName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.ResourceName); err != nil {
							return err
						}
					}
				}
				// ResourcePatternType
				if version >= 1 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.ResourcePatternType); err != nil {
						return err
					}
				}
				// Principal
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Principal); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Principal); err != nil {
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
				// Operation
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.Operation); err != nil {
						return err
					}
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.PermissionType); err != nil {
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
			m.Creations = make([]CreateAclsRequestAclCreation, len(decoded))
			for i, item := range decoded {
				m.Creations[i] = item.(CreateAclsRequestAclCreation)
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

// CreateAclsRequestAclCreation represents The ACLs that we want to create..
type CreateAclsRequestAclCreation struct {
	// The type of the resource.
	ResourceType int8 `json:"resourcetype" versions:"0-999"`
	// The resource name for the ACL.
	ResourceName string `json:"resourcename" versions:"0-999"`
	// The pattern type for the ACL.
	ResourcePatternType int8 `json:"resourcepatterntype" versions:"1-999"`
	// The principal for the ACL.
	Principal string `json:"principal" versions:"0-999"`
	// The host for the ACL.
	Host string `json:"host" versions:"0-999"`
	// The operation type for the ACL (read, write, etc.).
	Operation int8 `json:"operation" versions:"0-999"`
	// The permission type for the ACL (allow, deny, etc.).
	PermissionType int8 `json:"permissiontype" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for CreateAclsRequestAclCreation.
func (m *CreateAclsRequestAclCreation) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreateAclsRequestAclCreation.
func (m *CreateAclsRequestAclCreation) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for CreateAclsRequest.
func (m *CreateAclsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreateAclsRequest.
func (m *CreateAclsRequest) readTaggedFields(r io.Reader, version int16) error {
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
