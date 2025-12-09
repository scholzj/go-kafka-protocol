package describeconfigs

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeConfigsRequestApiKey        = 32
	DescribeConfigsRequestHeaderVersion = 1
)

// DescribeConfigsRequest represents a request message.
type DescribeConfigsRequest struct {
	// The resources whose configurations we want to describe.
	Resources []DescribeConfigsRequestDescribeConfigsResource `json:"resources" versions:"0-999"`
	// True if we should include all synonyms.
	IncludeSynonyms bool `json:"includesynonyms" versions:"1-999"`
	// True if we should include configuration documentation.
	IncludeDocumentation bool `json:"includedocumentation" versions:"3-999"`
}

// Encode encodes a DescribeConfigsRequest to a byte slice for the given version.
func (m *DescribeConfigsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeConfigsRequest from a byte slice for the given version.
func (m *DescribeConfigsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeConfigsRequest to an io.Writer for the given version.
func (m *DescribeConfigsRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// Resources
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DescribeConfigsRequestDescribeConfigsResource)
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
			// ConfigurationKeys
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactStringArray(elemW, structItem.ConfigurationKeys); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteStringArray(elemW, structItem.ConfigurationKeys); err != nil {
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
		items := make([]interface{}, len(m.Resources))
		for i := range m.Resources {
			items[i] = m.Resources[i]
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
	// IncludeSynonyms
	if version >= 1 && version <= 999 {
		if err := protocol.WriteBool(w, m.IncludeSynonyms); err != nil {
			return err
		}
	}
	// IncludeDocumentation
	if version >= 3 && version <= 999 {
		if err := protocol.WriteBool(w, m.IncludeDocumentation); err != nil {
			return err
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

// Read reads a DescribeConfigsRequest from an io.Reader for the given version.
func (m *DescribeConfigsRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// Resources
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DescribeConfigsRequestDescribeConfigsResource
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
			// ConfigurationKeys
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactStringArray(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ConfigurationKeys = val
				} else {
					val, err := protocol.ReadStringArray(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ConfigurationKeys = val
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
				var tempElem DescribeConfigsRequestDescribeConfigsResource
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
				// ConfigurationKeys
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactStringArray(r)
						if err != nil {
							return err
						}
						tempElem.ConfigurationKeys = val
					} else {
						val, err := protocol.ReadStringArray(r)
						if err != nil {
							return err
						}
						tempElem.ConfigurationKeys = val
					}
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
				// ConfigurationKeys
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactStringArray(elemW, tempElem.ConfigurationKeys); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteStringArray(elemW, tempElem.ConfigurationKeys); err != nil {
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
			m.Resources = make([]DescribeConfigsRequestDescribeConfigsResource, len(decoded))
			for i, item := range decoded {
				m.Resources[i] = item.(DescribeConfigsRequestDescribeConfigsResource)
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
				var tempElem DescribeConfigsRequestDescribeConfigsResource
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
				// ConfigurationKeys
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactStringArray(r)
						if err != nil {
							return err
						}
						tempElem.ConfigurationKeys = val
					} else {
						val, err := protocol.ReadStringArray(r)
						if err != nil {
							return err
						}
						tempElem.ConfigurationKeys = val
					}
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
				// ConfigurationKeys
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactStringArray(elemW, tempElem.ConfigurationKeys); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteStringArray(elemW, tempElem.ConfigurationKeys); err != nil {
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
			m.Resources = make([]DescribeConfigsRequestDescribeConfigsResource, len(decoded))
			for i, item := range decoded {
				m.Resources[i] = item.(DescribeConfigsRequestDescribeConfigsResource)
			}
		}
	}
	// IncludeSynonyms
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IncludeSynonyms = val
	}
	// IncludeDocumentation
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IncludeDocumentation = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// DescribeConfigsRequestDescribeConfigsResource represents The resources whose configurations we want to describe..
type DescribeConfigsRequestDescribeConfigsResource struct {
	// The resource type.
	ResourceType int8 `json:"resourcetype" versions:"0-999"`
	// The resource name.
	ResourceName string `json:"resourcename" versions:"0-999"`
	// The configuration keys to list, or null to list all configuration keys.
	ConfigurationKeys []string `json:"configurationkeys" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeConfigsRequestDescribeConfigsResource.
func (m *DescribeConfigsRequestDescribeConfigsResource) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeConfigsRequestDescribeConfigsResource.
func (m *DescribeConfigsRequestDescribeConfigsResource) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for DescribeConfigsRequest.
func (m *DescribeConfigsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeConfigsRequest.
func (m *DescribeConfigsRequest) readTaggedFields(r io.Reader, version int16) error {
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
