package metadata

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	MetadataRequestApiKey        = 3
	MetadataRequestHeaderVersion = 1
)

// MetadataRequest represents a request message.
type MetadataRequest struct {
	// The topics to fetch metadata for.
	Topics []MetadataRequestMetadataRequestTopic `json:"topics" versions:"0-999"`
	// If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
	AllowAutoTopicCreation bool `json:"allowautotopiccreation" versions:"4-999"`
	// Whether to include cluster authorized operations.
	IncludeClusterAuthorizedOperations bool `json:"includeclusterauthorizedoperations" versions:"8-10"`
	// Whether to include topic authorized operations.
	IncludeTopicAuthorizedOperations bool `json:"includetopicauthorizedoperations" versions:"8-999"`
}

// Encode encodes a MetadataRequest to a byte slice for the given version.
func (m *MetadataRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a MetadataRequest from a byte slice for the given version.
func (m *MetadataRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a MetadataRequest to an io.Writer for the given version.
func (m *MetadataRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// Topics
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(MetadataRequestMetadataRequestTopic)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// TopicId
			if version >= 10 && version <= 999 {
				if err := protocol.WriteUUID(elemW, structItem.TopicId); err != nil {
					return nil, err
				}
			}
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.Name); err != nil {
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
		items := make([]interface{}, len(m.Topics))
		for i := range m.Topics {
			items[i] = m.Topics[i]
		}
		if m.Topics == nil {
			if isFlexible {
				if err := protocol.WriteVaruint32(w, 0); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteInt32(w, -1); err != nil {
					return err
				}
			}
		} else {
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
	}
	// AllowAutoTopicCreation
	if version >= 4 && version <= 999 {
		if err := protocol.WriteBool(w, m.AllowAutoTopicCreation); err != nil {
			return err
		}
	}
	// IncludeClusterAuthorizedOperations
	if version >= 8 && version <= 10 {
		if err := protocol.WriteBool(w, m.IncludeClusterAuthorizedOperations); err != nil {
			return err
		}
	}
	// IncludeTopicAuthorizedOperations
	if version >= 8 && version <= 999 {
		if err := protocol.WriteBool(w, m.IncludeTopicAuthorizedOperations); err != nil {
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

// Read reads a MetadataRequest from an io.Reader for the given version.
func (m *MetadataRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem MetadataRequestMetadataRequestTopic
			elemR := bytes.NewReader(data)
			// TopicId
			if version >= 10 && version <= 999 {
				val, err := protocol.ReadUUID(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.TopicId = val
			}
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
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
			if lengthUint == 0 {
				m.Topics = nil
				return nil
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
				var tempElem MetadataRequestMetadataRequestTopic
				// TopicId
				if version >= 10 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.TopicId = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// TopicId
				if version >= 10 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
						return err
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.Name); err != nil {
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
			m.Topics = make([]MetadataRequestMetadataRequestTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(MetadataRequestMetadataRequestTopic)
			}
		} else {
			length, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			if length == -1 {
				m.Topics = nil
				return nil
			}
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem MetadataRequestMetadataRequestTopic
				// TopicId
				if version >= 10 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.TopicId = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// TopicId
				if version >= 10 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
						return err
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.Name); err != nil {
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
			m.Topics = make([]MetadataRequestMetadataRequestTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(MetadataRequestMetadataRequestTopic)
			}
		}
	}
	// AllowAutoTopicCreation
	if version >= 4 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.AllowAutoTopicCreation = val
	}
	// IncludeClusterAuthorizedOperations
	if version >= 8 && version <= 10 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IncludeClusterAuthorizedOperations = val
	}
	// IncludeTopicAuthorizedOperations
	if version >= 8 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IncludeTopicAuthorizedOperations = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// MetadataRequestMetadataRequestTopic represents The topics to fetch metadata for..
type MetadataRequestMetadataRequestTopic struct {
	// The topic id.
	TopicId uuid.UUID `json:"topicid" versions:"10-999"`
	// The topic name.
	Name *string `json:"name" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for MetadataRequestMetadataRequestTopic.
func (m *MetadataRequestMetadataRequestTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for MetadataRequestMetadataRequestTopic.
func (m *MetadataRequestMetadataRequestTopic) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for MetadataRequest.
func (m *MetadataRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for MetadataRequest.
func (m *MetadataRequest) readTaggedFields(r io.Reader, version int16) error {
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
