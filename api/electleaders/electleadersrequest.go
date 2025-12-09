package electleaders

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ElectLeadersRequestApiKey        = 43
	ElectLeadersRequestHeaderVersion = 1
)

// ElectLeadersRequest represents a request message.
type ElectLeadersRequest struct {
	// Type of elections to conduct for the partition. A value of '0' elects the preferred replica. A value of '1' elects the first live replica if there are no in-sync replica.
	ElectionType int8 `json:"electiontype" versions:"1-999"`
	// The topic partitions to elect leaders.
	TopicPartitions []ElectLeadersRequestTopicPartitions `json:"topicpartitions" versions:"0-999"`
	// The time in ms to wait for the election to complete.
	TimeoutMs int32 `json:"timeoutms" versions:"0-999"`
}

// Encode encodes a ElectLeadersRequest to a byte slice for the given version.
func (m *ElectLeadersRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ElectLeadersRequest from a byte slice for the given version.
func (m *ElectLeadersRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ElectLeadersRequest to an io.Writer for the given version.
func (m *ElectLeadersRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ElectionType
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt8(w, m.ElectionType); err != nil {
			return err
		}
	}
	// TopicPartitions
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ElectLeadersRequestTopicPartitions)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Topic
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Topic); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Topic); err != nil {
						return nil, err
					}
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32Array(elemW, structItem.Partitions); err != nil {
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
		items := make([]interface{}, len(m.TopicPartitions))
		for i := range m.TopicPartitions {
			items[i] = m.TopicPartitions[i]
		}
		if m.TopicPartitions == nil {
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
	// TimeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TimeoutMs); err != nil {
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

// Read reads a ElectLeadersRequest from an io.Reader for the given version.
func (m *ElectLeadersRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ElectionType
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		m.ElectionType = val
	}
	// TopicPartitions
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ElectLeadersRequestTopicPartitions
			elemR := bytes.NewReader(data)
			// Topic
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Topic = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Topic = val
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactInt32Array(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Partitions = val
				} else {
					val, err := protocol.ReadInt32Array(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Partitions = val
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
				m.TopicPartitions = nil
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
				var tempElem ElectLeadersRequestTopicPartitions
				// Topic
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Topic = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Topic = val
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.Partitions = val
					} else {
						val, err := protocol.ReadInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.Partitions = val
					}
				}
				// Topic
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Topic); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Topic); err != nil {
							return err
						}
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32Array(elemW, tempElem.Partitions); err != nil {
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
			m.TopicPartitions = make([]ElectLeadersRequestTopicPartitions, len(decoded))
			for i, item := range decoded {
				m.TopicPartitions[i] = item.(ElectLeadersRequestTopicPartitions)
			}
		} else {
			length, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			if length == -1 {
				m.TopicPartitions = nil
				return nil
			}
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem ElectLeadersRequestTopicPartitions
				// Topic
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Topic = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Topic = val
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.Partitions = val
					} else {
						val, err := protocol.ReadInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.Partitions = val
					}
				}
				// Topic
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Topic); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Topic); err != nil {
							return err
						}
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32Array(elemW, tempElem.Partitions); err != nil {
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
			m.TopicPartitions = make([]ElectLeadersRequestTopicPartitions, len(decoded))
			for i, item := range decoded {
				m.TopicPartitions[i] = item.(ElectLeadersRequestTopicPartitions)
			}
		}
	}
	// TimeoutMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.TimeoutMs = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// ElectLeadersRequestTopicPartitions represents The topic partitions to elect leaders..
type ElectLeadersRequestTopicPartitions struct {
	// The name of a topic.
	Topic string `json:"topic" versions:"0-999"`
	// The partitions of this topic whose leader should be elected.
	Partitions []int32 `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ElectLeadersRequestTopicPartitions.
func (m *ElectLeadersRequestTopicPartitions) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ElectLeadersRequestTopicPartitions.
func (m *ElectLeadersRequestTopicPartitions) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for ElectLeadersRequest.
func (m *ElectLeadersRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ElectLeadersRequest.
func (m *ElectLeadersRequest) readTaggedFields(r io.Reader, version int16) error {
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
