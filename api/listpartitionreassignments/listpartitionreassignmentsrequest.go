package listpartitionreassignments

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ListPartitionReassignmentsRequestApiKey        = 46
	ListPartitionReassignmentsRequestHeaderVersion = 1
)

// ListPartitionReassignmentsRequest represents a request message.
type ListPartitionReassignmentsRequest struct {
	// The time in ms to wait for the request to complete.
	TimeoutMs int32 `json:"timeoutms" versions:"0-999"`
	// The topics to list partition reassignments for, or null to list everything.
	Topics []ListPartitionReassignmentsRequestListPartitionReassignmentsTopics `json:"topics" versions:"0-999"`
}

// Encode encodes a ListPartitionReassignmentsRequest to a byte slice for the given version.
func (m *ListPartitionReassignmentsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ListPartitionReassignmentsRequest from a byte slice for the given version.
func (m *ListPartitionReassignmentsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ListPartitionReassignmentsRequest to an io.Writer for the given version.
func (m *ListPartitionReassignmentsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// TimeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TimeoutMs); err != nil {
			return err
		}
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ListPartitionReassignmentsRequestListPartitionReassignmentsTopics)
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
			// PartitionIndexes
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactInt32Array(elemW, structItem.PartitionIndexes); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32Array(elemW, structItem.PartitionIndexes); err != nil {
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
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a ListPartitionReassignmentsRequest from an io.Reader for the given version.
func (m *ListPartitionReassignmentsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// TimeoutMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.TimeoutMs = val
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ListPartitionReassignmentsRequestListPartitionReassignmentsTopics
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
			// PartitionIndexes
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactInt32Array(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.PartitionIndexes = val
				} else {
					val, err := protocol.ReadInt32Array(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.PartitionIndexes = val
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
				var tempElem ListPartitionReassignmentsRequestListPartitionReassignmentsTopics
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
				// PartitionIndexes
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.PartitionIndexes = val
					} else {
						val, err := protocol.ReadInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.PartitionIndexes = val
					}
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
				// PartitionIndexes
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactInt32Array(elemW, tempElem.PartitionIndexes); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32Array(elemW, tempElem.PartitionIndexes); err != nil {
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
			m.Topics = make([]ListPartitionReassignmentsRequestListPartitionReassignmentsTopics, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(ListPartitionReassignmentsRequestListPartitionReassignmentsTopics)
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
				var tempElem ListPartitionReassignmentsRequestListPartitionReassignmentsTopics
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
				// PartitionIndexes
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.PartitionIndexes = val
					} else {
						val, err := protocol.ReadInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.PartitionIndexes = val
					}
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
				// PartitionIndexes
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactInt32Array(elemW, tempElem.PartitionIndexes); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32Array(elemW, tempElem.PartitionIndexes); err != nil {
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
			m.Topics = make([]ListPartitionReassignmentsRequestListPartitionReassignmentsTopics, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(ListPartitionReassignmentsRequestListPartitionReassignmentsTopics)
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

// ListPartitionReassignmentsRequestListPartitionReassignmentsTopics represents The topics to list partition reassignments for, or null to list everything..
type ListPartitionReassignmentsRequestListPartitionReassignmentsTopics struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The partitions to list partition reassignments for.
	PartitionIndexes []int32 `json:"partitionindexes" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ListPartitionReassignmentsRequestListPartitionReassignmentsTopics.
func (m *ListPartitionReassignmentsRequestListPartitionReassignmentsTopics) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ListPartitionReassignmentsRequestListPartitionReassignmentsTopics.
func (m *ListPartitionReassignmentsRequestListPartitionReassignmentsTopics) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for ListPartitionReassignmentsRequest.
func (m *ListPartitionReassignmentsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ListPartitionReassignmentsRequest.
func (m *ListPartitionReassignmentsRequest) readTaggedFields(r io.Reader, version int16) error {
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
