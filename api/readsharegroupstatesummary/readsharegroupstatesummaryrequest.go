package readsharegroupstatesummary

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ReadShareGroupStateSummaryRequestApiKey        = 87
	ReadShareGroupStateSummaryRequestHeaderVersion = 1
)

// ReadShareGroupStateSummaryRequest represents a request message.
type ReadShareGroupStateSummaryRequest struct {
	// The group identifier.
	GroupId string `json:"groupid" versions:"0-999"`
	// The data for the topics.
	Topics []ReadShareGroupStateSummaryRequestReadStateSummaryData `json:"topics" versions:"0-999"`
}

// Encode encodes a ReadShareGroupStateSummaryRequest to a byte slice for the given version.
func (m *ReadShareGroupStateSummaryRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ReadShareGroupStateSummaryRequest from a byte slice for the given version.
func (m *ReadShareGroupStateSummaryRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ReadShareGroupStateSummaryRequest to an io.Writer for the given version.
func (m *ReadShareGroupStateSummaryRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.GroupId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.GroupId); err != nil {
				return err
			}
		}
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ReadShareGroupStateSummaryRequestReadStateSummaryData)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// TopicId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteUUID(elemW, structItem.TopicId); err != nil {
					return nil, err
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Partitions) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Partitions {
					// Partition
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].Partition); err != nil {
							return nil, err
						}
					}
					// LeaderEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].LeaderEpoch); err != nil {
							return nil, err
						}
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

// Read reads a ReadShareGroupStateSummaryRequest from an io.Reader for the given version.
func (m *ReadShareGroupStateSummaryRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		}
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ReadShareGroupStateSummaryRequestReadStateSummaryData
			elemR := bytes.NewReader(data)
			// TopicId
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadUUID(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.TopicId = val
			}
			// Partitions
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
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
				var tempElem ReadShareGroupStateSummaryRequestReadStateSummaryData
				// TopicId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.TopicId = val
				}
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem ReadShareGroupStateSummaryRequestPartitionData
						elemR := bytes.NewReader(data)
						// Partition
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Partition = val
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderEpoch = val
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
							var tempElem ReadShareGroupStateSummaryRequestPartitionData
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
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
						tempElem.Partitions = make([]ReadShareGroupStateSummaryRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ReadShareGroupStateSummaryRequestPartitionData)
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
							var tempElem ReadShareGroupStateSummaryRequestPartitionData
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
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
						tempElem.Partitions = make([]ReadShareGroupStateSummaryRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ReadShareGroupStateSummaryRequestPartitionData)
						}
					}
				}
				// TopicId
				if version >= 0 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
						return err
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Partitions) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions))); err != nil {
							return err
						}
					}
					for i := range tempElem.Partitions {
						// Partition
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].Partition); err != nil {
								return err
							}
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
								return err
							}
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
			m.Topics = make([]ReadShareGroupStateSummaryRequestReadStateSummaryData, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(ReadShareGroupStateSummaryRequestReadStateSummaryData)
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
				var tempElem ReadShareGroupStateSummaryRequestReadStateSummaryData
				// TopicId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.TopicId = val
				}
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem ReadShareGroupStateSummaryRequestPartitionData
						elemR := bytes.NewReader(data)
						// Partition
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Partition = val
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderEpoch = val
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
							var tempElem ReadShareGroupStateSummaryRequestPartitionData
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
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
						tempElem.Partitions = make([]ReadShareGroupStateSummaryRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ReadShareGroupStateSummaryRequestPartitionData)
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
							var tempElem ReadShareGroupStateSummaryRequestPartitionData
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
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
						tempElem.Partitions = make([]ReadShareGroupStateSummaryRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ReadShareGroupStateSummaryRequestPartitionData)
						}
					}
				}
				// TopicId
				if version >= 0 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
						return err
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Partitions) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions))); err != nil {
							return err
						}
					}
					for i := range tempElem.Partitions {
						// Partition
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].Partition); err != nil {
								return err
							}
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
								return err
							}
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
			m.Topics = make([]ReadShareGroupStateSummaryRequestReadStateSummaryData, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(ReadShareGroupStateSummaryRequestReadStateSummaryData)
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

// ReadShareGroupStateSummaryRequestReadStateSummaryData represents The data for the topics..
type ReadShareGroupStateSummaryRequestReadStateSummaryData struct {
	// The topic identifier.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// The data for the partitions.
	Partitions []ReadShareGroupStateSummaryRequestPartitionData `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ReadShareGroupStateSummaryRequestReadStateSummaryData.
func (m *ReadShareGroupStateSummaryRequestReadStateSummaryData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ReadShareGroupStateSummaryRequestReadStateSummaryData.
func (m *ReadShareGroupStateSummaryRequestReadStateSummaryData) readTaggedFields(r io.Reader, version int16) error {
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

// ReadShareGroupStateSummaryRequestPartitionData represents The data for the partitions..
type ReadShareGroupStateSummaryRequestPartitionData struct {
	// The partition index.
	Partition int32 `json:"partition" versions:"0-999"`
	// The leader epoch of the share-partition.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ReadShareGroupStateSummaryRequestPartitionData.
func (m *ReadShareGroupStateSummaryRequestPartitionData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ReadShareGroupStateSummaryRequestPartitionData.
func (m *ReadShareGroupStateSummaryRequestPartitionData) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for ReadShareGroupStateSummaryRequest.
func (m *ReadShareGroupStateSummaryRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ReadShareGroupStateSummaryRequest.
func (m *ReadShareGroupStateSummaryRequest) readTaggedFields(r io.Reader, version int16) error {
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
