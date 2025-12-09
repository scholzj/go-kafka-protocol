package listoffsets

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ListOffsetsRequestApiKey        = 2
	ListOffsetsRequestHeaderVersion = 1
)

// ListOffsetsRequest represents a request message.
type ListOffsetsRequest struct {
	// The broker ID of the requester, or -1 if this request is being made by a normal consumer.
	ReplicaId int32 `json:"replicaid" versions:"0-999"`
	// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
	IsolationLevel int8 `json:"isolationlevel" versions:"2-999"`
	// Each topic in the request.
	Topics []ListOffsetsRequestListOffsetsTopic `json:"topics" versions:"0-999"`
	// The timeout to await a response in milliseconds for requests that require reading from remote storage for topics enabled with tiered storage.
	TimeoutMs int32 `json:"timeoutms" versions:"10-999"`
}

// Encode encodes a ListOffsetsRequest to a byte slice for the given version.
func (m *ListOffsetsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ListOffsetsRequest from a byte slice for the given version.
func (m *ListOffsetsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ListOffsetsRequest to an io.Writer for the given version.
func (m *ListOffsetsRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 11 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// ReplicaId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ReplicaId); err != nil {
			return err
		}
	}
	// IsolationLevel
	if version >= 2 && version <= 999 {
		if err := protocol.WriteInt8(w, m.IsolationLevel); err != nil {
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
			structItem, ok := item.(ListOffsetsRequestListOffsetsTopic)
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
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].PartitionIndex); err != nil {
							return nil, err
						}
					}
					// CurrentLeaderEpoch
					if version >= 4 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].CurrentLeaderEpoch); err != nil {
							return nil, err
						}
					}
					// Timestamp
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(elemW, structItem.Partitions[i].Timestamp); err != nil {
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
	// TimeoutMs
	if version >= 10 && version <= 999 {
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

// Read reads a ListOffsetsRequest from an io.Reader for the given version.
func (m *ListOffsetsRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 11 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// ReplicaId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ReplicaId = val
	}
	// IsolationLevel
	if version >= 2 && version <= 999 {
		val, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		m.IsolationLevel = val
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ListOffsetsRequestListOffsetsTopic
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
				var tempElem ListOffsetsRequestListOffsetsTopic
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
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem ListOffsetsRequestListOffsetsPartition
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// CurrentLeaderEpoch
						if version >= 4 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.CurrentLeaderEpoch = val
						}
						// Timestamp
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Timestamp = val
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
							var tempElem ListOffsetsRequestListOffsetsPartition
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// CurrentLeaderEpoch
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CurrentLeaderEpoch = val
							}
							// Timestamp
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.Timestamp = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// CurrentLeaderEpoch
							if version >= 4 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.CurrentLeaderEpoch); err != nil {
									return err
								}
							}
							// Timestamp
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.Timestamp); err != nil {
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
						tempElem.Partitions = make([]ListOffsetsRequestListOffsetsPartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ListOffsetsRequestListOffsetsPartition)
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
							var tempElem ListOffsetsRequestListOffsetsPartition
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// CurrentLeaderEpoch
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CurrentLeaderEpoch = val
							}
							// Timestamp
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.Timestamp = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// CurrentLeaderEpoch
							if version >= 4 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.CurrentLeaderEpoch); err != nil {
									return err
								}
							}
							// Timestamp
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.Timestamp); err != nil {
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
						tempElem.Partitions = make([]ListOffsetsRequestListOffsetsPartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ListOffsetsRequestListOffsetsPartition)
						}
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
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
							}
						}
						// CurrentLeaderEpoch
						if version >= 4 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CurrentLeaderEpoch); err != nil {
								return err
							}
						}
						// Timestamp
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Timestamp); err != nil {
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
			m.Topics = make([]ListOffsetsRequestListOffsetsTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(ListOffsetsRequestListOffsetsTopic)
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
				var tempElem ListOffsetsRequestListOffsetsTopic
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
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem ListOffsetsRequestListOffsetsPartition
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// CurrentLeaderEpoch
						if version >= 4 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.CurrentLeaderEpoch = val
						}
						// Timestamp
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Timestamp = val
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
							var tempElem ListOffsetsRequestListOffsetsPartition
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// CurrentLeaderEpoch
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CurrentLeaderEpoch = val
							}
							// Timestamp
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.Timestamp = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// CurrentLeaderEpoch
							if version >= 4 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.CurrentLeaderEpoch); err != nil {
									return err
								}
							}
							// Timestamp
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.Timestamp); err != nil {
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
						tempElem.Partitions = make([]ListOffsetsRequestListOffsetsPartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ListOffsetsRequestListOffsetsPartition)
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
							var tempElem ListOffsetsRequestListOffsetsPartition
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// CurrentLeaderEpoch
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CurrentLeaderEpoch = val
							}
							// Timestamp
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.Timestamp = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// CurrentLeaderEpoch
							if version >= 4 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.CurrentLeaderEpoch); err != nil {
									return err
								}
							}
							// Timestamp
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.Timestamp); err != nil {
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
						tempElem.Partitions = make([]ListOffsetsRequestListOffsetsPartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ListOffsetsRequestListOffsetsPartition)
						}
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
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
							}
						}
						// CurrentLeaderEpoch
						if version >= 4 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CurrentLeaderEpoch); err != nil {
								return err
							}
						}
						// Timestamp
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Timestamp); err != nil {
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
			m.Topics = make([]ListOffsetsRequestListOffsetsTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(ListOffsetsRequestListOffsetsTopic)
			}
		}
	}
	// TimeoutMs
	if version >= 10 && version <= 999 {
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

// ListOffsetsRequestListOffsetsTopic represents Each topic in the request..
type ListOffsetsRequestListOffsetsTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// Each partition in the request.
	Partitions []ListOffsetsRequestListOffsetsPartition `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ListOffsetsRequestListOffsetsTopic.
func (m *ListOffsetsRequestListOffsetsTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ListOffsetsRequestListOffsetsTopic.
func (m *ListOffsetsRequestListOffsetsTopic) readTaggedFields(r io.Reader, version int16) error {
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

// ListOffsetsRequestListOffsetsPartition represents Each partition in the request..
type ListOffsetsRequestListOffsetsPartition struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The current leader epoch.
	CurrentLeaderEpoch int32 `json:"currentleaderepoch" versions:"4-999"`
	// The current timestamp.
	Timestamp int64 `json:"timestamp" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ListOffsetsRequestListOffsetsPartition.
func (m *ListOffsetsRequestListOffsetsPartition) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ListOffsetsRequestListOffsetsPartition.
func (m *ListOffsetsRequestListOffsetsPartition) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for ListOffsetsRequest.
func (m *ListOffsetsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ListOffsetsRequest.
func (m *ListOffsetsRequest) readTaggedFields(r io.Reader, version int16) error {
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
