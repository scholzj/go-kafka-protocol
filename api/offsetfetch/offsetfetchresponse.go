package offsetfetch

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	OffsetFetchResponseApiKey        = 9
	OffsetFetchResponseHeaderVersion = 1
)

// OffsetFetchResponse represents a response message.
type OffsetFetchResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"3-999"`
	// The responses per topic.
	Topics []OffsetFetchResponseOffsetFetchResponseTopic `json:"topics" versions:"0-7"`
	// The top-level error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"2-7"`
	// The responses per group id.
	Groups []OffsetFetchResponseOffsetFetchResponseGroup `json:"groups" versions:"8-999"`
}

// Encode encodes a OffsetFetchResponse to a byte slice for the given version.
func (m *OffsetFetchResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a OffsetFetchResponse from a byte slice for the given version.
func (m *OffsetFetchResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a OffsetFetchResponse to an io.Writer for the given version.
func (m *OffsetFetchResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 10 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 3 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// Topics
	if version >= 0 && version <= 7 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(OffsetFetchResponseOffsetFetchResponseTopic)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Name
			if version >= 0 && version <= 7 {
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
			if version >= 0 && version <= 7 {
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
					if version >= 0 && version <= 7 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].PartitionIndex); err != nil {
							return nil, err
						}
					}
					// CommittedOffset
					if version >= 0 && version <= 7 {
						if err := protocol.WriteInt64(elemW, structItem.Partitions[i].CommittedOffset); err != nil {
							return nil, err
						}
					}
					// CommittedLeaderEpoch
					if version >= 5 && version <= 7 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].CommittedLeaderEpoch); err != nil {
							return nil, err
						}
					}
					// Metadata
					if version >= 0 && version <= 7 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.Partitions[i].Metadata); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.Partitions[i].Metadata); err != nil {
								return nil, err
							}
						}
					}
					// ErrorCode
					if version >= 0 && version <= 7 {
						if err := protocol.WriteInt16(elemW, structItem.Partitions[i].ErrorCode); err != nil {
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
	// ErrorCode
	if version >= 2 && version <= 7 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// Groups
	if version >= 8 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(OffsetFetchResponseOffsetFetchResponseGroup)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// GroupId
			if version >= 8 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.GroupId); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.GroupId); err != nil {
						return nil, err
					}
				}
			}
			// Topics
			if version >= 8 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Topics) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Topics))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Topics {
					// Name
					if version >= 8 && version <= 9 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Topics[i].Name); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Topics[i].Name); err != nil {
								return nil, err
							}
						}
					}
					// TopicId
					if version >= 10 && version <= 999 {
						if err := protocol.WriteUUID(elemW, structItem.Topics[i].TopicId); err != nil {
							return nil, err
						}
					}
					// Partitions
					if version >= 8 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Topics[i].Partitions) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Topics[i].Partitions))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Topics[i].Partitions {
							// PartitionIndex
							if version >= 8 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Topics[i].Partitions[i].PartitionIndex); err != nil {
									return nil, err
								}
							}
							// CommittedOffset
							if version >= 8 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Topics[i].Partitions[i].CommittedOffset); err != nil {
									return nil, err
								}
							}
							// CommittedLeaderEpoch
							if version >= 8 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Topics[i].Partitions[i].CommittedLeaderEpoch); err != nil {
									return nil, err
								}
							}
							// Metadata
							if version >= 8 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, structItem.Topics[i].Partitions[i].Metadata); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, structItem.Topics[i].Partitions[i].Metadata); err != nil {
										return nil, err
									}
								}
							}
							// ErrorCode
							if version >= 8 && version <= 999 {
								if err := protocol.WriteInt16(elemW, structItem.Topics[i].Partitions[i].ErrorCode); err != nil {
									return nil, err
								}
							}
						}
					}
				}
			}
			// ErrorCode
			if version >= 8 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ErrorCode); err != nil {
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
		items := make([]interface{}, len(m.Groups))
		for i := range m.Groups {
			items[i] = m.Groups[i]
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

// Read reads a OffsetFetchResponse from an io.Reader for the given version.
func (m *OffsetFetchResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 10 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// Topics
	if version >= 0 && version <= 7 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem OffsetFetchResponseOffsetFetchResponseTopic
			elemR := bytes.NewReader(data)
			// Name
			if version >= 0 && version <= 7 {
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
			if version >= 0 && version <= 7 {
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
				var tempElem OffsetFetchResponseOffsetFetchResponseTopic
				// Name
				if version >= 0 && version <= 7 {
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
				if version >= 0 && version <= 7 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem OffsetFetchResponseOffsetFetchResponsePartition
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 7 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// CommittedOffset
						if version >= 0 && version <= 7 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.CommittedOffset = val
						}
						// CommittedLeaderEpoch
						if version >= 5 && version <= 7 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.CommittedLeaderEpoch = val
						}
						// Metadata
						if version >= 0 && version <= 7 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Metadata = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Metadata = val
							}
						}
						// ErrorCode
						if version >= 0 && version <= 7 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ErrorCode = val
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
							var tempElem OffsetFetchResponseOffsetFetchResponsePartition
							// PartitionIndex
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// CommittedOffset
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.CommittedOffset = val
							}
							// CommittedLeaderEpoch
							if version >= 5 && version <= 7 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CommittedLeaderEpoch = val
							}
							// Metadata
							if version >= 0 && version <= 7 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Metadata = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Metadata = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// CommittedOffset
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
									return err
								}
							}
							// CommittedLeaderEpoch
							if version >= 5 && version <= 7 {
								if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
									return err
								}
							}
							// Metadata
							if version >= 0 && version <= 7 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
										return err
									}
								}
							}
							// ErrorCode
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
						tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartition)
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
							var tempElem OffsetFetchResponseOffsetFetchResponsePartition
							// PartitionIndex
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// CommittedOffset
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.CommittedOffset = val
							}
							// CommittedLeaderEpoch
							if version >= 5 && version <= 7 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CommittedLeaderEpoch = val
							}
							// Metadata
							if version >= 0 && version <= 7 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Metadata = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Metadata = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// CommittedOffset
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
									return err
								}
							}
							// CommittedLeaderEpoch
							if version >= 5 && version <= 7 {
								if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
									return err
								}
							}
							// Metadata
							if version >= 0 && version <= 7 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
										return err
									}
								}
							}
							// ErrorCode
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
						tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartition)
						}
					}
				}
				// Name
				if version >= 0 && version <= 7 {
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
				if version >= 0 && version <= 7 {
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
						if version >= 0 && version <= 7 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
							}
						}
						// CommittedOffset
						if version >= 0 && version <= 7 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CommittedOffset); err != nil {
								return err
							}
						}
						// CommittedLeaderEpoch
						if version >= 5 && version <= 7 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CommittedLeaderEpoch); err != nil {
								return err
							}
						}
						// Metadata
						if version >= 0 && version <= 7 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
									return err
								}
							}
						}
						// ErrorCode
						if version >= 0 && version <= 7 {
							if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
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
			m.Topics = make([]OffsetFetchResponseOffsetFetchResponseTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(OffsetFetchResponseOffsetFetchResponseTopic)
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
				var tempElem OffsetFetchResponseOffsetFetchResponseTopic
				// Name
				if version >= 0 && version <= 7 {
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
				if version >= 0 && version <= 7 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem OffsetFetchResponseOffsetFetchResponsePartition
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 7 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// CommittedOffset
						if version >= 0 && version <= 7 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.CommittedOffset = val
						}
						// CommittedLeaderEpoch
						if version >= 5 && version <= 7 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.CommittedLeaderEpoch = val
						}
						// Metadata
						if version >= 0 && version <= 7 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Metadata = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Metadata = val
							}
						}
						// ErrorCode
						if version >= 0 && version <= 7 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ErrorCode = val
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
							var tempElem OffsetFetchResponseOffsetFetchResponsePartition
							// PartitionIndex
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// CommittedOffset
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.CommittedOffset = val
							}
							// CommittedLeaderEpoch
							if version >= 5 && version <= 7 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CommittedLeaderEpoch = val
							}
							// Metadata
							if version >= 0 && version <= 7 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Metadata = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Metadata = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// CommittedOffset
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
									return err
								}
							}
							// CommittedLeaderEpoch
							if version >= 5 && version <= 7 {
								if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
									return err
								}
							}
							// Metadata
							if version >= 0 && version <= 7 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
										return err
									}
								}
							}
							// ErrorCode
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
						tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartition)
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
							var tempElem OffsetFetchResponseOffsetFetchResponsePartition
							// PartitionIndex
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// CommittedOffset
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.CommittedOffset = val
							}
							// CommittedLeaderEpoch
							if version >= 5 && version <= 7 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CommittedLeaderEpoch = val
							}
							// Metadata
							if version >= 0 && version <= 7 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Metadata = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Metadata = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 7 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// CommittedOffset
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
									return err
								}
							}
							// CommittedLeaderEpoch
							if version >= 5 && version <= 7 {
								if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
									return err
								}
							}
							// Metadata
							if version >= 0 && version <= 7 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
										return err
									}
								}
							}
							// ErrorCode
							if version >= 0 && version <= 7 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
						tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartition)
						}
					}
				}
				// Name
				if version >= 0 && version <= 7 {
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
				if version >= 0 && version <= 7 {
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
						if version >= 0 && version <= 7 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
							}
						}
						// CommittedOffset
						if version >= 0 && version <= 7 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CommittedOffset); err != nil {
								return err
							}
						}
						// CommittedLeaderEpoch
						if version >= 5 && version <= 7 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CommittedLeaderEpoch); err != nil {
								return err
							}
						}
						// Metadata
						if version >= 0 && version <= 7 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
									return err
								}
							}
						}
						// ErrorCode
						if version >= 0 && version <= 7 {
							if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
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
			m.Topics = make([]OffsetFetchResponseOffsetFetchResponseTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(OffsetFetchResponseOffsetFetchResponseTopic)
			}
		}
	}
	// ErrorCode
	if version >= 2 && version <= 7 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// Groups
	if version >= 8 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem OffsetFetchResponseOffsetFetchResponseGroup
			elemR := bytes.NewReader(data)
			// GroupId
			if version >= 8 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupId = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupId = val
				}
			}
			// Topics
			if version >= 8 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// ErrorCode
			if version >= 8 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ErrorCode = val
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
				var tempElem OffsetFetchResponseOffsetFetchResponseGroup
				// GroupId
				if version >= 8 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					}
				}
				// Topics
				if version >= 8 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem OffsetFetchResponseOffsetFetchResponseTopics
						elemR := bytes.NewReader(data)
						// Name
						if version >= 8 && version <= 9 {
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
						// TopicId
						if version >= 10 && version <= 999 {
							val, err := protocol.ReadUUID(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.TopicId = val
						}
						// Partitions
						if version >= 8 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
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
							var tempElem OffsetFetchResponseOffsetFetchResponseTopics
							// Name
							if version >= 8 && version <= 9 {
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
							// TopicId
							if version >= 10 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.TopicId = val
							}
							// Partitions
							if version >= 8 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem OffsetFetchResponseOffsetFetchResponsePartitions
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// CommittedOffset
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CommittedOffset = val
									}
									// CommittedLeaderEpoch
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CommittedLeaderEpoch = val
									}
									// Metadata
									if version >= 8 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Metadata = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Metadata = val
										}
									}
									// ErrorCode
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ErrorCode = val
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
										var tempElem OffsetFetchResponseOffsetFetchResponsePartitions
										// PartitionIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CommittedOffset = val
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CommittedLeaderEpoch = val
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.ErrorCode = val
										}
										// PartitionIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
												return err
											}
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
												return err
											}
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartitions, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartitions)
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
										var tempElem OffsetFetchResponseOffsetFetchResponsePartitions
										// PartitionIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CommittedOffset = val
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CommittedLeaderEpoch = val
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.ErrorCode = val
										}
										// PartitionIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
												return err
											}
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
												return err
											}
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartitions, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartitions)
									}
								}
							}
							// Name
							if version >= 8 && version <= 9 {
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
							// TopicId
							if version >= 10 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
									return err
								}
							}
							// Partitions
							if version >= 8 && version <= 999 {
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
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// CommittedOffset
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CommittedOffset); err != nil {
											return err
										}
									}
									// CommittedLeaderEpoch
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CommittedLeaderEpoch); err != nil {
											return err
										}
									}
									// Metadata
									if version >= 8 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
												return err
											}
										}
									}
									// ErrorCode
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
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
						tempElem.Topics = make([]OffsetFetchResponseOffsetFetchResponseTopics, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(OffsetFetchResponseOffsetFetchResponseTopics)
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
							var tempElem OffsetFetchResponseOffsetFetchResponseTopics
							// Name
							if version >= 8 && version <= 9 {
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
							// TopicId
							if version >= 10 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.TopicId = val
							}
							// Partitions
							if version >= 8 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem OffsetFetchResponseOffsetFetchResponsePartitions
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// CommittedOffset
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CommittedOffset = val
									}
									// CommittedLeaderEpoch
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CommittedLeaderEpoch = val
									}
									// Metadata
									if version >= 8 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Metadata = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Metadata = val
										}
									}
									// ErrorCode
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ErrorCode = val
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
										var tempElem OffsetFetchResponseOffsetFetchResponsePartitions
										// PartitionIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CommittedOffset = val
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CommittedLeaderEpoch = val
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.ErrorCode = val
										}
										// PartitionIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
												return err
											}
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
												return err
											}
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartitions, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartitions)
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
										var tempElem OffsetFetchResponseOffsetFetchResponsePartitions
										// PartitionIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CommittedOffset = val
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CommittedLeaderEpoch = val
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.ErrorCode = val
										}
										// PartitionIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
												return err
											}
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
												return err
											}
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartitions, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartitions)
									}
								}
							}
							// Name
							if version >= 8 && version <= 9 {
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
							// TopicId
							if version >= 10 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
									return err
								}
							}
							// Partitions
							if version >= 8 && version <= 999 {
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
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// CommittedOffset
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CommittedOffset); err != nil {
											return err
										}
									}
									// CommittedLeaderEpoch
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CommittedLeaderEpoch); err != nil {
											return err
										}
									}
									// Metadata
									if version >= 8 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
												return err
											}
										}
									}
									// ErrorCode
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
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
						tempElem.Topics = make([]OffsetFetchResponseOffsetFetchResponseTopics, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(OffsetFetchResponseOffsetFetchResponseTopics)
						}
					}
				}
				// ErrorCode
				if version >= 8 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// GroupId
				if version >= 8 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					}
				}
				// Topics
				if version >= 8 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Topics) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topics))); err != nil {
							return err
						}
					}
					for i := range tempElem.Topics {
						// Name
						if version >= 8 && version <= 9 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
							}
						}
						// TopicId
						if version >= 10 && version <= 999 {
							if err := protocol.WriteUUID(elemW, tempElem.Topics[i].TopicId); err != nil {
								return err
							}
						}
						// Partitions
						if version >= 8 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Topics[i].Partitions) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topics[i].Partitions))); err != nil {
									return err
								}
							}
							for i := range tempElem.Topics[i].Partitions {
								// PartitionIndex
								if version >= 8 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Topics[i].Partitions[i].PartitionIndex); err != nil {
										return err
									}
								}
								// CommittedOffset
								if version >= 8 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Topics[i].Partitions[i].CommittedOffset); err != nil {
										return err
									}
								}
								// CommittedLeaderEpoch
								if version >= 8 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Topics[i].Partitions[i].CommittedLeaderEpoch); err != nil {
										return err
									}
								}
								// Metadata
								if version >= 8 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactNullableString(elemW, tempElem.Topics[i].Partitions[i].Metadata); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteNullableString(elemW, tempElem.Topics[i].Partitions[i].Metadata); err != nil {
											return err
										}
									}
								}
								// ErrorCode
								if version >= 8 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Topics[i].Partitions[i].ErrorCode); err != nil {
										return err
									}
								}
							}
						}
					}
				}
				// ErrorCode
				if version >= 8 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
			m.Groups = make([]OffsetFetchResponseOffsetFetchResponseGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(OffsetFetchResponseOffsetFetchResponseGroup)
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
				var tempElem OffsetFetchResponseOffsetFetchResponseGroup
				// GroupId
				if version >= 8 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					}
				}
				// Topics
				if version >= 8 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem OffsetFetchResponseOffsetFetchResponseTopics
						elemR := bytes.NewReader(data)
						// Name
						if version >= 8 && version <= 9 {
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
						// TopicId
						if version >= 10 && version <= 999 {
							val, err := protocol.ReadUUID(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.TopicId = val
						}
						// Partitions
						if version >= 8 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
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
							var tempElem OffsetFetchResponseOffsetFetchResponseTopics
							// Name
							if version >= 8 && version <= 9 {
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
							// TopicId
							if version >= 10 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.TopicId = val
							}
							// Partitions
							if version >= 8 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem OffsetFetchResponseOffsetFetchResponsePartitions
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// CommittedOffset
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CommittedOffset = val
									}
									// CommittedLeaderEpoch
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CommittedLeaderEpoch = val
									}
									// Metadata
									if version >= 8 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Metadata = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Metadata = val
										}
									}
									// ErrorCode
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ErrorCode = val
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
										var tempElem OffsetFetchResponseOffsetFetchResponsePartitions
										// PartitionIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CommittedOffset = val
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CommittedLeaderEpoch = val
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.ErrorCode = val
										}
										// PartitionIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
												return err
											}
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
												return err
											}
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartitions, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartitions)
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
										var tempElem OffsetFetchResponseOffsetFetchResponsePartitions
										// PartitionIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CommittedOffset = val
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CommittedLeaderEpoch = val
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.ErrorCode = val
										}
										// PartitionIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
												return err
											}
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
												return err
											}
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartitions, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartitions)
									}
								}
							}
							// Name
							if version >= 8 && version <= 9 {
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
							// TopicId
							if version >= 10 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
									return err
								}
							}
							// Partitions
							if version >= 8 && version <= 999 {
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
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// CommittedOffset
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CommittedOffset); err != nil {
											return err
										}
									}
									// CommittedLeaderEpoch
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CommittedLeaderEpoch); err != nil {
											return err
										}
									}
									// Metadata
									if version >= 8 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
												return err
											}
										}
									}
									// ErrorCode
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
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
						tempElem.Topics = make([]OffsetFetchResponseOffsetFetchResponseTopics, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(OffsetFetchResponseOffsetFetchResponseTopics)
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
							var tempElem OffsetFetchResponseOffsetFetchResponseTopics
							// Name
							if version >= 8 && version <= 9 {
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
							// TopicId
							if version >= 10 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.TopicId = val
							}
							// Partitions
							if version >= 8 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem OffsetFetchResponseOffsetFetchResponsePartitions
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// CommittedOffset
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CommittedOffset = val
									}
									// CommittedLeaderEpoch
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CommittedLeaderEpoch = val
									}
									// Metadata
									if version >= 8 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Metadata = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Metadata = val
										}
									}
									// ErrorCode
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ErrorCode = val
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
										var tempElem OffsetFetchResponseOffsetFetchResponsePartitions
										// PartitionIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CommittedOffset = val
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CommittedLeaderEpoch = val
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.ErrorCode = val
										}
										// PartitionIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
												return err
											}
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
												return err
											}
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartitions, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartitions)
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
										var tempElem OffsetFetchResponseOffsetFetchResponsePartitions
										// PartitionIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CommittedOffset = val
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CommittedLeaderEpoch = val
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Metadata = val
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.ErrorCode = val
										}
										// PartitionIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// CommittedOffset
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CommittedOffset); err != nil {
												return err
											}
										}
										// CommittedLeaderEpoch
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CommittedLeaderEpoch); err != nil {
												return err
											}
										}
										// Metadata
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Metadata); err != nil {
													return err
												}
											}
										}
										// ErrorCode
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]OffsetFetchResponseOffsetFetchResponsePartitions, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(OffsetFetchResponseOffsetFetchResponsePartitions)
									}
								}
							}
							// Name
							if version >= 8 && version <= 9 {
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
							// TopicId
							if version >= 10 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
									return err
								}
							}
							// Partitions
							if version >= 8 && version <= 999 {
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
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// CommittedOffset
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CommittedOffset); err != nil {
											return err
										}
									}
									// CommittedLeaderEpoch
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CommittedLeaderEpoch); err != nil {
											return err
										}
									}
									// Metadata
									if version >= 8 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].Metadata); err != nil {
												return err
											}
										}
									}
									// ErrorCode
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
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
						tempElem.Topics = make([]OffsetFetchResponseOffsetFetchResponseTopics, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(OffsetFetchResponseOffsetFetchResponseTopics)
						}
					}
				}
				// ErrorCode
				if version >= 8 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// GroupId
				if version >= 8 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					}
				}
				// Topics
				if version >= 8 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Topics) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topics))); err != nil {
							return err
						}
					}
					for i := range tempElem.Topics {
						// Name
						if version >= 8 && version <= 9 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
							}
						}
						// TopicId
						if version >= 10 && version <= 999 {
							if err := protocol.WriteUUID(elemW, tempElem.Topics[i].TopicId); err != nil {
								return err
							}
						}
						// Partitions
						if version >= 8 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Topics[i].Partitions) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topics[i].Partitions))); err != nil {
									return err
								}
							}
							for i := range tempElem.Topics[i].Partitions {
								// PartitionIndex
								if version >= 8 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Topics[i].Partitions[i].PartitionIndex); err != nil {
										return err
									}
								}
								// CommittedOffset
								if version >= 8 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Topics[i].Partitions[i].CommittedOffset); err != nil {
										return err
									}
								}
								// CommittedLeaderEpoch
								if version >= 8 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Topics[i].Partitions[i].CommittedLeaderEpoch); err != nil {
										return err
									}
								}
								// Metadata
								if version >= 8 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactNullableString(elemW, tempElem.Topics[i].Partitions[i].Metadata); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteNullableString(elemW, tempElem.Topics[i].Partitions[i].Metadata); err != nil {
											return err
										}
									}
								}
								// ErrorCode
								if version >= 8 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Topics[i].Partitions[i].ErrorCode); err != nil {
										return err
									}
								}
							}
						}
					}
				}
				// ErrorCode
				if version >= 8 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
			m.Groups = make([]OffsetFetchResponseOffsetFetchResponseGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(OffsetFetchResponseOffsetFetchResponseGroup)
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

// OffsetFetchResponseOffsetFetchResponseTopic represents The responses per topic..
type OffsetFetchResponseOffsetFetchResponseTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-7"`
	// The responses per partition.
	Partitions []OffsetFetchResponseOffsetFetchResponsePartition `json:"partitions" versions:"0-7"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for OffsetFetchResponseOffsetFetchResponseTopic.
func (m *OffsetFetchResponseOffsetFetchResponseTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetFetchResponseOffsetFetchResponseTopic.
func (m *OffsetFetchResponseOffsetFetchResponseTopic) readTaggedFields(r io.Reader, version int16) error {
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

// OffsetFetchResponseOffsetFetchResponsePartition represents The responses per partition..
type OffsetFetchResponseOffsetFetchResponsePartition struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-7"`
	// The committed message offset.
	CommittedOffset int64 `json:"committedoffset" versions:"0-7"`
	// The leader epoch.
	CommittedLeaderEpoch int32 `json:"committedleaderepoch" versions:"5-7"`
	// The partition metadata.
	Metadata *string `json:"metadata" versions:"0-7"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-7"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for OffsetFetchResponseOffsetFetchResponsePartition.
func (m *OffsetFetchResponseOffsetFetchResponsePartition) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetFetchResponseOffsetFetchResponsePartition.
func (m *OffsetFetchResponseOffsetFetchResponsePartition) readTaggedFields(r io.Reader, version int16) error {
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

// OffsetFetchResponseOffsetFetchResponseGroup represents The responses per group id..
type OffsetFetchResponseOffsetFetchResponseGroup struct {
	// The group ID.
	GroupId string `json:"groupid" versions:"8-999"`
	// The responses per topic.
	Topics []OffsetFetchResponseOffsetFetchResponseTopics `json:"topics" versions:"8-999"`
	// The group-level error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"8-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for OffsetFetchResponseOffsetFetchResponseGroup.
func (m *OffsetFetchResponseOffsetFetchResponseGroup) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetFetchResponseOffsetFetchResponseGroup.
func (m *OffsetFetchResponseOffsetFetchResponseGroup) readTaggedFields(r io.Reader, version int16) error {
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

// OffsetFetchResponseOffsetFetchResponseTopics represents The responses per topic..
type OffsetFetchResponseOffsetFetchResponseTopics struct {
	// The topic name.
	Name string `json:"name" versions:"8-9"`
	// The topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"10-999"`
	// The responses per partition.
	Partitions []OffsetFetchResponseOffsetFetchResponsePartitions `json:"partitions" versions:"8-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for OffsetFetchResponseOffsetFetchResponseTopics.
func (m *OffsetFetchResponseOffsetFetchResponseTopics) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetFetchResponseOffsetFetchResponseTopics.
func (m *OffsetFetchResponseOffsetFetchResponseTopics) readTaggedFields(r io.Reader, version int16) error {
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

// OffsetFetchResponseOffsetFetchResponsePartitions represents The responses per partition..
type OffsetFetchResponseOffsetFetchResponsePartitions struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"8-999"`
	// The committed message offset.
	CommittedOffset int64 `json:"committedoffset" versions:"8-999"`
	// The leader epoch.
	CommittedLeaderEpoch int32 `json:"committedleaderepoch" versions:"8-999"`
	// The partition metadata.
	Metadata *string `json:"metadata" versions:"8-999"`
	// The partition-level error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"8-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for OffsetFetchResponseOffsetFetchResponsePartitions.
func (m *OffsetFetchResponseOffsetFetchResponsePartitions) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetFetchResponseOffsetFetchResponsePartitions.
func (m *OffsetFetchResponseOffsetFetchResponsePartitions) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for OffsetFetchResponse.
func (m *OffsetFetchResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetFetchResponse.
func (m *OffsetFetchResponse) readTaggedFields(r io.Reader, version int16) error {
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
