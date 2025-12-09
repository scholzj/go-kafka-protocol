package offsetfetch

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	OffsetFetchRequestApiKey        = 9
	OffsetFetchRequestHeaderVersion = 1
)

// OffsetFetchRequest represents a request message.
type OffsetFetchRequest struct {
	// The group to fetch offsets for.
	GroupId string `json:"groupid" versions:"0-7"`
	// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
	Topics []OffsetFetchRequestOffsetFetchRequestTopic `json:"topics" versions:"0-7"`
	// Each group we would like to fetch offsets for.
	Groups []OffsetFetchRequestOffsetFetchRequestGroup `json:"groups" versions:"8-999"`
	// Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
	RequireStable bool `json:"requirestable" versions:"7-999"`
}

// Encode encodes a OffsetFetchRequest to a byte slice for the given version.
func (m *OffsetFetchRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a OffsetFetchRequest from a byte slice for the given version.
func (m *OffsetFetchRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a OffsetFetchRequest to an io.Writer for the given version.
func (m *OffsetFetchRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 10 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 7 {
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
	if version >= 0 && version <= 7 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(OffsetFetchRequestOffsetFetchRequestTopic)
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
			// PartitionIndexes
			if version >= 0 && version <= 7 {
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
	// Groups
	if version >= 8 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(OffsetFetchRequestOffsetFetchRequestGroup)
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
			// MemberId
			if version >= 9 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.MemberId); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.MemberId); err != nil {
						return nil, err
					}
				}
			}
			// MemberEpoch
			if version >= 9 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.MemberEpoch); err != nil {
					return nil, err
				}
			}
			// Topics
			if version >= 8 && version <= 999 {
				if structItem.Topics == nil {
					if isFlexible {
						if err := protocol.WriteVaruint32(elemW, 0); err != nil {
							return nil, err
						}
					} else {
						if err := protocol.WriteInt32(elemW, -1); err != nil {
							return nil, err
						}
					}
				} else {
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
						// PartitionIndexes
						if version >= 8 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, structItem.Topics[i].PartitionIndexes); err != nil {
									return nil, err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, structItem.Topics[i].PartitionIndexes); err != nil {
									return nil, err
								}
							}
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
	// RequireStable
	if version >= 7 && version <= 999 {
		if err := protocol.WriteBool(w, m.RequireStable); err != nil {
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

// Read reads a OffsetFetchRequest from an io.Reader for the given version.
func (m *OffsetFetchRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 10 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 7 {
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
	if version >= 0 && version <= 7 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem OffsetFetchRequestOffsetFetchRequestTopic
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
			// PartitionIndexes
			if version >= 0 && version <= 7 {
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
				var tempElem OffsetFetchRequestOffsetFetchRequestTopic
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
				// PartitionIndexes
				if version >= 0 && version <= 7 {
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
				// PartitionIndexes
				if version >= 0 && version <= 7 {
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
			m.Topics = make([]OffsetFetchRequestOffsetFetchRequestTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(OffsetFetchRequestOffsetFetchRequestTopic)
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
				var tempElem OffsetFetchRequestOffsetFetchRequestTopic
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
				// PartitionIndexes
				if version >= 0 && version <= 7 {
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
				// PartitionIndexes
				if version >= 0 && version <= 7 {
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
			m.Topics = make([]OffsetFetchRequestOffsetFetchRequestTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(OffsetFetchRequestOffsetFetchRequestTopic)
			}
		}
	}
	// Groups
	if version >= 8 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem OffsetFetchRequestOffsetFetchRequestGroup
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
			// MemberId
			if version >= 9 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.MemberId = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.MemberId = val
				}
			}
			// MemberEpoch
			if version >= 9 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.MemberEpoch = val
			}
			// Topics
			if version >= 8 && version <= 999 {
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
				var tempElem OffsetFetchRequestOffsetFetchRequestGroup
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
				// MemberId
				if version >= 9 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.MemberId = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.MemberId = val
					}
				}
				// MemberEpoch
				if version >= 9 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.MemberEpoch = val
				}
				// Topics
				if version >= 8 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem OffsetFetchRequestOffsetFetchRequestTopics
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
						// PartitionIndexes
						if version >= 8 && version <= 999 {
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
						consumed := len(data) - elemR.Len()
						return elem, consumed, nil
					}
					if isFlexible {
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint == 0 {
							tempElem.Topics = nil
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
							var tempElem OffsetFetchRequestOffsetFetchRequestTopics
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
							// PartitionIndexes
							if version >= 8 && version <= 999 {
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
							// PartitionIndexes
							if version >= 8 && version <= 999 {
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
						tempElem.Topics = make([]OffsetFetchRequestOffsetFetchRequestTopics, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(OffsetFetchRequestOffsetFetchRequestTopics)
						}
					} else {
						length, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						if length == -1 {
							tempElem.Topics = nil
							return nil
						}
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem OffsetFetchRequestOffsetFetchRequestTopics
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
							// PartitionIndexes
							if version >= 8 && version <= 999 {
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
							// PartitionIndexes
							if version >= 8 && version <= 999 {
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
						tempElem.Topics = make([]OffsetFetchRequestOffsetFetchRequestTopics, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(OffsetFetchRequestOffsetFetchRequestTopics)
						}
					}
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
				// MemberId
				if version >= 9 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.MemberId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.MemberId); err != nil {
							return err
						}
					}
				}
				// MemberEpoch
				if version >= 9 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.MemberEpoch); err != nil {
						return err
					}
				}
				// Topics
				if version >= 8 && version <= 999 {
					if tempElem.Topics == nil {
						if isFlexible {
							if err := protocol.WriteVaruint32(elemW, 0); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(elemW, -1); err != nil {
								return err
							}
						}
					} else {
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
							// PartitionIndexes
							if version >= 8 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.Topics[i].PartitionIndexes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.Topics[i].PartitionIndexes); err != nil {
										return err
									}
								}
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
			m.Groups = make([]OffsetFetchRequestOffsetFetchRequestGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(OffsetFetchRequestOffsetFetchRequestGroup)
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
				var tempElem OffsetFetchRequestOffsetFetchRequestGroup
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
				// MemberId
				if version >= 9 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.MemberId = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.MemberId = val
					}
				}
				// MemberEpoch
				if version >= 9 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.MemberEpoch = val
				}
				// Topics
				if version >= 8 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem OffsetFetchRequestOffsetFetchRequestTopics
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
						// PartitionIndexes
						if version >= 8 && version <= 999 {
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
						consumed := len(data) - elemR.Len()
						return elem, consumed, nil
					}
					if isFlexible {
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint == 0 {
							tempElem.Topics = nil
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
							var tempElem OffsetFetchRequestOffsetFetchRequestTopics
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
							// PartitionIndexes
							if version >= 8 && version <= 999 {
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
							// PartitionIndexes
							if version >= 8 && version <= 999 {
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
						tempElem.Topics = make([]OffsetFetchRequestOffsetFetchRequestTopics, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(OffsetFetchRequestOffsetFetchRequestTopics)
						}
					} else {
						length, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						if length == -1 {
							tempElem.Topics = nil
							return nil
						}
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem OffsetFetchRequestOffsetFetchRequestTopics
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
							// PartitionIndexes
							if version >= 8 && version <= 999 {
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
							// PartitionIndexes
							if version >= 8 && version <= 999 {
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
						tempElem.Topics = make([]OffsetFetchRequestOffsetFetchRequestTopics, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(OffsetFetchRequestOffsetFetchRequestTopics)
						}
					}
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
				// MemberId
				if version >= 9 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.MemberId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.MemberId); err != nil {
							return err
						}
					}
				}
				// MemberEpoch
				if version >= 9 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.MemberEpoch); err != nil {
						return err
					}
				}
				// Topics
				if version >= 8 && version <= 999 {
					if tempElem.Topics == nil {
						if isFlexible {
							if err := protocol.WriteVaruint32(elemW, 0); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(elemW, -1); err != nil {
								return err
							}
						}
					} else {
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
							// PartitionIndexes
							if version >= 8 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.Topics[i].PartitionIndexes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.Topics[i].PartitionIndexes); err != nil {
										return err
									}
								}
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
			m.Groups = make([]OffsetFetchRequestOffsetFetchRequestGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(OffsetFetchRequestOffsetFetchRequestGroup)
			}
		}
	}
	// RequireStable
	if version >= 7 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.RequireStable = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// OffsetFetchRequestOffsetFetchRequestTopic represents Each topic we would like to fetch offsets for, or null to fetch offsets for all topics..
type OffsetFetchRequestOffsetFetchRequestTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-7"`
	// The partition indexes we would like to fetch offsets for.
	PartitionIndexes []int32 `json:"partitionindexes" versions:"0-7"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for OffsetFetchRequestOffsetFetchRequestTopic.
func (m *OffsetFetchRequestOffsetFetchRequestTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetFetchRequestOffsetFetchRequestTopic.
func (m *OffsetFetchRequestOffsetFetchRequestTopic) readTaggedFields(r io.Reader, version int16) error {
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

// OffsetFetchRequestOffsetFetchRequestGroup represents Each group we would like to fetch offsets for..
type OffsetFetchRequestOffsetFetchRequestGroup struct {
	// The group ID.
	GroupId string `json:"groupid" versions:"8-999"`
	// The member id.
	MemberId *string `json:"memberid" versions:"9-999"`
	// The member epoch if using the new consumer protocol (KIP-848).
	MemberEpoch int32 `json:"memberepoch" versions:"9-999"`
	// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
	Topics []OffsetFetchRequestOffsetFetchRequestTopics `json:"topics" versions:"8-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for OffsetFetchRequestOffsetFetchRequestGroup.
func (m *OffsetFetchRequestOffsetFetchRequestGroup) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetFetchRequestOffsetFetchRequestGroup.
func (m *OffsetFetchRequestOffsetFetchRequestGroup) readTaggedFields(r io.Reader, version int16) error {
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

// OffsetFetchRequestOffsetFetchRequestTopics represents Each topic we would like to fetch offsets for, or null to fetch offsets for all topics..
type OffsetFetchRequestOffsetFetchRequestTopics struct {
	// The topic name.
	Name string `json:"name" versions:"8-9"`
	// The topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"10-999"`
	// The partition indexes we would like to fetch offsets for.
	PartitionIndexes []int32 `json:"partitionindexes" versions:"8-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for OffsetFetchRequestOffsetFetchRequestTopics.
func (m *OffsetFetchRequestOffsetFetchRequestTopics) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetFetchRequestOffsetFetchRequestTopics.
func (m *OffsetFetchRequestOffsetFetchRequestTopics) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for OffsetFetchRequest.
func (m *OffsetFetchRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetFetchRequest.
func (m *OffsetFetchRequest) readTaggedFields(r io.Reader, version int16) error {
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
