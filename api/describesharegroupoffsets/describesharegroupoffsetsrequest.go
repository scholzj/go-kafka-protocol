package describesharegroupoffsets

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeShareGroupOffsetsRequestApiKey        = 90
	DescribeShareGroupOffsetsRequestHeaderVersion = 1
)

// DescribeShareGroupOffsetsRequest represents a request message.
type DescribeShareGroupOffsetsRequest struct {
	// The groups to describe offsets for.
	Groups []DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup `json:"groups" versions:"0-999"`
}

// Encode encodes a DescribeShareGroupOffsetsRequest to a byte slice for the given version.
func (m *DescribeShareGroupOffsetsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeShareGroupOffsetsRequest from a byte slice for the given version.
func (m *DescribeShareGroupOffsetsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeShareGroupOffsetsRequest to an io.Writer for the given version.
func (m *DescribeShareGroupOffsetsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// Groups
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// GroupId
			if version >= 0 && version <= 999 {
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
			if version >= 0 && version <= 999 {
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
						// TopicName
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, structItem.Topics[i].TopicName); err != nil {
									return nil, err
								}
							} else {
								if err := protocol.WriteString(elemW, structItem.Topics[i].TopicName); err != nil {
									return nil, err
								}
							}
						}
						// Partitions
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, structItem.Topics[i].Partitions); err != nil {
									return nil, err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, structItem.Topics[i].Partitions); err != nil {
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
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a DescribeShareGroupOffsetsRequest from an io.Reader for the given version.
func (m *DescribeShareGroupOffsetsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// Groups
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup
			elemR := bytes.NewReader(data)
			// GroupId
			if version >= 0 && version <= 999 {
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
				var tempElem DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup
				// GroupId
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic
						elemR := bytes.NewReader(data)
						// TopicName
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.TopicName = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.TopicName = val
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
							var tempElem DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
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
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.TopicName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.TopicName); err != nil {
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
						tempElem.Topics = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic)
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
							var tempElem DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
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
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.TopicName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.TopicName); err != nil {
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
						tempElem.Topics = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic)
						}
					}
				}
				// GroupId
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
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
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Topics[i].TopicName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Topics[i].TopicName); err != nil {
										return err
									}
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.Topics[i].Partitions); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.Topics[i].Partitions); err != nil {
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
			m.Groups = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup)
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
				var tempElem DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup
				// GroupId
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic
						elemR := bytes.NewReader(data)
						// TopicName
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.TopicName = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.TopicName = val
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
							var tempElem DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
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
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.TopicName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.TopicName); err != nil {
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
						tempElem.Topics = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic)
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
							var tempElem DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
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
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.TopicName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.TopicName); err != nil {
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
						tempElem.Topics = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic)
						}
					}
				}
				// GroupId
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
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
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Topics[i].TopicName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Topics[i].TopicName); err != nil {
										return err
									}
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.Topics[i].Partitions); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.Topics[i].Partitions); err != nil {
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
			m.Groups = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup)
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

// DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup represents The groups to describe offsets for..
type DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup struct {
	// The group identifier.
	GroupId string `json:"groupid" versions:"0-999"`
	// The topics to describe offsets for, or null for all topic-partitions.
	Topics []DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic `json:"topics" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup.
func (m *DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup.
func (m *DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic represents The topics to describe offsets for, or null for all topic-partitions..
type DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic struct {
	// The topic name.
	TopicName string `json:"topicname" versions:"0-999"`
	// The partitions.
	Partitions []int32 `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic.
func (m *DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic.
func (m *DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for DescribeShareGroupOffsetsRequest.
func (m *DescribeShareGroupOffsetsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeShareGroupOffsetsRequest.
func (m *DescribeShareGroupOffsetsRequest) readTaggedFields(r io.Reader, version int16) error {
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
