package produce

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ProduceRequestApiKey        = 0
	ProduceRequestHeaderVersion = 1
)

// ProduceRequest represents a request message.
type ProduceRequest struct {
	// The transactional ID, or null if the producer is not transactional.
	TransactionalId *string `json:"transactionalid" versions:"3-999"`
	// The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
	Acks int16 `json:"acks" versions:"0-999"`
	// The timeout to await a response in milliseconds.
	TimeoutMs int32 `json:"timeoutms" versions:"0-999"`
	// Each topic to produce to.
	TopicData []ProduceRequestTopicProduceData `json:"topicdata" versions:"0-999"`
}

// Encode encodes a ProduceRequest to a byte slice for the given version.
func (m *ProduceRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ProduceRequest from a byte slice for the given version.
func (m *ProduceRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ProduceRequest to an io.Writer for the given version.
func (m *ProduceRequest) Write(w io.Writer, version int16) error {
	if version < 3 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// TransactionalId
	if version >= 3 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.TransactionalId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.TransactionalId); err != nil {
				return err
			}
		}
	}
	// Acks
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.Acks); err != nil {
			return err
		}
	}
	// TimeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TimeoutMs); err != nil {
			return err
		}
	}
	// TopicData
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ProduceRequestTopicProduceData)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Name
			if version >= 0 && version <= 12 {
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
			// TopicId
			if version >= 13 && version <= 999 {
				if err := protocol.WriteUUID(elemW, structItem.TopicId); err != nil {
					return nil, err
				}
			}
			// PartitionData
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.PartitionData) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.PartitionData))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.PartitionData {
					// Index
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.PartitionData[i].Index); err != nil {
							return nil, err
						}
					}
					// Records
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableBytes(elemW, structItem.PartitionData[i].Records); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableBytes(elemW, structItem.PartitionData[i].Records); err != nil {
								return nil, err
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
		items := make([]interface{}, len(m.TopicData))
		for i := range m.TopicData {
			items[i] = m.TopicData[i]
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

// Read reads a ProduceRequest from an io.Reader for the given version.
func (m *ProduceRequest) Read(r io.Reader, version int16) error {
	if version < 3 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// TransactionalId
	if version >= 3 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.TransactionalId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.TransactionalId = val
		}
	}
	// Acks
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.Acks = val
	}
	// TimeoutMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.TimeoutMs = val
	}
	// TopicData
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ProduceRequestTopicProduceData
			elemR := bytes.NewReader(data)
			// Name
			if version >= 0 && version <= 12 {
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
			if version >= 13 && version <= 999 {
				val, err := protocol.ReadUUID(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.TopicId = val
			}
			// PartitionData
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
				var tempElem ProduceRequestTopicProduceData
				// Name
				if version >= 0 && version <= 12 {
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
				if version >= 13 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.TopicId = val
				}
				// PartitionData
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem ProduceRequestPartitionProduceData
						elemR := bytes.NewReader(data)
						// Index
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Index = val
						}
						// Records
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableBytes(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Records = val
							} else {
								val, err := protocol.ReadNullableBytes(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Records = val
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
							var tempElem ProduceRequestPartitionProduceData
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Index = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								}
							}
							// Index
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Index); err != nil {
									return err
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableBytes(elemW, tempElem.Records); err != nil {
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
						tempElem.PartitionData = make([]ProduceRequestPartitionProduceData, len(decoded))
						for i, item := range decoded {
							tempElem.PartitionData[i] = item.(ProduceRequestPartitionProduceData)
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
							var tempElem ProduceRequestPartitionProduceData
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Index = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								}
							}
							// Index
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Index); err != nil {
									return err
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableBytes(elemW, tempElem.Records); err != nil {
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
						tempElem.PartitionData = make([]ProduceRequestPartitionProduceData, len(decoded))
						for i, item := range decoded {
							tempElem.PartitionData[i] = item.(ProduceRequestPartitionProduceData)
						}
					}
				}
				// Name
				if version >= 0 && version <= 12 {
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
				if version >= 13 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
						return err
					}
				}
				// PartitionData
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.PartitionData) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.PartitionData))); err != nil {
							return err
						}
					}
					for i := range tempElem.PartitionData {
						// Index
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.PartitionData[i].Index); err != nil {
								return err
							}
						}
						// Records
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableBytes(elemW, tempElem.PartitionData[i].Records); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableBytes(elemW, tempElem.PartitionData[i].Records); err != nil {
									return err
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
			m.TopicData = make([]ProduceRequestTopicProduceData, len(decoded))
			for i, item := range decoded {
				m.TopicData[i] = item.(ProduceRequestTopicProduceData)
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
				var tempElem ProduceRequestTopicProduceData
				// Name
				if version >= 0 && version <= 12 {
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
				if version >= 13 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.TopicId = val
				}
				// PartitionData
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem ProduceRequestPartitionProduceData
						elemR := bytes.NewReader(data)
						// Index
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Index = val
						}
						// Records
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableBytes(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Records = val
							} else {
								val, err := protocol.ReadNullableBytes(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Records = val
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
							var tempElem ProduceRequestPartitionProduceData
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Index = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								}
							}
							// Index
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Index); err != nil {
									return err
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableBytes(elemW, tempElem.Records); err != nil {
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
						tempElem.PartitionData = make([]ProduceRequestPartitionProduceData, len(decoded))
						for i, item := range decoded {
							tempElem.PartitionData[i] = item.(ProduceRequestPartitionProduceData)
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
							var tempElem ProduceRequestPartitionProduceData
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Index = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								}
							}
							// Index
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Index); err != nil {
									return err
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableBytes(elemW, tempElem.Records); err != nil {
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
						tempElem.PartitionData = make([]ProduceRequestPartitionProduceData, len(decoded))
						for i, item := range decoded {
							tempElem.PartitionData[i] = item.(ProduceRequestPartitionProduceData)
						}
					}
				}
				// Name
				if version >= 0 && version <= 12 {
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
				if version >= 13 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
						return err
					}
				}
				// PartitionData
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.PartitionData) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.PartitionData))); err != nil {
							return err
						}
					}
					for i := range tempElem.PartitionData {
						// Index
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.PartitionData[i].Index); err != nil {
								return err
							}
						}
						// Records
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableBytes(elemW, tempElem.PartitionData[i].Records); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableBytes(elemW, tempElem.PartitionData[i].Records); err != nil {
									return err
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
			m.TopicData = make([]ProduceRequestTopicProduceData, len(decoded))
			for i, item := range decoded {
				m.TopicData[i] = item.(ProduceRequestTopicProduceData)
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

// ProduceRequestTopicProduceData represents Each topic to produce to..
type ProduceRequestTopicProduceData struct {
	// The topic name.
	Name string `json:"name" versions:"0-12"`
	// The unique topic ID
	TopicId uuid.UUID `json:"topicid" versions:"13-999"`
	// Each partition to produce to.
	PartitionData []ProduceRequestPartitionProduceData `json:"partitiondata" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ProduceRequestTopicProduceData.
func (m *ProduceRequestTopicProduceData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ProduceRequestTopicProduceData.
func (m *ProduceRequestTopicProduceData) readTaggedFields(r io.Reader, version int16) error {
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

// ProduceRequestPartitionProduceData represents Each partition to produce to..
type ProduceRequestPartitionProduceData struct {
	// The partition index.
	Index int32 `json:"index" versions:"0-999"`
	// The record data to be produced.
	Records *[]byte `json:"records" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ProduceRequestPartitionProduceData.
func (m *ProduceRequestPartitionProduceData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ProduceRequestPartitionProduceData.
func (m *ProduceRequestPartitionProduceData) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for ProduceRequest.
func (m *ProduceRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ProduceRequest.
func (m *ProduceRequest) readTaggedFields(r io.Reader, version int16) error {
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
