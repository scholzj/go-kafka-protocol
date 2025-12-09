package createtopics

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	CreateTopicsResponseApiKey        = 19
	CreateTopicsResponseHeaderVersion = 1
)

// CreateTopicsResponse represents a response message.
type CreateTopicsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"2-999"`
	// Results for each topic we tried to create.
	Topics []CreateTopicsResponseCreatableTopicResult `json:"topics" versions:"0-999"`
}

// Encode encodes a CreateTopicsResponse to a byte slice for the given version.
func (m *CreateTopicsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a CreateTopicsResponse from a byte slice for the given version.
func (m *CreateTopicsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a CreateTopicsResponse to an io.Writer for the given version.
func (m *CreateTopicsResponse) Write(w io.Writer, version int16) error {
	if version < 2 || version > 7 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 5 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 2 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
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
			structItem, ok := item.(CreateTopicsResponseCreatableTopicResult)
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
			// TopicId
			if version >= 7 && version <= 999 {
				if err := protocol.WriteUUID(elemW, structItem.TopicId); err != nil {
					return nil, err
				}
			}
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ErrorCode); err != nil {
					return nil, err
				}
			}
			// ErrorMessage
			if version >= 1 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.ErrorMessage); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.ErrorMessage); err != nil {
						return nil, err
					}
				}
			}
			// TopicConfigErrorCode
			if version >= 5 && version <= 999 {
			}
			// NumPartitions
			if version >= 5 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.NumPartitions); err != nil {
					return nil, err
				}
			}
			// ReplicationFactor
			if version >= 5 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ReplicationFactor); err != nil {
					return nil, err
				}
			}
			// Configs
			if version >= 5 && version <= 999 {
				if structItem.Configs == nil {
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
						length := uint32(len(structItem.Configs) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return nil, err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(structItem.Configs))); err != nil {
							return nil, err
						}
					}
					for i := range structItem.Configs {
						// Name
						if version >= 5 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, structItem.Configs[i].Name); err != nil {
									return nil, err
								}
							} else {
								if err := protocol.WriteString(elemW, structItem.Configs[i].Name); err != nil {
									return nil, err
								}
							}
						}
						// Value
						if version >= 5 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, structItem.Configs[i].Value); err != nil {
									return nil, err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, structItem.Configs[i].Value); err != nil {
									return nil, err
								}
							}
						}
						// ReadOnly
						if version >= 5 && version <= 999 {
							if err := protocol.WriteBool(elemW, structItem.Configs[i].ReadOnly); err != nil {
								return nil, err
							}
						}
						// ConfigSource
						if version >= 5 && version <= 999 {
							if err := protocol.WriteInt8(elemW, structItem.Configs[i].ConfigSource); err != nil {
								return nil, err
							}
						}
						// IsSensitive
						if version >= 5 && version <= 999 {
							if err := protocol.WriteBool(elemW, structItem.Configs[i].IsSensitive); err != nil {
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

// Read reads a CreateTopicsResponse from an io.Reader for the given version.
func (m *CreateTopicsResponse) Read(r io.Reader, version int16) error {
	if version < 2 || version > 7 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 5 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 2 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem CreateTopicsResponseCreatableTopicResult
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
			// TopicId
			if version >= 7 && version <= 999 {
				val, err := protocol.ReadUUID(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.TopicId = val
			}
			// ErrorCode
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ErrorCode = val
			}
			// ErrorMessage
			if version >= 1 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ErrorMessage = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ErrorMessage = val
				}
			}
			// TopicConfigErrorCode
			if version >= 5 && version <= 999 {
			}
			// NumPartitions
			if version >= 5 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.NumPartitions = val
			}
			// ReplicationFactor
			if version >= 5 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ReplicationFactor = val
			}
			// Configs
			if version >= 5 && version <= 999 {
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
				var tempElem CreateTopicsResponseCreatableTopicResult
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
				// TopicId
				if version >= 7 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.TopicId = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// ErrorMessage
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					}
				}
				// TopicConfigErrorCode
				if version >= 5 && version <= 999 {
				}
				// NumPartitions
				if version >= 5 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.NumPartitions = val
				}
				// ReplicationFactor
				if version >= 5 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ReplicationFactor = val
				}
				// Configs
				if version >= 5 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem CreateTopicsResponseCreatableTopicConfigs
						elemR := bytes.NewReader(data)
						// Name
						if version >= 5 && version <= 999 {
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
						// Value
						if version >= 5 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Value = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Value = val
							}
						}
						// ReadOnly
						if version >= 5 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ReadOnly = val
						}
						// ConfigSource
						if version >= 5 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ConfigSource = val
						}
						// IsSensitive
						if version >= 5 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.IsSensitive = val
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
							tempElem.Configs = nil
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
							var tempElem CreateTopicsResponseCreatableTopicConfigs
							// Name
							if version >= 5 && version <= 999 {
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
							// Value
							if version >= 5 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								}
							}
							// ReadOnly
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.ReadOnly = val
							}
							// ConfigSource
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigSource = val
							}
							// IsSensitive
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsSensitive = val
							}
							// Name
							if version >= 5 && version <= 999 {
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
							// Value
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								}
							}
							// ReadOnly
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.ReadOnly); err != nil {
									return err
								}
							}
							// ConfigSource
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigSource); err != nil {
									return err
								}
							}
							// IsSensitive
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsSensitive); err != nil {
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
						tempElem.Configs = make([]CreateTopicsResponseCreatableTopicConfigs, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(CreateTopicsResponseCreatableTopicConfigs)
						}
					} else {
						length, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						if length == -1 {
							tempElem.Configs = nil
							return nil
						}
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem CreateTopicsResponseCreatableTopicConfigs
							// Name
							if version >= 5 && version <= 999 {
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
							// Value
							if version >= 5 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								}
							}
							// ReadOnly
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.ReadOnly = val
							}
							// ConfigSource
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigSource = val
							}
							// IsSensitive
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsSensitive = val
							}
							// Name
							if version >= 5 && version <= 999 {
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
							// Value
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								}
							}
							// ReadOnly
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.ReadOnly); err != nil {
									return err
								}
							}
							// ConfigSource
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigSource); err != nil {
									return err
								}
							}
							// IsSensitive
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsSensitive); err != nil {
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
						tempElem.Configs = make([]CreateTopicsResponseCreatableTopicConfigs, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(CreateTopicsResponseCreatableTopicConfigs)
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
				// TopicId
				if version >= 7 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
						return err
					}
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// ErrorMessage
				if version >= 1 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					}
				}
				// TopicConfigErrorCode
				if version >= 5 && version <= 999 {
				}
				// NumPartitions
				if version >= 5 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.NumPartitions); err != nil {
						return err
					}
				}
				// ReplicationFactor
				if version >= 5 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
						return err
					}
				}
				// Configs
				if version >= 5 && version <= 999 {
					if tempElem.Configs == nil {
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
							length := uint32(len(tempElem.Configs) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(tempElem.Configs))); err != nil {
								return err
							}
						}
						for i := range tempElem.Configs {
							// Name
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Configs[i].Name); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Configs[i].Name); err != nil {
										return err
									}
								}
							}
							// Value
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Configs[i].Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Configs[i].Value); err != nil {
										return err
									}
								}
							}
							// ReadOnly
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.Configs[i].ReadOnly); err != nil {
									return err
								}
							}
							// ConfigSource
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.Configs[i].ConfigSource); err != nil {
									return err
								}
							}
							// IsSensitive
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.Configs[i].IsSensitive); err != nil {
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
			m.Topics = make([]CreateTopicsResponseCreatableTopicResult, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(CreateTopicsResponseCreatableTopicResult)
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
				var tempElem CreateTopicsResponseCreatableTopicResult
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
				// TopicId
				if version >= 7 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.TopicId = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// ErrorMessage
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					}
				}
				// TopicConfigErrorCode
				if version >= 5 && version <= 999 {
				}
				// NumPartitions
				if version >= 5 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.NumPartitions = val
				}
				// ReplicationFactor
				if version >= 5 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ReplicationFactor = val
				}
				// Configs
				if version >= 5 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem CreateTopicsResponseCreatableTopicConfigs
						elemR := bytes.NewReader(data)
						// Name
						if version >= 5 && version <= 999 {
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
						// Value
						if version >= 5 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Value = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Value = val
							}
						}
						// ReadOnly
						if version >= 5 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ReadOnly = val
						}
						// ConfigSource
						if version >= 5 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ConfigSource = val
						}
						// IsSensitive
						if version >= 5 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.IsSensitive = val
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
							tempElem.Configs = nil
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
							var tempElem CreateTopicsResponseCreatableTopicConfigs
							// Name
							if version >= 5 && version <= 999 {
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
							// Value
							if version >= 5 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								}
							}
							// ReadOnly
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.ReadOnly = val
							}
							// ConfigSource
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigSource = val
							}
							// IsSensitive
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsSensitive = val
							}
							// Name
							if version >= 5 && version <= 999 {
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
							// Value
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								}
							}
							// ReadOnly
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.ReadOnly); err != nil {
									return err
								}
							}
							// ConfigSource
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigSource); err != nil {
									return err
								}
							}
							// IsSensitive
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsSensitive); err != nil {
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
						tempElem.Configs = make([]CreateTopicsResponseCreatableTopicConfigs, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(CreateTopicsResponseCreatableTopicConfigs)
						}
					} else {
						length, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						if length == -1 {
							tempElem.Configs = nil
							return nil
						}
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem CreateTopicsResponseCreatableTopicConfigs
							// Name
							if version >= 5 && version <= 999 {
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
							// Value
							if version >= 5 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								}
							}
							// ReadOnly
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.ReadOnly = val
							}
							// ConfigSource
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigSource = val
							}
							// IsSensitive
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsSensitive = val
							}
							// Name
							if version >= 5 && version <= 999 {
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
							// Value
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								}
							}
							// ReadOnly
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.ReadOnly); err != nil {
									return err
								}
							}
							// ConfigSource
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigSource); err != nil {
									return err
								}
							}
							// IsSensitive
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsSensitive); err != nil {
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
						tempElem.Configs = make([]CreateTopicsResponseCreatableTopicConfigs, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(CreateTopicsResponseCreatableTopicConfigs)
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
				// TopicId
				if version >= 7 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
						return err
					}
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// ErrorMessage
				if version >= 1 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					}
				}
				// TopicConfigErrorCode
				if version >= 5 && version <= 999 {
				}
				// NumPartitions
				if version >= 5 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.NumPartitions); err != nil {
						return err
					}
				}
				// ReplicationFactor
				if version >= 5 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
						return err
					}
				}
				// Configs
				if version >= 5 && version <= 999 {
					if tempElem.Configs == nil {
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
							length := uint32(len(tempElem.Configs) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(tempElem.Configs))); err != nil {
								return err
							}
						}
						for i := range tempElem.Configs {
							// Name
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Configs[i].Name); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Configs[i].Name); err != nil {
										return err
									}
								}
							}
							// Value
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Configs[i].Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Configs[i].Value); err != nil {
										return err
									}
								}
							}
							// ReadOnly
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.Configs[i].ReadOnly); err != nil {
									return err
								}
							}
							// ConfigSource
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.Configs[i].ConfigSource); err != nil {
									return err
								}
							}
							// IsSensitive
							if version >= 5 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.Configs[i].IsSensitive); err != nil {
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
			m.Topics = make([]CreateTopicsResponseCreatableTopicResult, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(CreateTopicsResponseCreatableTopicResult)
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

// CreateTopicsResponseCreatableTopicResult represents Results for each topic we tried to create..
type CreateTopicsResponseCreatableTopicResult struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The unique topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"7-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"1-999"`
	// Optional topic config error returned if configs are not returned in the response.
	TopicConfigErrorCode int16 `json:"topicconfigerrorcode" versions:"5-999" tag:"0"`
	// Number of partitions of the topic.
	NumPartitions int32 `json:"numpartitions" versions:"5-999"`
	// Replication factor of the topic.
	ReplicationFactor int16 `json:"replicationfactor" versions:"5-999"`
	// Configuration of the topic.
	Configs []CreateTopicsResponseCreatableTopicConfigs `json:"configs" versions:"5-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for CreateTopicsResponseCreatableTopicResult.
func (m *CreateTopicsResponseCreatableTopicResult) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	// TopicConfigErrorCode (tag 0)
	if version >= 5 {
		if m.TopicConfigErrorCode != 0 {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(0)); err != nil {
				return err
			}
			if err := protocol.WriteInt16(&taggedFieldsBuf, m.TopicConfigErrorCode); err != nil {
				return err
			}
			taggedFieldsCount++
		}
	}

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

// readTaggedFields reads tagged fields for CreateTopicsResponseCreatableTopicResult.
func (m *CreateTopicsResponseCreatableTopicResult) readTaggedFields(r io.Reader, version int16) error {
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
		case 0: // TopicConfigErrorCode
			if version >= 5 {
				val, err := protocol.ReadInt16(r)
				if err != nil {
					return err
				}
				m.TopicConfigErrorCode = val
			}
		default:
			// Unknown tag, skip it
		}
	}

	return nil
}

// CreateTopicsResponseCreatableTopicConfigs represents Configuration of the topic..
type CreateTopicsResponseCreatableTopicConfigs struct {
	// The configuration name.
	Name string `json:"name" versions:"5-999"`
	// The configuration value.
	Value *string `json:"value" versions:"5-999"`
	// True if the configuration is read-only.
	ReadOnly bool `json:"readonly" versions:"5-999"`
	// The configuration source.
	ConfigSource int8 `json:"configsource" versions:"5-999"`
	// True if this configuration is sensitive.
	IsSensitive bool `json:"issensitive" versions:"5-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for CreateTopicsResponseCreatableTopicConfigs.
func (m *CreateTopicsResponseCreatableTopicConfigs) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreateTopicsResponseCreatableTopicConfigs.
func (m *CreateTopicsResponseCreatableTopicConfigs) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for CreateTopicsResponse.
func (m *CreateTopicsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreateTopicsResponse.
func (m *CreateTopicsResponse) readTaggedFields(r io.Reader, version int16) error {
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
