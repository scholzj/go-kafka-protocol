package createtopics

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	CreateTopicsRequestApiKey        = 19
	CreateTopicsRequestHeaderVersion = 1
)

// CreateTopicsRequest represents a request message.
type CreateTopicsRequest struct {
	// The topics to create.
	Topics []CreateTopicsRequestCreatableTopic `json:"topics" versions:"0-999"`
	// How long to wait in milliseconds before timing out the request.
	TimeoutMs int32 `json:"timeoutms" versions:"0-999"`
	// If true, check that the topics can be created as specified, but don't create anything.
	ValidateOnly bool `json:"validateonly" versions:"1-999"`
}

// Encode encodes a CreateTopicsRequest to a byte slice for the given version.
func (m *CreateTopicsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a CreateTopicsRequest from a byte slice for the given version.
func (m *CreateTopicsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a CreateTopicsRequest to an io.Writer for the given version.
func (m *CreateTopicsRequest) Write(w io.Writer, version int16) error {
	if version < 2 || version > 7 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 5 {
		isFlexible = true
	}

	// Topics
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(CreateTopicsRequestCreatableTopic)
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
			// NumPartitions
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.NumPartitions); err != nil {
					return nil, err
				}
			}
			// ReplicationFactor
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ReplicationFactor); err != nil {
					return nil, err
				}
			}
			// Assignments
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Assignments) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Assignments))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Assignments {
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Assignments[i].PartitionIndex); err != nil {
							return nil, err
						}
					}
					// BrokerIds
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Assignments[i].BrokerIds); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Assignments[i].BrokerIds); err != nil {
								return nil, err
							}
						}
					}
				}
			}
			// Configs
			if version >= 0 && version <= 999 {
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
					if version >= 0 && version <= 999 {
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
					if version >= 0 && version <= 999 {
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
	// timeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TimeoutMs); err != nil {
			return err
		}
	}
	// validateOnly
	if version >= 1 && version <= 999 {
		if err := protocol.WriteBool(w, m.ValidateOnly); err != nil {
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

// Read reads a CreateTopicsRequest from an io.Reader for the given version.
func (m *CreateTopicsRequest) Read(r io.Reader, version int16) error {
	if version < 2 || version > 7 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 5 {
		isFlexible = true
	}

	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem CreateTopicsRequestCreatableTopic
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
			// NumPartitions
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.NumPartitions = val
			}
			// ReplicationFactor
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ReplicationFactor = val
			}
			// Assignments
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// Configs
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
				var tempElem CreateTopicsRequestCreatableTopic
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
				// NumPartitions
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.NumPartitions = val
				}
				// ReplicationFactor
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ReplicationFactor = val
				}
				// Assignments
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem CreateTopicsRequestCreatableReplicaAssignment
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// BrokerIds
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.BrokerIds = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.BrokerIds = val
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
							var tempElem CreateTopicsRequestCreatableReplicaAssignment
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.BrokerIds); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.BrokerIds); err != nil {
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
						tempElem.Assignments = make([]CreateTopicsRequestCreatableReplicaAssignment, len(decoded))
						for i, item := range decoded {
							tempElem.Assignments[i] = item.(CreateTopicsRequestCreatableReplicaAssignment)
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
							var tempElem CreateTopicsRequestCreatableReplicaAssignment
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.BrokerIds); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.BrokerIds); err != nil {
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
						tempElem.Assignments = make([]CreateTopicsRequestCreatableReplicaAssignment, len(decoded))
						for i, item := range decoded {
							tempElem.Assignments[i] = item.(CreateTopicsRequestCreatableReplicaAssignment)
						}
					}
				}
				// Configs
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem CreateTopicsRequestCreatableTopicConfig
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
						// Value
						if version >= 0 && version <= 999 {
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
							var tempElem CreateTopicsRequestCreatableTopicConfig
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
							// Value
							if version >= 0 && version <= 999 {
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
							// Value
							if version >= 0 && version <= 999 {
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
						tempElem.Configs = make([]CreateTopicsRequestCreatableTopicConfig, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(CreateTopicsRequestCreatableTopicConfig)
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
							var tempElem CreateTopicsRequestCreatableTopicConfig
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
							// Value
							if version >= 0 && version <= 999 {
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
							// Value
							if version >= 0 && version <= 999 {
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
						tempElem.Configs = make([]CreateTopicsRequestCreatableTopicConfig, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(CreateTopicsRequestCreatableTopicConfig)
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
				// NumPartitions
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.NumPartitions); err != nil {
						return err
					}
				}
				// ReplicationFactor
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
						return err
					}
				}
				// Assignments
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Assignments) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Assignments))); err != nil {
							return err
						}
					}
					for i := range tempElem.Assignments {
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Assignments[i].PartitionIndex); err != nil {
								return err
							}
						}
						// BrokerIds
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Assignments[i].BrokerIds); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Assignments[i].BrokerIds); err != nil {
									return err
								}
							}
						}
					}
				}
				// Configs
				if version >= 0 && version <= 999 {
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
						if version >= 0 && version <= 999 {
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
						if version >= 0 && version <= 999 {
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
			m.Topics = make([]CreateTopicsRequestCreatableTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(CreateTopicsRequestCreatableTopic)
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
				var tempElem CreateTopicsRequestCreatableTopic
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
				// NumPartitions
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.NumPartitions = val
				}
				// ReplicationFactor
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ReplicationFactor = val
				}
				// Assignments
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem CreateTopicsRequestCreatableReplicaAssignment
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// BrokerIds
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.BrokerIds = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.BrokerIds = val
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
							var tempElem CreateTopicsRequestCreatableReplicaAssignment
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.BrokerIds); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.BrokerIds); err != nil {
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
						tempElem.Assignments = make([]CreateTopicsRequestCreatableReplicaAssignment, len(decoded))
						for i, item := range decoded {
							tempElem.Assignments[i] = item.(CreateTopicsRequestCreatableReplicaAssignment)
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
							var tempElem CreateTopicsRequestCreatableReplicaAssignment
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.BrokerIds); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.BrokerIds); err != nil {
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
						tempElem.Assignments = make([]CreateTopicsRequestCreatableReplicaAssignment, len(decoded))
						for i, item := range decoded {
							tempElem.Assignments[i] = item.(CreateTopicsRequestCreatableReplicaAssignment)
						}
					}
				}
				// Configs
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem CreateTopicsRequestCreatableTopicConfig
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
						// Value
						if version >= 0 && version <= 999 {
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
							var tempElem CreateTopicsRequestCreatableTopicConfig
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
							// Value
							if version >= 0 && version <= 999 {
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
							// Value
							if version >= 0 && version <= 999 {
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
						tempElem.Configs = make([]CreateTopicsRequestCreatableTopicConfig, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(CreateTopicsRequestCreatableTopicConfig)
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
							var tempElem CreateTopicsRequestCreatableTopicConfig
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
							// Value
							if version >= 0 && version <= 999 {
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
							// Value
							if version >= 0 && version <= 999 {
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
						tempElem.Configs = make([]CreateTopicsRequestCreatableTopicConfig, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(CreateTopicsRequestCreatableTopicConfig)
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
				// NumPartitions
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.NumPartitions); err != nil {
						return err
					}
				}
				// ReplicationFactor
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
						return err
					}
				}
				// Assignments
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Assignments) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Assignments))); err != nil {
							return err
						}
					}
					for i := range tempElem.Assignments {
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Assignments[i].PartitionIndex); err != nil {
								return err
							}
						}
						// BrokerIds
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Assignments[i].BrokerIds); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Assignments[i].BrokerIds); err != nil {
									return err
								}
							}
						}
					}
				}
				// Configs
				if version >= 0 && version <= 999 {
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
						if version >= 0 && version <= 999 {
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
						if version >= 0 && version <= 999 {
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
			m.Topics = make([]CreateTopicsRequestCreatableTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(CreateTopicsRequestCreatableTopic)
			}
		}
	}
	// timeoutMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.TimeoutMs = val
	}
	// validateOnly
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.ValidateOnly = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// CreateTopicsRequestCreatableTopic represents The topics to create..
type CreateTopicsRequestCreatableTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
	NumPartitions int32 `json:"numpartitions" versions:"0-999"`
	// The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
	ReplicationFactor int16 `json:"replicationfactor" versions:"0-999"`
	// The manual partition assignment, or the empty array if we are using automatic assignment.
	Assignments []CreateTopicsRequestCreatableReplicaAssignment `json:"assignments" versions:"0-999"`
	// The custom topic configurations to set.
	Configs []CreateTopicsRequestCreatableTopicConfig `json:"configs" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for CreateTopicsRequestCreatableTopic.
func (m *CreateTopicsRequestCreatableTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreateTopicsRequestCreatableTopic.
func (m *CreateTopicsRequestCreatableTopic) readTaggedFields(r io.Reader, version int16) error {
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

// CreateTopicsRequestCreatableReplicaAssignment represents The manual partition assignment, or the empty array if we are using automatic assignment..
type CreateTopicsRequestCreatableReplicaAssignment struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The brokers to place the partition on.
	BrokerIds []int32 `json:"brokerids" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for CreateTopicsRequestCreatableReplicaAssignment.
func (m *CreateTopicsRequestCreatableReplicaAssignment) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreateTopicsRequestCreatableReplicaAssignment.
func (m *CreateTopicsRequestCreatableReplicaAssignment) readTaggedFields(r io.Reader, version int16) error {
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

// CreateTopicsRequestCreatableTopicConfig represents The custom topic configurations to set..
type CreateTopicsRequestCreatableTopicConfig struct {
	// The configuration name.
	Name string `json:"name" versions:"0-999"`
	// The configuration value.
	Value *string `json:"value" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for CreateTopicsRequestCreatableTopicConfig.
func (m *CreateTopicsRequestCreatableTopicConfig) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreateTopicsRequestCreatableTopicConfig.
func (m *CreateTopicsRequestCreatableTopicConfig) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for CreateTopicsRequest.
func (m *CreateTopicsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreateTopicsRequest.
func (m *CreateTopicsRequest) readTaggedFields(r io.Reader, version int16) error {
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
