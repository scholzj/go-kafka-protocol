package alterpartition

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AlterPartitionRequestApiKey        = 56
	AlterPartitionRequestHeaderVersion = 1
)

// AlterPartitionRequest represents a request message.
type AlterPartitionRequest struct {
	// The ID of the requesting broker.
	BrokerId int32 `json:"brokerid" versions:"0-999"`
	// The epoch of the requesting broker.
	BrokerEpoch int64 `json:"brokerepoch" versions:"0-999"`
	// The topics to alter ISRs for.
	Topics []AlterPartitionRequestTopicData `json:"topics" versions:"0-999"`
}

// Encode encodes a AlterPartitionRequest to a byte slice for the given version.
func (m *AlterPartitionRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AlterPartitionRequest from a byte slice for the given version.
func (m *AlterPartitionRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AlterPartitionRequest to an io.Writer for the given version.
func (m *AlterPartitionRequest) Write(w io.Writer, version int16) error {
	if version < 2 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// BrokerId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.BrokerId); err != nil {
			return err
		}
	}
	// BrokerEpoch
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.BrokerEpoch); err != nil {
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
			structItem, ok := item.(AlterPartitionRequestTopicData)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// TopicId
			if version >= 2 && version <= 999 {
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
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].PartitionIndex); err != nil {
							return nil, err
						}
					}
					// LeaderEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].LeaderEpoch); err != nil {
							return nil, err
						}
					}
					// NewIsr
					if version >= 0 && version <= 2 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions[i].NewIsr); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Partitions[i].NewIsr); err != nil {
								return nil, err
							}
						}
					}
					// NewIsrWithEpochs
					if version >= 3 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Partitions[i].NewIsrWithEpochs) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions[i].NewIsrWithEpochs))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Partitions[i].NewIsrWithEpochs {
							// BrokerId
							if version >= 3 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Partitions[i].NewIsrWithEpochs[i].BrokerId); err != nil {
									return nil, err
								}
							}
							// BrokerEpoch
							if version >= 3 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].NewIsrWithEpochs[i].BrokerEpoch); err != nil {
									return nil, err
								}
							}
						}
					}
					// LeaderRecoveryState
					if version >= 1 && version <= 999 {
						if err := protocol.WriteInt8(elemW, structItem.Partitions[i].LeaderRecoveryState); err != nil {
							return nil, err
						}
					}
					// PartitionEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].PartitionEpoch); err != nil {
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

// Read reads a AlterPartitionRequest from an io.Reader for the given version.
func (m *AlterPartitionRequest) Read(r io.Reader, version int16) error {
	if version < 2 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// BrokerId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.BrokerId = val
	}
	// BrokerEpoch
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.BrokerEpoch = val
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem AlterPartitionRequestTopicData
			elemR := bytes.NewReader(data)
			// TopicId
			if version >= 2 && version <= 999 {
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
				var tempElem AlterPartitionRequestTopicData
				// TopicId
				if version >= 2 && version <= 999 {
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
						var elem AlterPartitionRequestPartitionData
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderEpoch = val
						}
						// NewIsr
						if version >= 0 && version <= 2 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.NewIsr = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.NewIsr = val
							}
						}
						// NewIsrWithEpochs
						if version >= 3 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// LeaderRecoveryState
						if version >= 1 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderRecoveryState = val
						}
						// PartitionEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionEpoch = val
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
							var tempElem AlterPartitionRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// NewIsr
							if version >= 0 && version <= 2 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.NewIsr = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.NewIsr = val
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AlterPartitionRequestBrokerState
									elemR := bytes.NewReader(data)
									// BrokerId
									if version >= 3 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BrokerId = val
									}
									// BrokerEpoch
									if version >= 3 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BrokerEpoch = val
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
										var tempElem AlterPartitionRequestBrokerState
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.BrokerEpoch = val
										}
										// BrokerId
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BrokerId); err != nil {
												return err
											}
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.BrokerEpoch); err != nil {
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
									tempElem.NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, len(decoded))
									for i, item := range decoded {
										tempElem.NewIsrWithEpochs[i] = item.(AlterPartitionRequestBrokerState)
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
										var tempElem AlterPartitionRequestBrokerState
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.BrokerEpoch = val
										}
										// BrokerId
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BrokerId); err != nil {
												return err
											}
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.BrokerEpoch); err != nil {
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
									tempElem.NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, len(decoded))
									for i, item := range decoded {
										tempElem.NewIsrWithEpochs[i] = item.(AlterPartitionRequestBrokerState)
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.LeaderRecoveryState = val
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionEpoch = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// NewIsr
							if version >= 0 && version <= 2 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.NewIsr); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.NewIsr); err != nil {
										return err
									}
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.NewIsrWithEpochs) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.NewIsrWithEpochs))); err != nil {
										return err
									}
								}
								for i := range tempElem.NewIsrWithEpochs {
									// BrokerId
									if version >= 3 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.NewIsrWithEpochs[i].BrokerId); err != nil {
											return err
										}
									}
									// BrokerEpoch
									if version >= 3 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.NewIsrWithEpochs[i].BrokerEpoch); err != nil {
											return err
										}
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.LeaderRecoveryState); err != nil {
									return err
								}
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionEpoch); err != nil {
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
						tempElem.Partitions = make([]AlterPartitionRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(AlterPartitionRequestPartitionData)
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
							var tempElem AlterPartitionRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// NewIsr
							if version >= 0 && version <= 2 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.NewIsr = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.NewIsr = val
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AlterPartitionRequestBrokerState
									elemR := bytes.NewReader(data)
									// BrokerId
									if version >= 3 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BrokerId = val
									}
									// BrokerEpoch
									if version >= 3 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BrokerEpoch = val
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
										var tempElem AlterPartitionRequestBrokerState
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.BrokerEpoch = val
										}
										// BrokerId
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BrokerId); err != nil {
												return err
											}
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.BrokerEpoch); err != nil {
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
									tempElem.NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, len(decoded))
									for i, item := range decoded {
										tempElem.NewIsrWithEpochs[i] = item.(AlterPartitionRequestBrokerState)
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
										var tempElem AlterPartitionRequestBrokerState
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.BrokerEpoch = val
										}
										// BrokerId
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BrokerId); err != nil {
												return err
											}
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.BrokerEpoch); err != nil {
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
									tempElem.NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, len(decoded))
									for i, item := range decoded {
										tempElem.NewIsrWithEpochs[i] = item.(AlterPartitionRequestBrokerState)
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.LeaderRecoveryState = val
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionEpoch = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// NewIsr
							if version >= 0 && version <= 2 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.NewIsr); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.NewIsr); err != nil {
										return err
									}
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.NewIsrWithEpochs) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.NewIsrWithEpochs))); err != nil {
										return err
									}
								}
								for i := range tempElem.NewIsrWithEpochs {
									// BrokerId
									if version >= 3 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.NewIsrWithEpochs[i].BrokerId); err != nil {
											return err
										}
									}
									// BrokerEpoch
									if version >= 3 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.NewIsrWithEpochs[i].BrokerEpoch); err != nil {
											return err
										}
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.LeaderRecoveryState); err != nil {
									return err
								}
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionEpoch); err != nil {
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
						tempElem.Partitions = make([]AlterPartitionRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(AlterPartitionRequestPartitionData)
						}
					}
				}
				// TopicId
				if version >= 2 && version <= 999 {
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
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
							}
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
								return err
							}
						}
						// NewIsr
						if version >= 0 && version <= 2 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].NewIsr); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].NewIsr); err != nil {
									return err
								}
							}
						}
						// NewIsrWithEpochs
						if version >= 3 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].NewIsrWithEpochs) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].NewIsrWithEpochs))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].NewIsrWithEpochs {
								// BrokerId
								if version >= 3 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].NewIsrWithEpochs[i].BrokerId); err != nil {
										return err
									}
								}
								// BrokerEpoch
								if version >= 3 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].NewIsrWithEpochs[i].BrokerEpoch); err != nil {
										return err
									}
								}
							}
						}
						// LeaderRecoveryState
						if version >= 1 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.Partitions[i].LeaderRecoveryState); err != nil {
								return err
							}
						}
						// PartitionEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionEpoch); err != nil {
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
			m.Topics = make([]AlterPartitionRequestTopicData, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(AlterPartitionRequestTopicData)
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
				var tempElem AlterPartitionRequestTopicData
				// TopicId
				if version >= 2 && version <= 999 {
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
						var elem AlterPartitionRequestPartitionData
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderEpoch = val
						}
						// NewIsr
						if version >= 0 && version <= 2 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.NewIsr = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.NewIsr = val
							}
						}
						// NewIsrWithEpochs
						if version >= 3 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// LeaderRecoveryState
						if version >= 1 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderRecoveryState = val
						}
						// PartitionEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionEpoch = val
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
							var tempElem AlterPartitionRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// NewIsr
							if version >= 0 && version <= 2 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.NewIsr = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.NewIsr = val
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AlterPartitionRequestBrokerState
									elemR := bytes.NewReader(data)
									// BrokerId
									if version >= 3 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BrokerId = val
									}
									// BrokerEpoch
									if version >= 3 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BrokerEpoch = val
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
										var tempElem AlterPartitionRequestBrokerState
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.BrokerEpoch = val
										}
										// BrokerId
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BrokerId); err != nil {
												return err
											}
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.BrokerEpoch); err != nil {
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
									tempElem.NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, len(decoded))
									for i, item := range decoded {
										tempElem.NewIsrWithEpochs[i] = item.(AlterPartitionRequestBrokerState)
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
										var tempElem AlterPartitionRequestBrokerState
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.BrokerEpoch = val
										}
										// BrokerId
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BrokerId); err != nil {
												return err
											}
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.BrokerEpoch); err != nil {
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
									tempElem.NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, len(decoded))
									for i, item := range decoded {
										tempElem.NewIsrWithEpochs[i] = item.(AlterPartitionRequestBrokerState)
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.LeaderRecoveryState = val
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionEpoch = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// NewIsr
							if version >= 0 && version <= 2 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.NewIsr); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.NewIsr); err != nil {
										return err
									}
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.NewIsrWithEpochs) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.NewIsrWithEpochs))); err != nil {
										return err
									}
								}
								for i := range tempElem.NewIsrWithEpochs {
									// BrokerId
									if version >= 3 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.NewIsrWithEpochs[i].BrokerId); err != nil {
											return err
										}
									}
									// BrokerEpoch
									if version >= 3 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.NewIsrWithEpochs[i].BrokerEpoch); err != nil {
											return err
										}
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.LeaderRecoveryState); err != nil {
									return err
								}
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionEpoch); err != nil {
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
						tempElem.Partitions = make([]AlterPartitionRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(AlterPartitionRequestPartitionData)
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
							var tempElem AlterPartitionRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// NewIsr
							if version >= 0 && version <= 2 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.NewIsr = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.NewIsr = val
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AlterPartitionRequestBrokerState
									elemR := bytes.NewReader(data)
									// BrokerId
									if version >= 3 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BrokerId = val
									}
									// BrokerEpoch
									if version >= 3 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BrokerEpoch = val
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
										var tempElem AlterPartitionRequestBrokerState
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.BrokerEpoch = val
										}
										// BrokerId
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BrokerId); err != nil {
												return err
											}
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.BrokerEpoch); err != nil {
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
									tempElem.NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, len(decoded))
									for i, item := range decoded {
										tempElem.NewIsrWithEpochs[i] = item.(AlterPartitionRequestBrokerState)
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
										var tempElem AlterPartitionRequestBrokerState
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.BrokerEpoch = val
										}
										// BrokerId
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BrokerId); err != nil {
												return err
											}
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.BrokerEpoch); err != nil {
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
									tempElem.NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, len(decoded))
									for i, item := range decoded {
										tempElem.NewIsrWithEpochs[i] = item.(AlterPartitionRequestBrokerState)
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.LeaderRecoveryState = val
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionEpoch = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// NewIsr
							if version >= 0 && version <= 2 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.NewIsr); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.NewIsr); err != nil {
										return err
									}
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.NewIsrWithEpochs) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.NewIsrWithEpochs))); err != nil {
										return err
									}
								}
								for i := range tempElem.NewIsrWithEpochs {
									// BrokerId
									if version >= 3 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.NewIsrWithEpochs[i].BrokerId); err != nil {
											return err
										}
									}
									// BrokerEpoch
									if version >= 3 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.NewIsrWithEpochs[i].BrokerEpoch); err != nil {
											return err
										}
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.LeaderRecoveryState); err != nil {
									return err
								}
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionEpoch); err != nil {
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
						tempElem.Partitions = make([]AlterPartitionRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(AlterPartitionRequestPartitionData)
						}
					}
				}
				// TopicId
				if version >= 2 && version <= 999 {
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
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
							}
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
								return err
							}
						}
						// NewIsr
						if version >= 0 && version <= 2 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].NewIsr); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].NewIsr); err != nil {
									return err
								}
							}
						}
						// NewIsrWithEpochs
						if version >= 3 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].NewIsrWithEpochs) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].NewIsrWithEpochs))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].NewIsrWithEpochs {
								// BrokerId
								if version >= 3 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].NewIsrWithEpochs[i].BrokerId); err != nil {
										return err
									}
								}
								// BrokerEpoch
								if version >= 3 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].NewIsrWithEpochs[i].BrokerEpoch); err != nil {
										return err
									}
								}
							}
						}
						// LeaderRecoveryState
						if version >= 1 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.Partitions[i].LeaderRecoveryState); err != nil {
								return err
							}
						}
						// PartitionEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionEpoch); err != nil {
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
			m.Topics = make([]AlterPartitionRequestTopicData, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(AlterPartitionRequestTopicData)
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

// AlterPartitionRequestTopicData represents The topics to alter ISRs for..
type AlterPartitionRequestTopicData struct {
	// The ID of the topic to alter ISRs for.
	TopicId uuid.UUID `json:"topicid" versions:"2-999"`
	// The partitions to alter ISRs for.
	Partitions []AlterPartitionRequestPartitionData `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AlterPartitionRequestTopicData.
func (m *AlterPartitionRequestTopicData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterPartitionRequestTopicData.
func (m *AlterPartitionRequestTopicData) readTaggedFields(r io.Reader, version int16) error {
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

// AlterPartitionRequestPartitionData represents The partitions to alter ISRs for..
type AlterPartitionRequestPartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The leader epoch of this partition.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// The ISR for this partition. Deprecated since version 3.
	NewIsr []int32 `json:"newisr" versions:"0-2"`
	// The ISR for this partition.
	NewIsrWithEpochs []AlterPartitionRequestBrokerState `json:"newisrwithepochs" versions:"3-999"`
	// 1 if the partition is recovering from an unclean leader election; 0 otherwise.
	LeaderRecoveryState int8 `json:"leaderrecoverystate" versions:"1-999"`
	// The expected epoch of the partition which is being updated.
	PartitionEpoch int32 `json:"partitionepoch" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AlterPartitionRequestPartitionData.
func (m *AlterPartitionRequestPartitionData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterPartitionRequestPartitionData.
func (m *AlterPartitionRequestPartitionData) readTaggedFields(r io.Reader, version int16) error {
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

// AlterPartitionRequestBrokerState represents The ISR for this partition..
type AlterPartitionRequestBrokerState struct {
	// The ID of the broker.
	BrokerId int32 `json:"brokerid" versions:"3-999"`
	// The epoch of the broker. It will be -1 if the epoch check is not supported.
	BrokerEpoch int64 `json:"brokerepoch" versions:"3-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AlterPartitionRequestBrokerState.
func (m *AlterPartitionRequestBrokerState) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterPartitionRequestBrokerState.
func (m *AlterPartitionRequestBrokerState) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for AlterPartitionRequest.
func (m *AlterPartitionRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterPartitionRequest.
func (m *AlterPartitionRequest) readTaggedFields(r io.Reader, version int16) error {
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
