package describeproducers

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeProducersResponseApiKey        = 61
	DescribeProducersResponseHeaderVersion = 1
)

// DescribeProducersResponse represents a response message.
type DescribeProducersResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// Each topic in the response.
	Topics []DescribeProducersResponseTopicResponse `json:"topics" versions:"0-999"`
}

// Encode encodes a DescribeProducersResponse to a byte slice for the given version.
func (m *DescribeProducersResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeProducersResponse from a byte slice for the given version.
func (m *DescribeProducersResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeProducersResponse to an io.Writer for the given version.
func (m *DescribeProducersResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
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
			structItem, ok := item.(DescribeProducersResponseTopicResponse)
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
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(elemW, structItem.Partitions[i].ErrorCode); err != nil {
							return nil, err
						}
					}
					// ErrorMessage
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.Partitions[i].ErrorMessage); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.Partitions[i].ErrorMessage); err != nil {
								return nil, err
							}
						}
					}
					// ActiveProducers
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Partitions[i].ActiveProducers) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions[i].ActiveProducers))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Partitions[i].ActiveProducers {
							// ProducerId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].ActiveProducers[i].ProducerId); err != nil {
									return nil, err
								}
							}
							// ProducerEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Partitions[i].ActiveProducers[i].ProducerEpoch); err != nil {
									return nil, err
								}
							}
							// LastSequence
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Partitions[i].ActiveProducers[i].LastSequence); err != nil {
									return nil, err
								}
							}
							// LastTimestamp
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].ActiveProducers[i].LastTimestamp); err != nil {
									return nil, err
								}
							}
							// CoordinatorEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Partitions[i].ActiveProducers[i].CoordinatorEpoch); err != nil {
									return nil, err
								}
							}
							// CurrentTxnStartOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].ActiveProducers[i].CurrentTxnStartOffset); err != nil {
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

// Read reads a DescribeProducersResponse from an io.Reader for the given version.
func (m *DescribeProducersResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
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
			var elem DescribeProducersResponseTopicResponse
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
				var tempElem DescribeProducersResponseTopicResponse
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
						var elem DescribeProducersResponsePartitionResponse
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
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
						if version >= 0 && version <= 999 {
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
						// ActiveProducers
						if version >= 0 && version <= 999 {
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
							var tempElem DescribeProducersResponsePartitionResponse
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
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
							if version >= 0 && version <= 999 {
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
							// ActiveProducers
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeProducersResponseProducerState
									elemR := bytes.NewReader(data)
									// ProducerId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerId = val
									}
									// ProducerEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerEpoch = val
									}
									// LastSequence
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastSequence = val
									}
									// LastTimestamp
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastTimestamp = val
									}
									// CoordinatorEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CoordinatorEpoch = val
									}
									// CurrentTxnStartOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CurrentTxnStartOffset = val
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
										var tempElem DescribeProducersResponseProducerState
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CurrentTxnStartOffset = val
										}
										// ProducerId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ProducerEpoch); err != nil {
												return err
											}
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LastSequence); err != nil {
												return err
											}
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastTimestamp); err != nil {
												return err
											}
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CoordinatorEpoch); err != nil {
												return err
											}
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CurrentTxnStartOffset); err != nil {
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
									tempElem.ActiveProducers = make([]DescribeProducersResponseProducerState, len(decoded))
									for i, item := range decoded {
										tempElem.ActiveProducers[i] = item.(DescribeProducersResponseProducerState)
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
										var tempElem DescribeProducersResponseProducerState
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CurrentTxnStartOffset = val
										}
										// ProducerId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ProducerEpoch); err != nil {
												return err
											}
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LastSequence); err != nil {
												return err
											}
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastTimestamp); err != nil {
												return err
											}
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CoordinatorEpoch); err != nil {
												return err
											}
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CurrentTxnStartOffset); err != nil {
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
									tempElem.ActiveProducers = make([]DescribeProducersResponseProducerState, len(decoded))
									for i, item := range decoded {
										tempElem.ActiveProducers[i] = item.(DescribeProducersResponseProducerState)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
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
							if version >= 0 && version <= 999 {
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
							// ActiveProducers
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ActiveProducers) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ActiveProducers))); err != nil {
										return err
									}
								}
								for i := range tempElem.ActiveProducers {
									// ProducerId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].ProducerId); err != nil {
											return err
										}
									}
									// ProducerEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].ProducerEpoch); err != nil {
											return err
										}
									}
									// LastSequence
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].LastSequence); err != nil {
											return err
										}
									}
									// LastTimestamp
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].LastTimestamp); err != nil {
											return err
										}
									}
									// CoordinatorEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].CoordinatorEpoch); err != nil {
											return err
										}
									}
									// CurrentTxnStartOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].CurrentTxnStartOffset); err != nil {
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
						tempElem.Partitions = make([]DescribeProducersResponsePartitionResponse, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeProducersResponsePartitionResponse)
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
							var tempElem DescribeProducersResponsePartitionResponse
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
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
							if version >= 0 && version <= 999 {
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
							// ActiveProducers
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeProducersResponseProducerState
									elemR := bytes.NewReader(data)
									// ProducerId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerId = val
									}
									// ProducerEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerEpoch = val
									}
									// LastSequence
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastSequence = val
									}
									// LastTimestamp
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastTimestamp = val
									}
									// CoordinatorEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CoordinatorEpoch = val
									}
									// CurrentTxnStartOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CurrentTxnStartOffset = val
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
										var tempElem DescribeProducersResponseProducerState
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CurrentTxnStartOffset = val
										}
										// ProducerId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ProducerEpoch); err != nil {
												return err
											}
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LastSequence); err != nil {
												return err
											}
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastTimestamp); err != nil {
												return err
											}
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CoordinatorEpoch); err != nil {
												return err
											}
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CurrentTxnStartOffset); err != nil {
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
									tempElem.ActiveProducers = make([]DescribeProducersResponseProducerState, len(decoded))
									for i, item := range decoded {
										tempElem.ActiveProducers[i] = item.(DescribeProducersResponseProducerState)
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
										var tempElem DescribeProducersResponseProducerState
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CurrentTxnStartOffset = val
										}
										// ProducerId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ProducerEpoch); err != nil {
												return err
											}
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LastSequence); err != nil {
												return err
											}
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastTimestamp); err != nil {
												return err
											}
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CoordinatorEpoch); err != nil {
												return err
											}
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CurrentTxnStartOffset); err != nil {
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
									tempElem.ActiveProducers = make([]DescribeProducersResponseProducerState, len(decoded))
									for i, item := range decoded {
										tempElem.ActiveProducers[i] = item.(DescribeProducersResponseProducerState)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
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
							if version >= 0 && version <= 999 {
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
							// ActiveProducers
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ActiveProducers) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ActiveProducers))); err != nil {
										return err
									}
								}
								for i := range tempElem.ActiveProducers {
									// ProducerId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].ProducerId); err != nil {
											return err
										}
									}
									// ProducerEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].ProducerEpoch); err != nil {
											return err
										}
									}
									// LastSequence
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].LastSequence); err != nil {
											return err
										}
									}
									// LastTimestamp
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].LastTimestamp); err != nil {
											return err
										}
									}
									// CoordinatorEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].CoordinatorEpoch); err != nil {
											return err
										}
									}
									// CurrentTxnStartOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].CurrentTxnStartOffset); err != nil {
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
						tempElem.Partitions = make([]DescribeProducersResponsePartitionResponse, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeProducersResponsePartitionResponse)
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
						// ErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
								return err
							}
						}
						// ErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].ErrorMessage); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].ErrorMessage); err != nil {
									return err
								}
							}
						}
						// ActiveProducers
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].ActiveProducers) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].ActiveProducers))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].ActiveProducers {
								// ProducerId
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].ActiveProducers[i].ProducerId); err != nil {
										return err
									}
								}
								// ProducerEpoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].ActiveProducers[i].ProducerEpoch); err != nil {
										return err
									}
								}
								// LastSequence
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].ActiveProducers[i].LastSequence); err != nil {
										return err
									}
								}
								// LastTimestamp
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].ActiveProducers[i].LastTimestamp); err != nil {
										return err
									}
								}
								// CoordinatorEpoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].ActiveProducers[i].CoordinatorEpoch); err != nil {
										return err
									}
								}
								// CurrentTxnStartOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].ActiveProducers[i].CurrentTxnStartOffset); err != nil {
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
			m.Topics = make([]DescribeProducersResponseTopicResponse, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(DescribeProducersResponseTopicResponse)
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
				var tempElem DescribeProducersResponseTopicResponse
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
						var elem DescribeProducersResponsePartitionResponse
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
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
						if version >= 0 && version <= 999 {
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
						// ActiveProducers
						if version >= 0 && version <= 999 {
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
							var tempElem DescribeProducersResponsePartitionResponse
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
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
							if version >= 0 && version <= 999 {
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
							// ActiveProducers
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeProducersResponseProducerState
									elemR := bytes.NewReader(data)
									// ProducerId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerId = val
									}
									// ProducerEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerEpoch = val
									}
									// LastSequence
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastSequence = val
									}
									// LastTimestamp
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastTimestamp = val
									}
									// CoordinatorEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CoordinatorEpoch = val
									}
									// CurrentTxnStartOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CurrentTxnStartOffset = val
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
										var tempElem DescribeProducersResponseProducerState
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CurrentTxnStartOffset = val
										}
										// ProducerId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ProducerEpoch); err != nil {
												return err
											}
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LastSequence); err != nil {
												return err
											}
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastTimestamp); err != nil {
												return err
											}
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CoordinatorEpoch); err != nil {
												return err
											}
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CurrentTxnStartOffset); err != nil {
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
									tempElem.ActiveProducers = make([]DescribeProducersResponseProducerState, len(decoded))
									for i, item := range decoded {
										tempElem.ActiveProducers[i] = item.(DescribeProducersResponseProducerState)
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
										var tempElem DescribeProducersResponseProducerState
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CurrentTxnStartOffset = val
										}
										// ProducerId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ProducerEpoch); err != nil {
												return err
											}
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LastSequence); err != nil {
												return err
											}
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastTimestamp); err != nil {
												return err
											}
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CoordinatorEpoch); err != nil {
												return err
											}
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CurrentTxnStartOffset); err != nil {
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
									tempElem.ActiveProducers = make([]DescribeProducersResponseProducerState, len(decoded))
									for i, item := range decoded {
										tempElem.ActiveProducers[i] = item.(DescribeProducersResponseProducerState)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
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
							if version >= 0 && version <= 999 {
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
							// ActiveProducers
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ActiveProducers) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ActiveProducers))); err != nil {
										return err
									}
								}
								for i := range tempElem.ActiveProducers {
									// ProducerId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].ProducerId); err != nil {
											return err
										}
									}
									// ProducerEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].ProducerEpoch); err != nil {
											return err
										}
									}
									// LastSequence
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].LastSequence); err != nil {
											return err
										}
									}
									// LastTimestamp
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].LastTimestamp); err != nil {
											return err
										}
									}
									// CoordinatorEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].CoordinatorEpoch); err != nil {
											return err
										}
									}
									// CurrentTxnStartOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].CurrentTxnStartOffset); err != nil {
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
						tempElem.Partitions = make([]DescribeProducersResponsePartitionResponse, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeProducersResponsePartitionResponse)
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
							var tempElem DescribeProducersResponsePartitionResponse
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
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
							if version >= 0 && version <= 999 {
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
							// ActiveProducers
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeProducersResponseProducerState
									elemR := bytes.NewReader(data)
									// ProducerId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerId = val
									}
									// ProducerEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerEpoch = val
									}
									// LastSequence
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastSequence = val
									}
									// LastTimestamp
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastTimestamp = val
									}
									// CoordinatorEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CoordinatorEpoch = val
									}
									// CurrentTxnStartOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CurrentTxnStartOffset = val
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
										var tempElem DescribeProducersResponseProducerState
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CurrentTxnStartOffset = val
										}
										// ProducerId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ProducerEpoch); err != nil {
												return err
											}
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LastSequence); err != nil {
												return err
											}
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastTimestamp); err != nil {
												return err
											}
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CoordinatorEpoch); err != nil {
												return err
											}
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CurrentTxnStartOffset); err != nil {
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
									tempElem.ActiveProducers = make([]DescribeProducersResponseProducerState, len(decoded))
									for i, item := range decoded {
										tempElem.ActiveProducers[i] = item.(DescribeProducersResponseProducerState)
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
										var tempElem DescribeProducersResponseProducerState
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.CurrentTxnStartOffset = val
										}
										// ProducerId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ProducerEpoch); err != nil {
												return err
											}
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LastSequence); err != nil {
												return err
											}
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastTimestamp); err != nil {
												return err
											}
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CoordinatorEpoch); err != nil {
												return err
											}
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.CurrentTxnStartOffset); err != nil {
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
									tempElem.ActiveProducers = make([]DescribeProducersResponseProducerState, len(decoded))
									for i, item := range decoded {
										tempElem.ActiveProducers[i] = item.(DescribeProducersResponseProducerState)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
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
							if version >= 0 && version <= 999 {
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
							// ActiveProducers
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ActiveProducers) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ActiveProducers))); err != nil {
										return err
									}
								}
								for i := range tempElem.ActiveProducers {
									// ProducerId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].ProducerId); err != nil {
											return err
										}
									}
									// ProducerEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].ProducerEpoch); err != nil {
											return err
										}
									}
									// LastSequence
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].LastSequence); err != nil {
											return err
										}
									}
									// LastTimestamp
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].LastTimestamp); err != nil {
											return err
										}
									}
									// CoordinatorEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ActiveProducers[i].CoordinatorEpoch); err != nil {
											return err
										}
									}
									// CurrentTxnStartOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.ActiveProducers[i].CurrentTxnStartOffset); err != nil {
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
						tempElem.Partitions = make([]DescribeProducersResponsePartitionResponse, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeProducersResponsePartitionResponse)
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
						// ErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
								return err
							}
						}
						// ErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].ErrorMessage); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].ErrorMessage); err != nil {
									return err
								}
							}
						}
						// ActiveProducers
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].ActiveProducers) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].ActiveProducers))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].ActiveProducers {
								// ProducerId
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].ActiveProducers[i].ProducerId); err != nil {
										return err
									}
								}
								// ProducerEpoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].ActiveProducers[i].ProducerEpoch); err != nil {
										return err
									}
								}
								// LastSequence
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].ActiveProducers[i].LastSequence); err != nil {
										return err
									}
								}
								// LastTimestamp
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].ActiveProducers[i].LastTimestamp); err != nil {
										return err
									}
								}
								// CoordinatorEpoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].ActiveProducers[i].CoordinatorEpoch); err != nil {
										return err
									}
								}
								// CurrentTxnStartOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].ActiveProducers[i].CurrentTxnStartOffset); err != nil {
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
			m.Topics = make([]DescribeProducersResponseTopicResponse, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(DescribeProducersResponseTopicResponse)
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

// DescribeProducersResponseTopicResponse represents Each topic in the response..
type DescribeProducersResponseTopicResponse struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// Each partition in the response.
	Partitions []DescribeProducersResponsePartitionResponse `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeProducersResponseTopicResponse.
func (m *DescribeProducersResponseTopicResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeProducersResponseTopicResponse.
func (m *DescribeProducersResponseTopicResponse) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeProducersResponsePartitionResponse represents Each partition in the response..
type DescribeProducersResponsePartitionResponse struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The partition error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The partition error message, which may be null if no additional details are available.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The active producers for the partition.
	ActiveProducers []DescribeProducersResponseProducerState `json:"activeproducers" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeProducersResponsePartitionResponse.
func (m *DescribeProducersResponsePartitionResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeProducersResponsePartitionResponse.
func (m *DescribeProducersResponsePartitionResponse) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeProducersResponseProducerState represents The active producers for the partition..
type DescribeProducersResponseProducerState struct {
	// The producer id.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// The producer epoch.
	ProducerEpoch int32 `json:"producerepoch" versions:"0-999"`
	// The last sequence number sent by the producer.
	LastSequence int32 `json:"lastsequence" versions:"0-999"`
	// The last timestamp sent by the producer.
	LastTimestamp int64 `json:"lasttimestamp" versions:"0-999"`
	// The current epoch of the producer group.
	CoordinatorEpoch int32 `json:"coordinatorepoch" versions:"0-999"`
	// The current transaction start offset of the producer.
	CurrentTxnStartOffset int64 `json:"currenttxnstartoffset" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeProducersResponseProducerState.
func (m *DescribeProducersResponseProducerState) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeProducersResponseProducerState.
func (m *DescribeProducersResponseProducerState) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for DescribeProducersResponse.
func (m *DescribeProducersResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeProducersResponse.
func (m *DescribeProducersResponse) readTaggedFields(r io.Reader, version int16) error {
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
