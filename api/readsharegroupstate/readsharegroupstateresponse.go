package readsharegroupstate

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ReadShareGroupStateResponseApiKey        = 84
	ReadShareGroupStateResponseHeaderVersion = 1
)

// ReadShareGroupStateResponse represents a response message.
type ReadShareGroupStateResponse struct {
	// The read results.
	Results []ReadShareGroupStateResponseReadStateResult `json:"results" versions:"0-999"`
}

// Encode encodes a ReadShareGroupStateResponse to a byte slice for the given version.
func (m *ReadShareGroupStateResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ReadShareGroupStateResponse from a byte slice for the given version.
func (m *ReadShareGroupStateResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ReadShareGroupStateResponse to an io.Writer for the given version.
func (m *ReadShareGroupStateResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// Results
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ReadShareGroupStateResponseReadStateResult)
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
					// StateEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].StateEpoch); err != nil {
							return nil, err
						}
					}
					// StartOffset
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(elemW, structItem.Partitions[i].StartOffset); err != nil {
							return nil, err
						}
					}
					// StateBatches
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Partitions[i].StateBatches) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions[i].StateBatches))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Partitions[i].StateBatches {
							// FirstOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].StateBatches[i].FirstOffset); err != nil {
									return nil, err
								}
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].StateBatches[i].LastOffset); err != nil {
									return nil, err
								}
							}
							// DeliveryState
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt8(elemW, structItem.Partitions[i].StateBatches[i].DeliveryState); err != nil {
									return nil, err
								}
							}
							// DeliveryCount
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, structItem.Partitions[i].StateBatches[i].DeliveryCount); err != nil {
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
		items := make([]interface{}, len(m.Results))
		for i := range m.Results {
			items[i] = m.Results[i]
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

// Read reads a ReadShareGroupStateResponse from an io.Reader for the given version.
func (m *ReadShareGroupStateResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// Results
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ReadShareGroupStateResponseReadStateResult
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
				var tempElem ReadShareGroupStateResponseReadStateResult
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
						var elem ReadShareGroupStateResponsePartitionResult
						elemR := bytes.NewReader(data)
						// Partition
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Partition = val
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
						// StateEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.StateEpoch = val
						}
						// StartOffset
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.StartOffset = val
						}
						// StateBatches
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
							var tempElem ReadShareGroupStateResponsePartitionResult
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
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
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.StateEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.StartOffset = val
							}
							// StateBatches
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReadShareGroupStateResponseStateBatch
									elemR := bytes.NewReader(data)
									// FirstOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastOffset = val
									}
									// DeliveryState
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt8(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryState = val
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryCount = val
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
										var tempElem ReadShareGroupStateResponseStateBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.DeliveryState); err != nil {
												return err
											}
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.StateBatches = make([]ReadShareGroupStateResponseStateBatch, len(decoded))
									for i, item := range decoded {
										tempElem.StateBatches[i] = item.(ReadShareGroupStateResponseStateBatch)
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
										var tempElem ReadShareGroupStateResponseStateBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.DeliveryState); err != nil {
												return err
											}
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.StateBatches = make([]ReadShareGroupStateResponseStateBatch, len(decoded))
									for i, item := range decoded {
										tempElem.StateBatches[i] = item.(ReadShareGroupStateResponseStateBatch)
									}
								}
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
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
							// StateEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.StateEpoch); err != nil {
									return err
								}
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
									return err
								}
							}
							// StateBatches
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.StateBatches) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateBatches))); err != nil {
										return err
									}
								}
								for i := range tempElem.StateBatches {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.StateBatches[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.StateBatches[i].LastOffset); err != nil {
											return err
										}
									}
									// DeliveryState
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt8(elemW, tempElem.StateBatches[i].DeliveryState); err != nil {
											return err
										}
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.StateBatches[i].DeliveryCount); err != nil {
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
						tempElem.Partitions = make([]ReadShareGroupStateResponsePartitionResult, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ReadShareGroupStateResponsePartitionResult)
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
							var tempElem ReadShareGroupStateResponsePartitionResult
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
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
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.StateEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.StartOffset = val
							}
							// StateBatches
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReadShareGroupStateResponseStateBatch
									elemR := bytes.NewReader(data)
									// FirstOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastOffset = val
									}
									// DeliveryState
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt8(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryState = val
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryCount = val
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
										var tempElem ReadShareGroupStateResponseStateBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.DeliveryState); err != nil {
												return err
											}
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.StateBatches = make([]ReadShareGroupStateResponseStateBatch, len(decoded))
									for i, item := range decoded {
										tempElem.StateBatches[i] = item.(ReadShareGroupStateResponseStateBatch)
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
										var tempElem ReadShareGroupStateResponseStateBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.DeliveryState); err != nil {
												return err
											}
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.StateBatches = make([]ReadShareGroupStateResponseStateBatch, len(decoded))
									for i, item := range decoded {
										tempElem.StateBatches[i] = item.(ReadShareGroupStateResponseStateBatch)
									}
								}
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
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
							// StateEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.StateEpoch); err != nil {
									return err
								}
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
									return err
								}
							}
							// StateBatches
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.StateBatches) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateBatches))); err != nil {
										return err
									}
								}
								for i := range tempElem.StateBatches {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.StateBatches[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.StateBatches[i].LastOffset); err != nil {
											return err
										}
									}
									// DeliveryState
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt8(elemW, tempElem.StateBatches[i].DeliveryState); err != nil {
											return err
										}
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.StateBatches[i].DeliveryCount); err != nil {
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
						tempElem.Partitions = make([]ReadShareGroupStateResponsePartitionResult, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ReadShareGroupStateResponsePartitionResult)
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
						// StateEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].StateEpoch); err != nil {
								return err
							}
						}
						// StartOffset
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].StartOffset); err != nil {
								return err
							}
						}
						// StateBatches
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].StateBatches) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].StateBatches))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].StateBatches {
								// FirstOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].StateBatches[i].FirstOffset); err != nil {
										return err
									}
								}
								// LastOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].StateBatches[i].LastOffset); err != nil {
										return err
									}
								}
								// DeliveryState
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt8(elemW, tempElem.Partitions[i].StateBatches[i].DeliveryState); err != nil {
										return err
									}
								}
								// DeliveryCount
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].StateBatches[i].DeliveryCount); err != nil {
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
			m.Results = make([]ReadShareGroupStateResponseReadStateResult, len(decoded))
			for i, item := range decoded {
				m.Results[i] = item.(ReadShareGroupStateResponseReadStateResult)
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
				var tempElem ReadShareGroupStateResponseReadStateResult
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
						var elem ReadShareGroupStateResponsePartitionResult
						elemR := bytes.NewReader(data)
						// Partition
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Partition = val
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
						// StateEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.StateEpoch = val
						}
						// StartOffset
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.StartOffset = val
						}
						// StateBatches
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
							var tempElem ReadShareGroupStateResponsePartitionResult
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
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
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.StateEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.StartOffset = val
							}
							// StateBatches
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReadShareGroupStateResponseStateBatch
									elemR := bytes.NewReader(data)
									// FirstOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastOffset = val
									}
									// DeliveryState
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt8(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryState = val
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryCount = val
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
										var tempElem ReadShareGroupStateResponseStateBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.DeliveryState); err != nil {
												return err
											}
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.StateBatches = make([]ReadShareGroupStateResponseStateBatch, len(decoded))
									for i, item := range decoded {
										tempElem.StateBatches[i] = item.(ReadShareGroupStateResponseStateBatch)
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
										var tempElem ReadShareGroupStateResponseStateBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.DeliveryState); err != nil {
												return err
											}
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.StateBatches = make([]ReadShareGroupStateResponseStateBatch, len(decoded))
									for i, item := range decoded {
										tempElem.StateBatches[i] = item.(ReadShareGroupStateResponseStateBatch)
									}
								}
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
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
							// StateEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.StateEpoch); err != nil {
									return err
								}
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
									return err
								}
							}
							// StateBatches
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.StateBatches) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateBatches))); err != nil {
										return err
									}
								}
								for i := range tempElem.StateBatches {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.StateBatches[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.StateBatches[i].LastOffset); err != nil {
											return err
										}
									}
									// DeliveryState
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt8(elemW, tempElem.StateBatches[i].DeliveryState); err != nil {
											return err
										}
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.StateBatches[i].DeliveryCount); err != nil {
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
						tempElem.Partitions = make([]ReadShareGroupStateResponsePartitionResult, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ReadShareGroupStateResponsePartitionResult)
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
							var tempElem ReadShareGroupStateResponsePartitionResult
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
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
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.StateEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.StartOffset = val
							}
							// StateBatches
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReadShareGroupStateResponseStateBatch
									elemR := bytes.NewReader(data)
									// FirstOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastOffset = val
									}
									// DeliveryState
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt8(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryState = val
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryCount = val
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
										var tempElem ReadShareGroupStateResponseStateBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.DeliveryState); err != nil {
												return err
											}
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.StateBatches = make([]ReadShareGroupStateResponseStateBatch, len(decoded))
									for i, item := range decoded {
										tempElem.StateBatches[i] = item.(ReadShareGroupStateResponseStateBatch)
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
										var tempElem ReadShareGroupStateResponseStateBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.DeliveryState); err != nil {
												return err
											}
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.StateBatches = make([]ReadShareGroupStateResponseStateBatch, len(decoded))
									for i, item := range decoded {
										tempElem.StateBatches[i] = item.(ReadShareGroupStateResponseStateBatch)
									}
								}
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
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
							// StateEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.StateEpoch); err != nil {
									return err
								}
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
									return err
								}
							}
							// StateBatches
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.StateBatches) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateBatches))); err != nil {
										return err
									}
								}
								for i := range tempElem.StateBatches {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.StateBatches[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.StateBatches[i].LastOffset); err != nil {
											return err
										}
									}
									// DeliveryState
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt8(elemW, tempElem.StateBatches[i].DeliveryState); err != nil {
											return err
										}
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.StateBatches[i].DeliveryCount); err != nil {
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
						tempElem.Partitions = make([]ReadShareGroupStateResponsePartitionResult, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ReadShareGroupStateResponsePartitionResult)
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
						// StateEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].StateEpoch); err != nil {
								return err
							}
						}
						// StartOffset
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].StartOffset); err != nil {
								return err
							}
						}
						// StateBatches
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].StateBatches) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].StateBatches))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].StateBatches {
								// FirstOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].StateBatches[i].FirstOffset); err != nil {
										return err
									}
								}
								// LastOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].StateBatches[i].LastOffset); err != nil {
										return err
									}
								}
								// DeliveryState
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt8(elemW, tempElem.Partitions[i].StateBatches[i].DeliveryState); err != nil {
										return err
									}
								}
								// DeliveryCount
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].StateBatches[i].DeliveryCount); err != nil {
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
			m.Results = make([]ReadShareGroupStateResponseReadStateResult, len(decoded))
			for i, item := range decoded {
				m.Results[i] = item.(ReadShareGroupStateResponseReadStateResult)
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

// ReadShareGroupStateResponseReadStateResult represents The read results..
type ReadShareGroupStateResponseReadStateResult struct {
	// The topic identifier.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// The results for the partitions.
	Partitions []ReadShareGroupStateResponsePartitionResult `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ReadShareGroupStateResponseReadStateResult.
func (m *ReadShareGroupStateResponseReadStateResult) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ReadShareGroupStateResponseReadStateResult.
func (m *ReadShareGroupStateResponseReadStateResult) readTaggedFields(r io.Reader, version int16) error {
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

// ReadShareGroupStateResponsePartitionResult represents The results for the partitions..
type ReadShareGroupStateResponsePartitionResult struct {
	// The partition index.
	Partition int32 `json:"partition" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The state epoch of the share-partition.
	StateEpoch int32 `json:"stateepoch" versions:"0-999"`
	// The share-partition start offset, which can be -1 if it is not yet initialized.
	StartOffset int64 `json:"startoffset" versions:"0-999"`
	// The state batches for this share-partition.
	StateBatches []ReadShareGroupStateResponseStateBatch `json:"statebatches" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ReadShareGroupStateResponsePartitionResult.
func (m *ReadShareGroupStateResponsePartitionResult) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ReadShareGroupStateResponsePartitionResult.
func (m *ReadShareGroupStateResponsePartitionResult) readTaggedFields(r io.Reader, version int16) error {
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

// ReadShareGroupStateResponseStateBatch represents The state batches for this share-partition..
type ReadShareGroupStateResponseStateBatch struct {
	// The first offset of this state batch.
	FirstOffset int64 `json:"firstoffset" versions:"0-999"`
	// The last offset of this state batch.
	LastOffset int64 `json:"lastoffset" versions:"0-999"`
	// The delivery state - 0:Available,2:Acked,4:Archived.
	DeliveryState int8 `json:"deliverystate" versions:"0-999"`
	// The delivery count.
	DeliveryCount int16 `json:"deliverycount" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ReadShareGroupStateResponseStateBatch.
func (m *ReadShareGroupStateResponseStateBatch) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ReadShareGroupStateResponseStateBatch.
func (m *ReadShareGroupStateResponseStateBatch) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for ReadShareGroupStateResponse.
func (m *ReadShareGroupStateResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ReadShareGroupStateResponse.
func (m *ReadShareGroupStateResponse) readTaggedFields(r io.Reader, version int16) error {
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
