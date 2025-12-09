package writetxnmarkers

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	WriteTxnMarkersResponseApiKey        = 27
	WriteTxnMarkersResponseHeaderVersion = 1
)

// WriteTxnMarkersResponse represents a response message.
type WriteTxnMarkersResponse struct {
	// The results for writing makers.
	Markers []WriteTxnMarkersResponseWritableTxnMarkerResult `json:"markers" versions:"0-999"`
}

// Encode encodes a WriteTxnMarkersResponse to a byte slice for the given version.
func (m *WriteTxnMarkersResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a WriteTxnMarkersResponse from a byte slice for the given version.
func (m *WriteTxnMarkersResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a WriteTxnMarkersResponse to an io.Writer for the given version.
func (m *WriteTxnMarkersResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
		isFlexible = true
	}

	// Markers
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(WriteTxnMarkersResponseWritableTxnMarkerResult)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// ProducerId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(elemW, structItem.ProducerId); err != nil {
					return nil, err
				}
			}
			// Topics
			if version >= 0 && version <= 999 {
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
					if version >= 0 && version <= 999 {
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
					// Partitions
					if version >= 0 && version <= 999 {
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
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Topics[i].Partitions[i].PartitionIndex); err != nil {
									return nil, err
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, structItem.Topics[i].Partitions[i].ErrorCode); err != nil {
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
		items := make([]interface{}, len(m.Markers))
		for i := range m.Markers {
			items[i] = m.Markers[i]
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

// Read reads a WriteTxnMarkersResponse from an io.Reader for the given version.
func (m *WriteTxnMarkersResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
		isFlexible = true
	}

	// Markers
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem WriteTxnMarkersResponseWritableTxnMarkerResult
			elemR := bytes.NewReader(data)
			// ProducerId
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt64(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ProducerId = val
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
				var tempElem WriteTxnMarkersResponseWritableTxnMarkerResult
				// ProducerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.ProducerId = val
				}
				// Topics
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem WriteTxnMarkersResponseWritableTxnMarkerTopicResult
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
							var tempElem WriteTxnMarkersResponseWritableTxnMarkerTopicResult
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
									var elem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
										var tempElem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
									tempElem.Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerPartitionResult)
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
										var tempElem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
									tempElem.Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerPartitionResult)
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
						tempElem.Topics = make([]WriteTxnMarkersResponseWritableTxnMarkerTopicResult, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerTopicResult)
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
							var tempElem WriteTxnMarkersResponseWritableTxnMarkerTopicResult
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
									var elem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
										var tempElem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
									tempElem.Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerPartitionResult)
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
										var tempElem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
									tempElem.Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerPartitionResult)
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
						tempElem.Topics = make([]WriteTxnMarkersResponseWritableTxnMarkerTopicResult, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerTopicResult)
						}
					}
				}
				// ProducerId
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
						return err
					}
				}
				// Topics
				if version >= 0 && version <= 999 {
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
						if version >= 0 && version <= 999 {
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
						// Partitions
						if version >= 0 && version <= 999 {
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
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Topics[i].Partitions[i].PartitionIndex); err != nil {
										return err
									}
								}
								// ErrorCode
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Topics[i].Partitions[i].ErrorCode); err != nil {
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
			m.Markers = make([]WriteTxnMarkersResponseWritableTxnMarkerResult, len(decoded))
			for i, item := range decoded {
				m.Markers[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerResult)
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
				var tempElem WriteTxnMarkersResponseWritableTxnMarkerResult
				// ProducerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.ProducerId = val
				}
				// Topics
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem WriteTxnMarkersResponseWritableTxnMarkerTopicResult
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
							var tempElem WriteTxnMarkersResponseWritableTxnMarkerTopicResult
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
									var elem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
										var tempElem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
									tempElem.Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerPartitionResult)
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
										var tempElem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
									tempElem.Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerPartitionResult)
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
						tempElem.Topics = make([]WriteTxnMarkersResponseWritableTxnMarkerTopicResult, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerTopicResult)
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
							var tempElem WriteTxnMarkersResponseWritableTxnMarkerTopicResult
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
									var elem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
										var tempElem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
									tempElem.Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerPartitionResult)
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
										var tempElem WriteTxnMarkersResponseWritableTxnMarkerPartitionResult
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
									tempElem.Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerPartitionResult)
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
						tempElem.Topics = make([]WriteTxnMarkersResponseWritableTxnMarkerTopicResult, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerTopicResult)
						}
					}
				}
				// ProducerId
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
						return err
					}
				}
				// Topics
				if version >= 0 && version <= 999 {
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
						if version >= 0 && version <= 999 {
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
						// Partitions
						if version >= 0 && version <= 999 {
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
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Topics[i].Partitions[i].PartitionIndex); err != nil {
										return err
									}
								}
								// ErrorCode
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Topics[i].Partitions[i].ErrorCode); err != nil {
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
			m.Markers = make([]WriteTxnMarkersResponseWritableTxnMarkerResult, len(decoded))
			for i, item := range decoded {
				m.Markers[i] = item.(WriteTxnMarkersResponseWritableTxnMarkerResult)
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

// WriteTxnMarkersResponseWritableTxnMarkerResult represents The results for writing makers..
type WriteTxnMarkersResponseWritableTxnMarkerResult struct {
	// The current producer ID in use by the transactional ID.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// The results by topic.
	Topics []WriteTxnMarkersResponseWritableTxnMarkerTopicResult `json:"topics" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for WriteTxnMarkersResponseWritableTxnMarkerResult.
func (m *WriteTxnMarkersResponseWritableTxnMarkerResult) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for WriteTxnMarkersResponseWritableTxnMarkerResult.
func (m *WriteTxnMarkersResponseWritableTxnMarkerResult) readTaggedFields(r io.Reader, version int16) error {
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

// WriteTxnMarkersResponseWritableTxnMarkerTopicResult represents The results by topic..
type WriteTxnMarkersResponseWritableTxnMarkerTopicResult struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The results by partition.
	Partitions []WriteTxnMarkersResponseWritableTxnMarkerPartitionResult `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for WriteTxnMarkersResponseWritableTxnMarkerTopicResult.
func (m *WriteTxnMarkersResponseWritableTxnMarkerTopicResult) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for WriteTxnMarkersResponseWritableTxnMarkerTopicResult.
func (m *WriteTxnMarkersResponseWritableTxnMarkerTopicResult) readTaggedFields(r io.Reader, version int16) error {
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

// WriteTxnMarkersResponseWritableTxnMarkerPartitionResult represents The results by partition..
type WriteTxnMarkersResponseWritableTxnMarkerPartitionResult struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for WriteTxnMarkersResponseWritableTxnMarkerPartitionResult.
func (m *WriteTxnMarkersResponseWritableTxnMarkerPartitionResult) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for WriteTxnMarkersResponseWritableTxnMarkerPartitionResult.
func (m *WriteTxnMarkersResponseWritableTxnMarkerPartitionResult) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for WriteTxnMarkersResponse.
func (m *WriteTxnMarkersResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for WriteTxnMarkersResponse.
func (m *WriteTxnMarkersResponse) readTaggedFields(r io.Reader, version int16) error {
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
