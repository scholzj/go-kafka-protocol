package writetxnmarkers

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	WriteTxnMarkersRequestApiKey        = 27
	WriteTxnMarkersRequestHeaderVersion = 1
)

// WriteTxnMarkersRequest represents a request message.
type WriteTxnMarkersRequest struct {
	// The transaction markers to be written.
	Markers []WriteTxnMarkersRequestWritableTxnMarker `json:"markers" versions:"0-999"`
}

// Encode encodes a WriteTxnMarkersRequest to a byte slice for the given version.
func (m *WriteTxnMarkersRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a WriteTxnMarkersRequest from a byte slice for the given version.
func (m *WriteTxnMarkersRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a WriteTxnMarkersRequest to an io.Writer for the given version.
func (m *WriteTxnMarkersRequest) Write(w io.Writer, version int16) error {
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
			structItem, ok := item.(WriteTxnMarkersRequestWritableTxnMarker)
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
			// ProducerEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ProducerEpoch); err != nil {
					return nil, err
				}
			}
			// TransactionResult
			if version >= 0 && version <= 999 {
				if err := protocol.WriteBool(elemW, structItem.TransactionResult); err != nil {
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
					// PartitionIndexes
					if version >= 0 && version <= 999 {
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
			// CoordinatorEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.CoordinatorEpoch); err != nil {
					return nil, err
				}
			}
			// TransactionVersion
			if version >= 2 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.TransactionVersion); err != nil {
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

// Read reads a WriteTxnMarkersRequest from an io.Reader for the given version.
func (m *WriteTxnMarkersRequest) Read(r io.Reader, version int16) error {
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
			var elem WriteTxnMarkersRequestWritableTxnMarker
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
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ProducerEpoch = val
			}
			// TransactionResult
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadBool(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.TransactionResult = val
			}
			// Topics
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// CoordinatorEpoch
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.CoordinatorEpoch = val
			}
			// TransactionVersion
			if version >= 2 && version <= 999 {
				val, err := protocol.ReadInt8(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.TransactionVersion = val
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
				var tempElem WriteTxnMarkersRequestWritableTxnMarker
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
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ProducerEpoch = val
				}
				// TransactionResult
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					tempElem.TransactionResult = val
				}
				// Topics
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem WriteTxnMarkersRequestWritableTxnMarkerTopic
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
						// PartitionIndexes
						if version >= 0 && version <= 999 {
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
							var tempElem WriteTxnMarkersRequestWritableTxnMarkerTopic
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
							// PartitionIndexes
							if version >= 0 && version <= 999 {
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
							// PartitionIndexes
							if version >= 0 && version <= 999 {
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
						tempElem.Topics = make([]WriteTxnMarkersRequestWritableTxnMarkerTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(WriteTxnMarkersRequestWritableTxnMarkerTopic)
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
							var tempElem WriteTxnMarkersRequestWritableTxnMarkerTopic
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
							// PartitionIndexes
							if version >= 0 && version <= 999 {
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
							// PartitionIndexes
							if version >= 0 && version <= 999 {
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
						tempElem.Topics = make([]WriteTxnMarkersRequestWritableTxnMarkerTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(WriteTxnMarkersRequestWritableTxnMarkerTopic)
						}
					}
				}
				// CoordinatorEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.CoordinatorEpoch = val
				}
				// TransactionVersion
				if version >= 2 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.TransactionVersion = val
				}
				// ProducerId
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
						return err
					}
				}
				// ProducerEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ProducerEpoch); err != nil {
						return err
					}
				}
				// TransactionResult
				if version >= 0 && version <= 999 {
					if err := protocol.WriteBool(elemW, tempElem.TransactionResult); err != nil {
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
						// PartitionIndexes
						if version >= 0 && version <= 999 {
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
				// CoordinatorEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.CoordinatorEpoch); err != nil {
						return err
					}
				}
				// TransactionVersion
				if version >= 2 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.TransactionVersion); err != nil {
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
			m.Markers = make([]WriteTxnMarkersRequestWritableTxnMarker, len(decoded))
			for i, item := range decoded {
				m.Markers[i] = item.(WriteTxnMarkersRequestWritableTxnMarker)
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
				var tempElem WriteTxnMarkersRequestWritableTxnMarker
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
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ProducerEpoch = val
				}
				// TransactionResult
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					tempElem.TransactionResult = val
				}
				// Topics
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem WriteTxnMarkersRequestWritableTxnMarkerTopic
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
						// PartitionIndexes
						if version >= 0 && version <= 999 {
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
							var tempElem WriteTxnMarkersRequestWritableTxnMarkerTopic
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
							// PartitionIndexes
							if version >= 0 && version <= 999 {
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
							// PartitionIndexes
							if version >= 0 && version <= 999 {
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
						tempElem.Topics = make([]WriteTxnMarkersRequestWritableTxnMarkerTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(WriteTxnMarkersRequestWritableTxnMarkerTopic)
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
							var tempElem WriteTxnMarkersRequestWritableTxnMarkerTopic
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
							// PartitionIndexes
							if version >= 0 && version <= 999 {
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
							// PartitionIndexes
							if version >= 0 && version <= 999 {
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
						tempElem.Topics = make([]WriteTxnMarkersRequestWritableTxnMarkerTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(WriteTxnMarkersRequestWritableTxnMarkerTopic)
						}
					}
				}
				// CoordinatorEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.CoordinatorEpoch = val
				}
				// TransactionVersion
				if version >= 2 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.TransactionVersion = val
				}
				// ProducerId
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
						return err
					}
				}
				// ProducerEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ProducerEpoch); err != nil {
						return err
					}
				}
				// TransactionResult
				if version >= 0 && version <= 999 {
					if err := protocol.WriteBool(elemW, tempElem.TransactionResult); err != nil {
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
						// PartitionIndexes
						if version >= 0 && version <= 999 {
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
				// CoordinatorEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.CoordinatorEpoch); err != nil {
						return err
					}
				}
				// TransactionVersion
				if version >= 2 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.TransactionVersion); err != nil {
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
			m.Markers = make([]WriteTxnMarkersRequestWritableTxnMarker, len(decoded))
			for i, item := range decoded {
				m.Markers[i] = item.(WriteTxnMarkersRequestWritableTxnMarker)
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

// WriteTxnMarkersRequestWritableTxnMarker represents The transaction markers to be written..
type WriteTxnMarkersRequestWritableTxnMarker struct {
	// The current producer ID.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// The current epoch associated with the producer ID.
	ProducerEpoch int16 `json:"producerepoch" versions:"0-999"`
	// The result of the transaction to write to the partitions (false = ABORT, true = COMMIT).
	TransactionResult bool `json:"transactionresult" versions:"0-999"`
	// Each topic that we want to write transaction marker(s) for.
	Topics []WriteTxnMarkersRequestWritableTxnMarkerTopic `json:"topics" versions:"0-999"`
	// Epoch associated with the transaction state partition hosted by this transaction coordinator.
	CoordinatorEpoch int32 `json:"coordinatorepoch" versions:"0-999"`
	// Transaction version of the marker. Ex: 0/1 = legacy (TV0/TV1), 2 = TV2 etc.
	TransactionVersion int8 `json:"transactionversion" versions:"2-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for WriteTxnMarkersRequestWritableTxnMarker.
func (m *WriteTxnMarkersRequestWritableTxnMarker) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for WriteTxnMarkersRequestWritableTxnMarker.
func (m *WriteTxnMarkersRequestWritableTxnMarker) readTaggedFields(r io.Reader, version int16) error {
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

// WriteTxnMarkersRequestWritableTxnMarkerTopic represents Each topic that we want to write transaction marker(s) for..
type WriteTxnMarkersRequestWritableTxnMarkerTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The indexes of the partitions to write transaction markers for.
	PartitionIndexes []int32 `json:"partitionindexes" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for WriteTxnMarkersRequestWritableTxnMarkerTopic.
func (m *WriteTxnMarkersRequestWritableTxnMarkerTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for WriteTxnMarkersRequestWritableTxnMarkerTopic.
func (m *WriteTxnMarkersRequestWritableTxnMarkerTopic) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for WriteTxnMarkersRequest.
func (m *WriteTxnMarkersRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for WriteTxnMarkersRequest.
func (m *WriteTxnMarkersRequest) readTaggedFields(r io.Reader, version int16) error {
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
