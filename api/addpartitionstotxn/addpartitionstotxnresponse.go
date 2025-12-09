package addpartitionstotxn

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AddPartitionsToTxnResponseApiKey        = 24
	AddPartitionsToTxnResponseHeaderVersion = 1
)

// AddPartitionsToTxnResponse represents a response message.
type AddPartitionsToTxnResponse struct {
	// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The response top level error code.
	ErrorCode int16 `json:"errorcode" versions:"4-999"`
	// Results categorized by transactional ID.
	ResultsByTransaction []AddPartitionsToTxnResponseAddPartitionsToTxnResult `json:"resultsbytransaction" versions:"4-999"`
	// The results for each topic.
	ResultsByTopicV3AndBelow []AddPartitionsToTxnTopicResult `json:"resultsbytopicv3andbelow" versions:"0-3"`
}

// Encode encodes a AddPartitionsToTxnResponse to a byte slice for the given version.
func (m *AddPartitionsToTxnResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AddPartitionsToTxnResponse from a byte slice for the given version.
func (m *AddPartitionsToTxnResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AddPartitionsToTxnResponse to an io.Writer for the given version.
func (m *AddPartitionsToTxnResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// ErrorCode
	if version >= 4 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// ResultsByTransaction
	if version >= 4 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(AddPartitionsToTxnResponseAddPartitionsToTxnResult)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// TransactionalId
			if version >= 4 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.TransactionalId); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.TransactionalId); err != nil {
						return nil, err
					}
				}
			}
			// TopicResults
			if version >= 4 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.TopicResults) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.TopicResults))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.TopicResults {
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.TopicResults[i].Name); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.TopicResults[i].Name); err != nil {
								return nil, err
							}
						}
					}
					// ResultsByPartition
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.TopicResults[i].ResultsByPartition) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.TopicResults[i].ResultsByPartition))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.TopicResults[i].ResultsByPartition {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.TopicResults[i].ResultsByPartition[i].PartitionIndex); err != nil {
									return nil, err
								}
							}
							// PartitionErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, structItem.TopicResults[i].ResultsByPartition[i].PartitionErrorCode); err != nil {
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
		items := make([]interface{}, len(m.ResultsByTransaction))
		for i := range m.ResultsByTransaction {
			items[i] = m.ResultsByTransaction[i]
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
	// ResultsByTopicV3AndBelow
	if version >= 0 && version <= 3 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(AddPartitionsToTxnTopicResult)
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
			// ResultsByPartition
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.ResultsByPartition) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.ResultsByPartition))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.ResultsByPartition {
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.ResultsByPartition[i].PartitionIndex); err != nil {
							return nil, err
						}
					}
					// PartitionErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(elemW, structItem.ResultsByPartition[i].PartitionErrorCode); err != nil {
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
		items := make([]interface{}, len(m.ResultsByTopicV3AndBelow))
		for i := range m.ResultsByTopicV3AndBelow {
			items[i] = m.ResultsByTopicV3AndBelow[i]
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

// Read reads a AddPartitionsToTxnResponse from an io.Reader for the given version.
func (m *AddPartitionsToTxnResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
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
	// ErrorCode
	if version >= 4 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// ResultsByTransaction
	if version >= 4 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem AddPartitionsToTxnResponseAddPartitionsToTxnResult
			elemR := bytes.NewReader(data)
			// TransactionalId
			if version >= 4 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TransactionalId = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TransactionalId = val
				}
			}
			// TopicResults
			if version >= 4 && version <= 999 {
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
				var tempElem AddPartitionsToTxnResponseAddPartitionsToTxnResult
				// TransactionalId
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TransactionalId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TransactionalId = val
					}
				}
				// TopicResults
				if version >= 4 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AddPartitionsToTxnTopicResult
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
						// ResultsByPartition
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
							var tempElem AddPartitionsToTxnTopicResult
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
							// ResultsByPartition
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AddPartitionsToTxnPartitionResult
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// PartitionErrorCode
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionErrorCode = val
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
										var tempElem AddPartitionsToTxnPartitionResult
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.PartitionErrorCode = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
									tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
										var tempElem AddPartitionsToTxnPartitionResult
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.PartitionErrorCode = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
									tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
							// ResultsByPartition
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ResultsByPartition) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ResultsByPartition))); err != nil {
										return err
									}
								}
								for i := range tempElem.ResultsByPartition {
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ResultsByPartition[i].PartitionIndex); err != nil {
											return err
										}
									}
									// PartitionErrorCode
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.ResultsByPartition[i].PartitionErrorCode); err != nil {
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
						tempElem.TopicResults = make([]AddPartitionsToTxnTopicResult, len(decoded))
						for i, item := range decoded {
							tempElem.TopicResults[i] = item.(AddPartitionsToTxnTopicResult)
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
							var tempElem AddPartitionsToTxnTopicResult
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
							// ResultsByPartition
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AddPartitionsToTxnPartitionResult
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// PartitionErrorCode
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionErrorCode = val
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
										var tempElem AddPartitionsToTxnPartitionResult
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.PartitionErrorCode = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
									tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
										var tempElem AddPartitionsToTxnPartitionResult
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.PartitionErrorCode = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
									tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
							// ResultsByPartition
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ResultsByPartition) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ResultsByPartition))); err != nil {
										return err
									}
								}
								for i := range tempElem.ResultsByPartition {
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ResultsByPartition[i].PartitionIndex); err != nil {
											return err
										}
									}
									// PartitionErrorCode
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.ResultsByPartition[i].PartitionErrorCode); err != nil {
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
						tempElem.TopicResults = make([]AddPartitionsToTxnTopicResult, len(decoded))
						for i, item := range decoded {
							tempElem.TopicResults[i] = item.(AddPartitionsToTxnTopicResult)
						}
					}
				}
				// TransactionalId
				if version >= 4 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TransactionalId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TransactionalId); err != nil {
							return err
						}
					}
				}
				// TopicResults
				if version >= 4 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.TopicResults) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicResults))); err != nil {
							return err
						}
					}
					for i := range tempElem.TopicResults {
						// Name
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.TopicResults[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.TopicResults[i].Name); err != nil {
									return err
								}
							}
						}
						// ResultsByPartition
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.TopicResults[i].ResultsByPartition) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicResults[i].ResultsByPartition))); err != nil {
									return err
								}
							}
							for i := range tempElem.TopicResults[i].ResultsByPartition {
								// PartitionIndex
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.TopicResults[i].ResultsByPartition[i].PartitionIndex); err != nil {
										return err
									}
								}
								// PartitionErrorCode
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.TopicResults[i].ResultsByPartition[i].PartitionErrorCode); err != nil {
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
			m.ResultsByTransaction = make([]AddPartitionsToTxnResponseAddPartitionsToTxnResult, len(decoded))
			for i, item := range decoded {
				m.ResultsByTransaction[i] = item.(AddPartitionsToTxnResponseAddPartitionsToTxnResult)
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
				var tempElem AddPartitionsToTxnResponseAddPartitionsToTxnResult
				// TransactionalId
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TransactionalId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TransactionalId = val
					}
				}
				// TopicResults
				if version >= 4 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AddPartitionsToTxnTopicResult
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
						// ResultsByPartition
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
							var tempElem AddPartitionsToTxnTopicResult
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
							// ResultsByPartition
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AddPartitionsToTxnPartitionResult
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// PartitionErrorCode
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionErrorCode = val
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
										var tempElem AddPartitionsToTxnPartitionResult
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.PartitionErrorCode = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
									tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
										var tempElem AddPartitionsToTxnPartitionResult
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.PartitionErrorCode = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
									tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
							// ResultsByPartition
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ResultsByPartition) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ResultsByPartition))); err != nil {
										return err
									}
								}
								for i := range tempElem.ResultsByPartition {
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ResultsByPartition[i].PartitionIndex); err != nil {
											return err
										}
									}
									// PartitionErrorCode
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.ResultsByPartition[i].PartitionErrorCode); err != nil {
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
						tempElem.TopicResults = make([]AddPartitionsToTxnTopicResult, len(decoded))
						for i, item := range decoded {
							tempElem.TopicResults[i] = item.(AddPartitionsToTxnTopicResult)
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
							var tempElem AddPartitionsToTxnTopicResult
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
							// ResultsByPartition
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AddPartitionsToTxnPartitionResult
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// PartitionErrorCode
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionErrorCode = val
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
										var tempElem AddPartitionsToTxnPartitionResult
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.PartitionErrorCode = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
									tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
										var tempElem AddPartitionsToTxnPartitionResult
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.PartitionErrorCode = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
									tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
									for i, item := range decoded {
										tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
							// ResultsByPartition
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ResultsByPartition) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ResultsByPartition))); err != nil {
										return err
									}
								}
								for i := range tempElem.ResultsByPartition {
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.ResultsByPartition[i].PartitionIndex); err != nil {
											return err
										}
									}
									// PartitionErrorCode
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.ResultsByPartition[i].PartitionErrorCode); err != nil {
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
						tempElem.TopicResults = make([]AddPartitionsToTxnTopicResult, len(decoded))
						for i, item := range decoded {
							tempElem.TopicResults[i] = item.(AddPartitionsToTxnTopicResult)
						}
					}
				}
				// TransactionalId
				if version >= 4 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TransactionalId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TransactionalId); err != nil {
							return err
						}
					}
				}
				// TopicResults
				if version >= 4 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.TopicResults) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicResults))); err != nil {
							return err
						}
					}
					for i := range tempElem.TopicResults {
						// Name
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.TopicResults[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.TopicResults[i].Name); err != nil {
									return err
								}
							}
						}
						// ResultsByPartition
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.TopicResults[i].ResultsByPartition) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicResults[i].ResultsByPartition))); err != nil {
									return err
								}
							}
							for i := range tempElem.TopicResults[i].ResultsByPartition {
								// PartitionIndex
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.TopicResults[i].ResultsByPartition[i].PartitionIndex); err != nil {
										return err
									}
								}
								// PartitionErrorCode
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.TopicResults[i].ResultsByPartition[i].PartitionErrorCode); err != nil {
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
			m.ResultsByTransaction = make([]AddPartitionsToTxnResponseAddPartitionsToTxnResult, len(decoded))
			for i, item := range decoded {
				m.ResultsByTransaction[i] = item.(AddPartitionsToTxnResponseAddPartitionsToTxnResult)
			}
		}
	}
	// ResultsByTopicV3AndBelow
	if version >= 0 && version <= 3 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem AddPartitionsToTxnTopicResult
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
			// ResultsByPartition
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
				var tempElem AddPartitionsToTxnTopicResult
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
				// ResultsByPartition
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AddPartitionsToTxnPartitionResult
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// PartitionErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionErrorCode = val
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
							var tempElem AddPartitionsToTxnPartitionResult
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// PartitionErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.PartitionErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// PartitionErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
						tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
						for i, item := range decoded {
							tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
							var tempElem AddPartitionsToTxnPartitionResult
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// PartitionErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.PartitionErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// PartitionErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
						tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
						for i, item := range decoded {
							tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
				// ResultsByPartition
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.ResultsByPartition) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.ResultsByPartition))); err != nil {
							return err
						}
					}
					for i := range tempElem.ResultsByPartition {
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.ResultsByPartition[i].PartitionIndex); err != nil {
								return err
							}
						}
						// PartitionErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.ResultsByPartition[i].PartitionErrorCode); err != nil {
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
			m.ResultsByTopicV3AndBelow = make([]AddPartitionsToTxnTopicResult, len(decoded))
			for i, item := range decoded {
				m.ResultsByTopicV3AndBelow[i] = item.(AddPartitionsToTxnTopicResult)
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
				var tempElem AddPartitionsToTxnTopicResult
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
				// ResultsByPartition
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AddPartitionsToTxnPartitionResult
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// PartitionErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionErrorCode = val
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
							var tempElem AddPartitionsToTxnPartitionResult
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// PartitionErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.PartitionErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// PartitionErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
						tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
						for i, item := range decoded {
							tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
							var tempElem AddPartitionsToTxnPartitionResult
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// PartitionErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.PartitionErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// PartitionErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.PartitionErrorCode); err != nil {
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
						tempElem.ResultsByPartition = make([]AddPartitionsToTxnPartitionResult, len(decoded))
						for i, item := range decoded {
							tempElem.ResultsByPartition[i] = item.(AddPartitionsToTxnPartitionResult)
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
				// ResultsByPartition
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.ResultsByPartition) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.ResultsByPartition))); err != nil {
							return err
						}
					}
					for i := range tempElem.ResultsByPartition {
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.ResultsByPartition[i].PartitionIndex); err != nil {
								return err
							}
						}
						// PartitionErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.ResultsByPartition[i].PartitionErrorCode); err != nil {
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
			m.ResultsByTopicV3AndBelow = make([]AddPartitionsToTxnTopicResult, len(decoded))
			for i, item := range decoded {
				m.ResultsByTopicV3AndBelow[i] = item.(AddPartitionsToTxnTopicResult)
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

// AddPartitionsToTxnResponseAddPartitionsToTxnResult represents Results categorized by transactional ID..
type AddPartitionsToTxnResponseAddPartitionsToTxnResult struct {
	// The transactional id corresponding to the transaction.
	TransactionalId string `json:"transactionalid" versions:"4-999"`
	// The results for each topic.
	TopicResults []AddPartitionsToTxnTopicResult `json:"topicresults" versions:"4-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AddPartitionsToTxnResponseAddPartitionsToTxnResult.
func (m *AddPartitionsToTxnResponseAddPartitionsToTxnResult) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AddPartitionsToTxnResponseAddPartitionsToTxnResult.
func (m *AddPartitionsToTxnResponseAddPartitionsToTxnResult) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for AddPartitionsToTxnResponse.
func (m *AddPartitionsToTxnResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AddPartitionsToTxnResponse.
func (m *AddPartitionsToTxnResponse) readTaggedFields(r io.Reader, version int16) error {
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
