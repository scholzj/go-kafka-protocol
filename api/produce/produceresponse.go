package produce

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ProduceResponseApiKey        = 0
	ProduceResponseHeaderVersion = 1
)

// ProduceResponse represents a response message.
type ProduceResponse struct {
	// Each produce response.
	Responses []ProduceResponseTopicProduceResponse `json:"responses" versions:"0-999"`
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"1-999"`
	// Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
	NodeEndpoints []ProduceResponseNodeEndpoint `json:"nodeendpoints" versions:"10-999" tag:"0"`
}

// Encode encodes a ProduceResponse to a byte slice for the given version.
func (m *ProduceResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ProduceResponse from a byte slice for the given version.
func (m *ProduceResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ProduceResponse to an io.Writer for the given version.
func (m *ProduceResponse) Write(w io.Writer, version int16) error {
	if version < 3 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// Responses
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ProduceResponseTopicProduceResponse)
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
			// PartitionResponses
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.PartitionResponses) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.PartitionResponses))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.PartitionResponses {
					// Index
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.PartitionResponses[i].Index); err != nil {
							return nil, err
						}
					}
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(elemW, structItem.PartitionResponses[i].ErrorCode); err != nil {
							return nil, err
						}
					}
					// BaseOffset
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(elemW, structItem.PartitionResponses[i].BaseOffset); err != nil {
							return nil, err
						}
					}
					// LogAppendTimeMs
					if version >= 2 && version <= 999 {
						if err := protocol.WriteInt64(elemW, structItem.PartitionResponses[i].LogAppendTimeMs); err != nil {
							return nil, err
						}
					}
					// LogStartOffset
					if version >= 5 && version <= 999 {
						if err := protocol.WriteInt64(elemW, structItem.PartitionResponses[i].LogStartOffset); err != nil {
							return nil, err
						}
					}
					// RecordErrors
					if version >= 8 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.PartitionResponses[i].RecordErrors) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.PartitionResponses[i].RecordErrors))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.PartitionResponses[i].RecordErrors {
							// BatchIndex
							if version >= 8 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.PartitionResponses[i].RecordErrors[i].BatchIndex); err != nil {
									return nil, err
								}
							}
							// BatchIndexErrorMessage
							if version >= 8 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, structItem.PartitionResponses[i].RecordErrors[i].BatchIndexErrorMessage); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, structItem.PartitionResponses[i].RecordErrors[i].BatchIndexErrorMessage); err != nil {
										return nil, err
									}
								}
							}
						}
					}
					// ErrorMessage
					if version >= 8 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.PartitionResponses[i].ErrorMessage); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.PartitionResponses[i].ErrorMessage); err != nil {
								return nil, err
							}
						}
					}
					// CurrentLeader
					if version >= 10 && version <= 999 {
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
		items := make([]interface{}, len(m.Responses))
		for i := range m.Responses {
			items[i] = m.Responses[i]
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
	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// NodeEndpoints
	if version >= 10 && version <= 999 {
	}
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a ProduceResponse from an io.Reader for the given version.
func (m *ProduceResponse) Read(r io.Reader, version int16) error {
	if version < 3 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// Responses
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ProduceResponseTopicProduceResponse
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
			// PartitionResponses
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
				var tempElem ProduceResponseTopicProduceResponse
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
				// PartitionResponses
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem ProduceResponsePartitionProduceResponse
						elemR := bytes.NewReader(data)
						// Index
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Index = val
						}
						// ErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ErrorCode = val
						}
						// BaseOffset
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.BaseOffset = val
						}
						// LogAppendTimeMs
						if version >= 2 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LogAppendTimeMs = val
						}
						// LogStartOffset
						if version >= 5 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LogStartOffset = val
						}
						// RecordErrors
						if version >= 8 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// ErrorMessage
						if version >= 8 && version <= 999 {
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
						// CurrentLeader
						if version >= 10 && version <= 999 {
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
							var tempElem ProduceResponsePartitionProduceResponse
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Index = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// BaseOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.BaseOffset = val
							}
							// LogAppendTimeMs
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogAppendTimeMs = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogStartOffset = val
							}
							// RecordErrors
							if version >= 8 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ProduceResponseBatchIndexAndErrorMessage
									elemR := bytes.NewReader(data)
									// BatchIndex
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BatchIndex = val
									}
									// BatchIndexErrorMessage
									if version >= 8 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.BatchIndexErrorMessage = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.BatchIndexErrorMessage = val
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
										var tempElem ProduceResponseBatchIndexAndErrorMessage
										// BatchIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BatchIndex = val
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											}
										}
										// BatchIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BatchIndex); err != nil {
												return err
											}
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
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
									tempElem.RecordErrors = make([]ProduceResponseBatchIndexAndErrorMessage, len(decoded))
									for i, item := range decoded {
										tempElem.RecordErrors[i] = item.(ProduceResponseBatchIndexAndErrorMessage)
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
										var tempElem ProduceResponseBatchIndexAndErrorMessage
										// BatchIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BatchIndex = val
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											}
										}
										// BatchIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BatchIndex); err != nil {
												return err
											}
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
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
									tempElem.RecordErrors = make([]ProduceResponseBatchIndexAndErrorMessage, len(decoded))
									for i, item := range decoded {
										tempElem.RecordErrors[i] = item.(ProduceResponseBatchIndexAndErrorMessage)
									}
								}
							}
							// ErrorMessage
							if version >= 8 && version <= 999 {
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
							// CurrentLeader
							if version >= 10 && version <= 999 {
							}
							// Index
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Index); err != nil {
									return err
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// BaseOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.BaseOffset); err != nil {
									return err
								}
							}
							// LogAppendTimeMs
							if version >= 2 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogAppendTimeMs); err != nil {
									return err
								}
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogStartOffset); err != nil {
									return err
								}
							}
							// RecordErrors
							if version >= 8 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.RecordErrors) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.RecordErrors))); err != nil {
										return err
									}
								}
								for i := range tempElem.RecordErrors {
									// BatchIndex
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.RecordErrors[i].BatchIndex); err != nil {
											return err
										}
									}
									// BatchIndexErrorMessage
									if version >= 8 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.RecordErrors[i].BatchIndexErrorMessage); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.RecordErrors[i].BatchIndexErrorMessage); err != nil {
												return err
											}
										}
									}
								}
							}
							// ErrorMessage
							if version >= 8 && version <= 999 {
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
							// CurrentLeader
							if version >= 10 && version <= 999 {
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
						tempElem.PartitionResponses = make([]ProduceResponsePartitionProduceResponse, len(decoded))
						for i, item := range decoded {
							tempElem.PartitionResponses[i] = item.(ProduceResponsePartitionProduceResponse)
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
							var tempElem ProduceResponsePartitionProduceResponse
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Index = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// BaseOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.BaseOffset = val
							}
							// LogAppendTimeMs
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogAppendTimeMs = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogStartOffset = val
							}
							// RecordErrors
							if version >= 8 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ProduceResponseBatchIndexAndErrorMessage
									elemR := bytes.NewReader(data)
									// BatchIndex
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BatchIndex = val
									}
									// BatchIndexErrorMessage
									if version >= 8 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.BatchIndexErrorMessage = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.BatchIndexErrorMessage = val
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
										var tempElem ProduceResponseBatchIndexAndErrorMessage
										// BatchIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BatchIndex = val
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											}
										}
										// BatchIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BatchIndex); err != nil {
												return err
											}
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
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
									tempElem.RecordErrors = make([]ProduceResponseBatchIndexAndErrorMessage, len(decoded))
									for i, item := range decoded {
										tempElem.RecordErrors[i] = item.(ProduceResponseBatchIndexAndErrorMessage)
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
										var tempElem ProduceResponseBatchIndexAndErrorMessage
										// BatchIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BatchIndex = val
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											}
										}
										// BatchIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BatchIndex); err != nil {
												return err
											}
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
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
									tempElem.RecordErrors = make([]ProduceResponseBatchIndexAndErrorMessage, len(decoded))
									for i, item := range decoded {
										tempElem.RecordErrors[i] = item.(ProduceResponseBatchIndexAndErrorMessage)
									}
								}
							}
							// ErrorMessage
							if version >= 8 && version <= 999 {
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
							// CurrentLeader
							if version >= 10 && version <= 999 {
							}
							// Index
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Index); err != nil {
									return err
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// BaseOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.BaseOffset); err != nil {
									return err
								}
							}
							// LogAppendTimeMs
							if version >= 2 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogAppendTimeMs); err != nil {
									return err
								}
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogStartOffset); err != nil {
									return err
								}
							}
							// RecordErrors
							if version >= 8 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.RecordErrors) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.RecordErrors))); err != nil {
										return err
									}
								}
								for i := range tempElem.RecordErrors {
									// BatchIndex
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.RecordErrors[i].BatchIndex); err != nil {
											return err
										}
									}
									// BatchIndexErrorMessage
									if version >= 8 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.RecordErrors[i].BatchIndexErrorMessage); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.RecordErrors[i].BatchIndexErrorMessage); err != nil {
												return err
											}
										}
									}
								}
							}
							// ErrorMessage
							if version >= 8 && version <= 999 {
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
							// CurrentLeader
							if version >= 10 && version <= 999 {
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
						tempElem.PartitionResponses = make([]ProduceResponsePartitionProduceResponse, len(decoded))
						for i, item := range decoded {
							tempElem.PartitionResponses[i] = item.(ProduceResponsePartitionProduceResponse)
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
				// PartitionResponses
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.PartitionResponses) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.PartitionResponses))); err != nil {
							return err
						}
					}
					for i := range tempElem.PartitionResponses {
						// Index
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.PartitionResponses[i].Index); err != nil {
								return err
							}
						}
						// ErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.PartitionResponses[i].ErrorCode); err != nil {
								return err
							}
						}
						// BaseOffset
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.PartitionResponses[i].BaseOffset); err != nil {
								return err
							}
						}
						// LogAppendTimeMs
						if version >= 2 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.PartitionResponses[i].LogAppendTimeMs); err != nil {
								return err
							}
						}
						// LogStartOffset
						if version >= 5 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.PartitionResponses[i].LogStartOffset); err != nil {
								return err
							}
						}
						// RecordErrors
						if version >= 8 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.PartitionResponses[i].RecordErrors) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.PartitionResponses[i].RecordErrors))); err != nil {
									return err
								}
							}
							for i := range tempElem.PartitionResponses[i].RecordErrors {
								// BatchIndex
								if version >= 8 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.PartitionResponses[i].RecordErrors[i].BatchIndex); err != nil {
										return err
									}
								}
								// BatchIndexErrorMessage
								if version >= 8 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactNullableString(elemW, tempElem.PartitionResponses[i].RecordErrors[i].BatchIndexErrorMessage); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteNullableString(elemW, tempElem.PartitionResponses[i].RecordErrors[i].BatchIndexErrorMessage); err != nil {
											return err
										}
									}
								}
							}
						}
						// ErrorMessage
						if version >= 8 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.PartitionResponses[i].ErrorMessage); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.PartitionResponses[i].ErrorMessage); err != nil {
									return err
								}
							}
						}
						// CurrentLeader
						if version >= 10 && version <= 999 {
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
			m.Responses = make([]ProduceResponseTopicProduceResponse, len(decoded))
			for i, item := range decoded {
				m.Responses[i] = item.(ProduceResponseTopicProduceResponse)
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
				var tempElem ProduceResponseTopicProduceResponse
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
				// PartitionResponses
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem ProduceResponsePartitionProduceResponse
						elemR := bytes.NewReader(data)
						// Index
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Index = val
						}
						// ErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ErrorCode = val
						}
						// BaseOffset
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.BaseOffset = val
						}
						// LogAppendTimeMs
						if version >= 2 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LogAppendTimeMs = val
						}
						// LogStartOffset
						if version >= 5 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LogStartOffset = val
						}
						// RecordErrors
						if version >= 8 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// ErrorMessage
						if version >= 8 && version <= 999 {
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
						// CurrentLeader
						if version >= 10 && version <= 999 {
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
							var tempElem ProduceResponsePartitionProduceResponse
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Index = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// BaseOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.BaseOffset = val
							}
							// LogAppendTimeMs
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogAppendTimeMs = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogStartOffset = val
							}
							// RecordErrors
							if version >= 8 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ProduceResponseBatchIndexAndErrorMessage
									elemR := bytes.NewReader(data)
									// BatchIndex
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BatchIndex = val
									}
									// BatchIndexErrorMessage
									if version >= 8 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.BatchIndexErrorMessage = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.BatchIndexErrorMessage = val
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
										var tempElem ProduceResponseBatchIndexAndErrorMessage
										// BatchIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BatchIndex = val
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											}
										}
										// BatchIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BatchIndex); err != nil {
												return err
											}
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
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
									tempElem.RecordErrors = make([]ProduceResponseBatchIndexAndErrorMessage, len(decoded))
									for i, item := range decoded {
										tempElem.RecordErrors[i] = item.(ProduceResponseBatchIndexAndErrorMessage)
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
										var tempElem ProduceResponseBatchIndexAndErrorMessage
										// BatchIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BatchIndex = val
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											}
										}
										// BatchIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BatchIndex); err != nil {
												return err
											}
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
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
									tempElem.RecordErrors = make([]ProduceResponseBatchIndexAndErrorMessage, len(decoded))
									for i, item := range decoded {
										tempElem.RecordErrors[i] = item.(ProduceResponseBatchIndexAndErrorMessage)
									}
								}
							}
							// ErrorMessage
							if version >= 8 && version <= 999 {
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
							// CurrentLeader
							if version >= 10 && version <= 999 {
							}
							// Index
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Index); err != nil {
									return err
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// BaseOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.BaseOffset); err != nil {
									return err
								}
							}
							// LogAppendTimeMs
							if version >= 2 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogAppendTimeMs); err != nil {
									return err
								}
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogStartOffset); err != nil {
									return err
								}
							}
							// RecordErrors
							if version >= 8 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.RecordErrors) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.RecordErrors))); err != nil {
										return err
									}
								}
								for i := range tempElem.RecordErrors {
									// BatchIndex
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.RecordErrors[i].BatchIndex); err != nil {
											return err
										}
									}
									// BatchIndexErrorMessage
									if version >= 8 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.RecordErrors[i].BatchIndexErrorMessage); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.RecordErrors[i].BatchIndexErrorMessage); err != nil {
												return err
											}
										}
									}
								}
							}
							// ErrorMessage
							if version >= 8 && version <= 999 {
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
							// CurrentLeader
							if version >= 10 && version <= 999 {
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
						tempElem.PartitionResponses = make([]ProduceResponsePartitionProduceResponse, len(decoded))
						for i, item := range decoded {
							tempElem.PartitionResponses[i] = item.(ProduceResponsePartitionProduceResponse)
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
							var tempElem ProduceResponsePartitionProduceResponse
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Index = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// BaseOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.BaseOffset = val
							}
							// LogAppendTimeMs
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogAppendTimeMs = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogStartOffset = val
							}
							// RecordErrors
							if version >= 8 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ProduceResponseBatchIndexAndErrorMessage
									elemR := bytes.NewReader(data)
									// BatchIndex
									if version >= 8 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.BatchIndex = val
									}
									// BatchIndexErrorMessage
									if version >= 8 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.BatchIndexErrorMessage = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.BatchIndexErrorMessage = val
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
										var tempElem ProduceResponseBatchIndexAndErrorMessage
										// BatchIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BatchIndex = val
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											}
										}
										// BatchIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BatchIndex); err != nil {
												return err
											}
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
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
									tempElem.RecordErrors = make([]ProduceResponseBatchIndexAndErrorMessage, len(decoded))
									for i, item := range decoded {
										tempElem.RecordErrors[i] = item.(ProduceResponseBatchIndexAndErrorMessage)
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
										var tempElem ProduceResponseBatchIndexAndErrorMessage
										// BatchIndex
										if version >= 8 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.BatchIndex = val
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.BatchIndexErrorMessage = val
											}
										}
										// BatchIndex
										if version >= 8 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.BatchIndex); err != nil {
												return err
											}
										}
										// BatchIndexErrorMessage
										if version >= 8 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.BatchIndexErrorMessage); err != nil {
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
									tempElem.RecordErrors = make([]ProduceResponseBatchIndexAndErrorMessage, len(decoded))
									for i, item := range decoded {
										tempElem.RecordErrors[i] = item.(ProduceResponseBatchIndexAndErrorMessage)
									}
								}
							}
							// ErrorMessage
							if version >= 8 && version <= 999 {
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
							// CurrentLeader
							if version >= 10 && version <= 999 {
							}
							// Index
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Index); err != nil {
									return err
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// BaseOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.BaseOffset); err != nil {
									return err
								}
							}
							// LogAppendTimeMs
							if version >= 2 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogAppendTimeMs); err != nil {
									return err
								}
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogStartOffset); err != nil {
									return err
								}
							}
							// RecordErrors
							if version >= 8 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.RecordErrors) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.RecordErrors))); err != nil {
										return err
									}
								}
								for i := range tempElem.RecordErrors {
									// BatchIndex
									if version >= 8 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.RecordErrors[i].BatchIndex); err != nil {
											return err
										}
									}
									// BatchIndexErrorMessage
									if version >= 8 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.RecordErrors[i].BatchIndexErrorMessage); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.RecordErrors[i].BatchIndexErrorMessage); err != nil {
												return err
											}
										}
									}
								}
							}
							// ErrorMessage
							if version >= 8 && version <= 999 {
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
							// CurrentLeader
							if version >= 10 && version <= 999 {
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
						tempElem.PartitionResponses = make([]ProduceResponsePartitionProduceResponse, len(decoded))
						for i, item := range decoded {
							tempElem.PartitionResponses[i] = item.(ProduceResponsePartitionProduceResponse)
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
				// PartitionResponses
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.PartitionResponses) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.PartitionResponses))); err != nil {
							return err
						}
					}
					for i := range tempElem.PartitionResponses {
						// Index
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.PartitionResponses[i].Index); err != nil {
								return err
							}
						}
						// ErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.PartitionResponses[i].ErrorCode); err != nil {
								return err
							}
						}
						// BaseOffset
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.PartitionResponses[i].BaseOffset); err != nil {
								return err
							}
						}
						// LogAppendTimeMs
						if version >= 2 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.PartitionResponses[i].LogAppendTimeMs); err != nil {
								return err
							}
						}
						// LogStartOffset
						if version >= 5 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.PartitionResponses[i].LogStartOffset); err != nil {
								return err
							}
						}
						// RecordErrors
						if version >= 8 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.PartitionResponses[i].RecordErrors) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.PartitionResponses[i].RecordErrors))); err != nil {
									return err
								}
							}
							for i := range tempElem.PartitionResponses[i].RecordErrors {
								// BatchIndex
								if version >= 8 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.PartitionResponses[i].RecordErrors[i].BatchIndex); err != nil {
										return err
									}
								}
								// BatchIndexErrorMessage
								if version >= 8 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactNullableString(elemW, tempElem.PartitionResponses[i].RecordErrors[i].BatchIndexErrorMessage); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteNullableString(elemW, tempElem.PartitionResponses[i].RecordErrors[i].BatchIndexErrorMessage); err != nil {
											return err
										}
									}
								}
							}
						}
						// ErrorMessage
						if version >= 8 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.PartitionResponses[i].ErrorMessage); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.PartitionResponses[i].ErrorMessage); err != nil {
									return err
								}
							}
						}
						// CurrentLeader
						if version >= 10 && version <= 999 {
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
			m.Responses = make([]ProduceResponseTopicProduceResponse, len(decoded))
			for i, item := range decoded {
				m.Responses[i] = item.(ProduceResponseTopicProduceResponse)
			}
		}
	}
	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// NodeEndpoints
	if version >= 10 && version <= 999 {
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// ProduceResponseTopicProduceResponse represents Each produce response..
type ProduceResponseTopicProduceResponse struct {
	// The topic name.
	Name string `json:"name" versions:"0-12"`
	// The unique topic ID
	TopicId uuid.UUID `json:"topicid" versions:"13-999"`
	// Each partition that we produced to within the topic.
	PartitionResponses []ProduceResponsePartitionProduceResponse `json:"partitionresponses" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ProduceResponseTopicProduceResponse.
func (m *ProduceResponseTopicProduceResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ProduceResponseTopicProduceResponse.
func (m *ProduceResponseTopicProduceResponse) readTaggedFields(r io.Reader, version int16) error {
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

// ProduceResponsePartitionProduceResponse represents Each partition that we produced to within the topic..
type ProduceResponsePartitionProduceResponse struct {
	// The partition index.
	Index int32 `json:"index" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The base offset.
	BaseOffset int64 `json:"baseoffset" versions:"0-999"`
	// The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
	LogAppendTimeMs int64 `json:"logappendtimems" versions:"2-999"`
	// The log start offset.
	LogStartOffset int64 `json:"logstartoffset" versions:"5-999"`
	// The batch indices of records that caused the batch to be dropped.
	RecordErrors []ProduceResponseBatchIndexAndErrorMessage `json:"recorderrors" versions:"8-999"`
	// The global error message summarizing the common root cause of the records that caused the batch to be dropped.
	ErrorMessage *string `json:"errormessage" versions:"8-999"`
	// The leader broker that the producer should use for future requests.
	CurrentLeader ProduceResponseLeaderIdAndEpoch `json:"currentleader" versions:"10-999" tag:"0"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ProduceResponsePartitionProduceResponse.
func (m *ProduceResponsePartitionProduceResponse) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	// CurrentLeader (tag 0)
	if version >= 10 {
		if true {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(0)); err != nil {
				return err
			}
			// LeaderId
			if version >= 10 && version <= 999 {
				if err := protocol.WriteInt32(w, m.CurrentLeader.LeaderId); err != nil {
					return err
				}
			}
			// LeaderEpoch
			if version >= 10 && version <= 999 {
				if err := protocol.WriteInt32(w, m.CurrentLeader.LeaderEpoch); err != nil {
					return err
				}
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

// readTaggedFields reads tagged fields for ProduceResponsePartitionProduceResponse.
func (m *ProduceResponsePartitionProduceResponse) readTaggedFields(r io.Reader, version int16) error {
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
		case 0: // CurrentLeader
			if version >= 10 {
				// LeaderId
				if version >= 10 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.CurrentLeader.LeaderId = val
				}
				// LeaderEpoch
				if version >= 10 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.CurrentLeader.LeaderEpoch = val
				}
			}
		default:
			// Unknown tag, skip it
		}
	}

	return nil
}

// ProduceResponseBatchIndexAndErrorMessage represents The batch indices of records that caused the batch to be dropped..
type ProduceResponseBatchIndexAndErrorMessage struct {
	// The batch index of the record that caused the batch to be dropped.
	BatchIndex int32 `json:"batchindex" versions:"8-999"`
	// The error message of the record that caused the batch to be dropped.
	BatchIndexErrorMessage *string `json:"batchindexerrormessage" versions:"8-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ProduceResponseBatchIndexAndErrorMessage.
func (m *ProduceResponseBatchIndexAndErrorMessage) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ProduceResponseBatchIndexAndErrorMessage.
func (m *ProduceResponseBatchIndexAndErrorMessage) readTaggedFields(r io.Reader, version int16) error {
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

// ProduceResponseLeaderIdAndEpoch represents The leader broker that the producer should use for future requests..
type ProduceResponseLeaderIdAndEpoch struct {
	// The ID of the current leader or -1 if the leader is unknown.
	LeaderId int32 `json:"leaderid" versions:"10-999"`
	// The latest known leader epoch.
	LeaderEpoch int32 `json:"leaderepoch" versions:"10-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ProduceResponseLeaderIdAndEpoch.
func (m *ProduceResponseLeaderIdAndEpoch) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ProduceResponseLeaderIdAndEpoch.
func (m *ProduceResponseLeaderIdAndEpoch) readTaggedFields(r io.Reader, version int16) error {
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

// ProduceResponseNodeEndpoint represents Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER..
type ProduceResponseNodeEndpoint struct {
	// The ID of the associated node.
	NodeId int32 `json:"nodeid" versions:"10-999"`
	// The node's hostname.
	Host string `json:"host" versions:"10-999"`
	// The node's port.
	Port int32 `json:"port" versions:"10-999"`
	// The rack of the node, or null if it has not been assigned to a rack.
	Rack *string `json:"rack" versions:"10-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ProduceResponseNodeEndpoint.
func (m *ProduceResponseNodeEndpoint) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ProduceResponseNodeEndpoint.
func (m *ProduceResponseNodeEndpoint) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for ProduceResponse.
func (m *ProduceResponse) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	isFlexible := version >= 9

	// NodeEndpoints (tag 0)
	if version >= 10 {
		if m.NodeEndpoints != nil && len(m.NodeEndpoints) > 0 {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(0)); err != nil {
				return err
			}
			// Array in tagged field
			length := uint32(len(m.NodeEndpoints) + 1)
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, length); err != nil {
				return err
			}
			for i := range m.NodeEndpoints {
				// NodeId
				if version >= 10 && version <= 999 {
					if err := protocol.WriteInt32(w, m.NodeEndpoints[i].NodeId); err != nil {
						return err
					}
				}
				// Host
				if version >= 10 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(w, m.NodeEndpoints[i].Host); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(w, m.NodeEndpoints[i].Host); err != nil {
							return err
						}
					}
				}
				// Port
				if version >= 10 && version <= 999 {
					if err := protocol.WriteInt32(w, m.NodeEndpoints[i].Port); err != nil {
						return err
					}
				}
				// Rack
				if version >= 10 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(w, m.NodeEndpoints[i].Rack); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(w, m.NodeEndpoints[i].Rack); err != nil {
							return err
						}
					}
				}
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

// readTaggedFields reads tagged fields for ProduceResponse.
func (m *ProduceResponse) readTaggedFields(r io.Reader, version int16) error {
	isFlexible := version >= 9

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
		case 0: // NodeEndpoints
			if version >= 10 {
				// Array in tagged field
				length, err := protocol.ReadVaruint32(r)
				if err != nil {
					return err
				}
				if length == 0 {
					m.NodeEndpoints = nil
				} else {
					if length < 1 {
						return errors.New("invalid compact array length")
					}
					m.NodeEndpoints = make([]ProduceResponseNodeEndpoint, length-1)
					for i := uint32(0); i < length-1; i++ {
						// NodeId
						if version >= 10 && version <= 999 {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.NodeEndpoints[i].NodeId = val
						}
						// Host
						if version >= 10 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(r)
								if err != nil {
									return err
								}
								m.NodeEndpoints[i].Host = val
							} else {
								val, err := protocol.ReadString(r)
								if err != nil {
									return err
								}
								m.NodeEndpoints[i].Host = val
							}
						}
						// Port
						if version >= 10 && version <= 999 {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.NodeEndpoints[i].Port = val
						}
						// Rack
						if version >= 10 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(r)
								if err != nil {
									return err
								}
								m.NodeEndpoints[i].Rack = val
							} else {
								val, err := protocol.ReadNullableString(r)
								if err != nil {
									return err
								}
								m.NodeEndpoints[i].Rack = val
							}
						}
					}
				}
			}
		default:
			// Unknown tag, skip it
			// Read and discard the field data
			// For now, we'll need to know the type to skip properly
			// This is a simplified implementation
		}
	}

	return nil
}
