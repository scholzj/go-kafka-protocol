package fetch

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	FetchResponseApiKey        = 1
	FetchResponseHeaderVersion = 1
)

// FetchResponse represents a response message.
type FetchResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"1-999"`
	// The top level response error code.
	ErrorCode int16 `json:"errorcode" versions:"7-999"`
	// The fetch session ID, or 0 if this is not part of a fetch session.
	SessionId int32 `json:"sessionid" versions:"7-999"`
	// The response topics.
	Responses []FetchResponseFetchableTopicResponse `json:"responses" versions:"0-999"`
	// Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
	NodeEndpoints []FetchResponseNodeEndpoint `json:"nodeendpoints" versions:"16-999" tag:"0"`
}

// Encode encodes a FetchResponse to a byte slice for the given version.
func (m *FetchResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a FetchResponse from a byte slice for the given version.
func (m *FetchResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a FetchResponse to an io.Writer for the given version.
func (m *FetchResponse) Write(w io.Writer, version int16) error {
	if version < 4 || version > 18 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 12 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// ErrorCode
	if version >= 7 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// SessionId
	if version >= 7 && version <= 999 {
		if err := protocol.WriteInt32(w, m.SessionId); err != nil {
			return err
		}
	}
	// Responses
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(FetchResponseFetchableTopicResponse)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Topic
			if version >= 0 && version <= 12 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Topic); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Topic); err != nil {
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
					// HighWatermark
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(elemW, structItem.Partitions[i].HighWatermark); err != nil {
							return nil, err
						}
					}
					// LastStableOffset
					if version >= 4 && version <= 999 {
						if err := protocol.WriteInt64(elemW, structItem.Partitions[i].LastStableOffset); err != nil {
							return nil, err
						}
					}
					// LogStartOffset
					if version >= 5 && version <= 999 {
						if err := protocol.WriteInt64(elemW, structItem.Partitions[i].LogStartOffset); err != nil {
							return nil, err
						}
					}
					// DivergingEpoch
					if version >= 12 && version <= 999 {
					}
					// CurrentLeader
					if version >= 12 && version <= 999 {
					}
					// SnapshotId
					if version >= 12 && version <= 999 {
					}
					// AbortedTransactions
					if version >= 4 && version <= 999 {
						if structItem.Partitions[i].AbortedTransactions == nil {
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
								length := uint32(len(structItem.Partitions[i].AbortedTransactions) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return nil, err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions[i].AbortedTransactions))); err != nil {
									return nil, err
								}
							}
							for i := range structItem.Partitions[i].AbortedTransactions {
								// ProducerId
								if version >= 4 && version <= 999 {
									if err := protocol.WriteInt64(elemW, structItem.Partitions[i].AbortedTransactions[i].ProducerId); err != nil {
										return nil, err
									}
								}
								// FirstOffset
								if version >= 4 && version <= 999 {
									if err := protocol.WriteInt64(elemW, structItem.Partitions[i].AbortedTransactions[i].FirstOffset); err != nil {
										return nil, err
									}
								}
							}
						}
					}
					// PreferredReadReplica
					if version >= 11 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].PreferredReadReplica); err != nil {
							return nil, err
						}
					}
					// Records
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableBytes(elemW, structItem.Partitions[i].Records); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableBytes(elemW, structItem.Partitions[i].Records); err != nil {
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
	// NodeEndpoints
	if version >= 16 && version <= 999 {
	}
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a FetchResponse from an io.Reader for the given version.
func (m *FetchResponse) Read(r io.Reader, version int16) error {
	if version < 4 || version > 18 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 12 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// ErrorCode
	if version >= 7 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// SessionId
	if version >= 7 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.SessionId = val
	}
	// Responses
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem FetchResponseFetchableTopicResponse
			elemR := bytes.NewReader(data)
			// Topic
			if version >= 0 && version <= 12 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Topic = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Topic = val
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
				var tempElem FetchResponseFetchableTopicResponse
				// Topic
				if version >= 0 && version <= 12 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Topic = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Topic = val
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
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem FetchResponsePartitionData
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
						// HighWatermark
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.HighWatermark = val
						}
						// LastStableOffset
						if version >= 4 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LastStableOffset = val
						}
						// LogStartOffset
						if version >= 5 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LogStartOffset = val
						}
						// DivergingEpoch
						if version >= 12 && version <= 999 {
						}
						// CurrentLeader
						if version >= 12 && version <= 999 {
						}
						// SnapshotId
						if version >= 12 && version <= 999 {
						}
						// AbortedTransactions
						if version >= 4 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// PreferredReadReplica
						if version >= 11 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PreferredReadReplica = val
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
							var tempElem FetchResponsePartitionData
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
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.HighWatermark = val
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LastStableOffset = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogStartOffset = val
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem FetchResponseAbortedTransaction
									elemR := bytes.NewReader(data)
									// ProducerId
									if version >= 4 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerId = val
									}
									// FirstOffset
									if version >= 4 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
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
										tempElem.AbortedTransactions = nil
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
										var tempElem FetchResponseAbortedTransaction
										// ProducerId
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
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
									tempElem.AbortedTransactions = make([]FetchResponseAbortedTransaction, len(decoded))
									for i, item := range decoded {
										tempElem.AbortedTransactions[i] = item.(FetchResponseAbortedTransaction)
									}
								} else {
									length, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									if length == -1 {
										tempElem.AbortedTransactions = nil
										return nil
									}
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem FetchResponseAbortedTransaction
										// ProducerId
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
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
									tempElem.AbortedTransactions = make([]FetchResponseAbortedTransaction, len(decoded))
									for i, item := range decoded {
										tempElem.AbortedTransactions[i] = item.(FetchResponseAbortedTransaction)
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PreferredReadReplica = val
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
							// HighWatermark
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.HighWatermark); err != nil {
									return err
								}
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LastStableOffset); err != nil {
									return err
								}
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogStartOffset); err != nil {
									return err
								}
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								if tempElem.AbortedTransactions == nil {
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
										length := uint32(len(tempElem.AbortedTransactions) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.AbortedTransactions))); err != nil {
											return err
										}
									}
									for i := range tempElem.AbortedTransactions {
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.AbortedTransactions[i].ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.AbortedTransactions[i].FirstOffset); err != nil {
												return err
											}
										}
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PreferredReadReplica); err != nil {
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
						tempElem.Partitions = make([]FetchResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(FetchResponsePartitionData)
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
							var tempElem FetchResponsePartitionData
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
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.HighWatermark = val
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LastStableOffset = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogStartOffset = val
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem FetchResponseAbortedTransaction
									elemR := bytes.NewReader(data)
									// ProducerId
									if version >= 4 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerId = val
									}
									// FirstOffset
									if version >= 4 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
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
										tempElem.AbortedTransactions = nil
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
										var tempElem FetchResponseAbortedTransaction
										// ProducerId
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
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
									tempElem.AbortedTransactions = make([]FetchResponseAbortedTransaction, len(decoded))
									for i, item := range decoded {
										tempElem.AbortedTransactions[i] = item.(FetchResponseAbortedTransaction)
									}
								} else {
									length, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									if length == -1 {
										tempElem.AbortedTransactions = nil
										return nil
									}
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem FetchResponseAbortedTransaction
										// ProducerId
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
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
									tempElem.AbortedTransactions = make([]FetchResponseAbortedTransaction, len(decoded))
									for i, item := range decoded {
										tempElem.AbortedTransactions[i] = item.(FetchResponseAbortedTransaction)
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PreferredReadReplica = val
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
							// HighWatermark
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.HighWatermark); err != nil {
									return err
								}
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LastStableOffset); err != nil {
									return err
								}
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogStartOffset); err != nil {
									return err
								}
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								if tempElem.AbortedTransactions == nil {
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
										length := uint32(len(tempElem.AbortedTransactions) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.AbortedTransactions))); err != nil {
											return err
										}
									}
									for i := range tempElem.AbortedTransactions {
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.AbortedTransactions[i].ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.AbortedTransactions[i].FirstOffset); err != nil {
												return err
											}
										}
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PreferredReadReplica); err != nil {
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
						tempElem.Partitions = make([]FetchResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(FetchResponsePartitionData)
						}
					}
				}
				// Topic
				if version >= 0 && version <= 12 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Topic); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Topic); err != nil {
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
						// HighWatermark
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].HighWatermark); err != nil {
								return err
							}
						}
						// LastStableOffset
						if version >= 4 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].LastStableOffset); err != nil {
								return err
							}
						}
						// LogStartOffset
						if version >= 5 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].LogStartOffset); err != nil {
								return err
							}
						}
						// DivergingEpoch
						if version >= 12 && version <= 999 {
						}
						// CurrentLeader
						if version >= 12 && version <= 999 {
						}
						// SnapshotId
						if version >= 12 && version <= 999 {
						}
						// AbortedTransactions
						if version >= 4 && version <= 999 {
							if tempElem.Partitions[i].AbortedTransactions == nil {
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
									length := uint32(len(tempElem.Partitions[i].AbortedTransactions) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].AbortedTransactions))); err != nil {
										return err
									}
								}
								for i := range tempElem.Partitions[i].AbortedTransactions {
									// ProducerId
									if version >= 4 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AbortedTransactions[i].ProducerId); err != nil {
											return err
										}
									}
									// FirstOffset
									if version >= 4 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AbortedTransactions[i].FirstOffset); err != nil {
											return err
										}
									}
								}
							}
						}
						// PreferredReadReplica
						if version >= 11 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PreferredReadReplica); err != nil {
								return err
							}
						}
						// Records
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Partitions[i].Records); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableBytes(elemW, tempElem.Partitions[i].Records); err != nil {
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
			m.Responses = make([]FetchResponseFetchableTopicResponse, len(decoded))
			for i, item := range decoded {
				m.Responses[i] = item.(FetchResponseFetchableTopicResponse)
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
				var tempElem FetchResponseFetchableTopicResponse
				// Topic
				if version >= 0 && version <= 12 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Topic = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Topic = val
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
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem FetchResponsePartitionData
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
						// HighWatermark
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.HighWatermark = val
						}
						// LastStableOffset
						if version >= 4 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LastStableOffset = val
						}
						// LogStartOffset
						if version >= 5 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LogStartOffset = val
						}
						// DivergingEpoch
						if version >= 12 && version <= 999 {
						}
						// CurrentLeader
						if version >= 12 && version <= 999 {
						}
						// SnapshotId
						if version >= 12 && version <= 999 {
						}
						// AbortedTransactions
						if version >= 4 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// PreferredReadReplica
						if version >= 11 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PreferredReadReplica = val
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
							var tempElem FetchResponsePartitionData
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
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.HighWatermark = val
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LastStableOffset = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogStartOffset = val
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem FetchResponseAbortedTransaction
									elemR := bytes.NewReader(data)
									// ProducerId
									if version >= 4 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerId = val
									}
									// FirstOffset
									if version >= 4 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
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
										tempElem.AbortedTransactions = nil
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
										var tempElem FetchResponseAbortedTransaction
										// ProducerId
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
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
									tempElem.AbortedTransactions = make([]FetchResponseAbortedTransaction, len(decoded))
									for i, item := range decoded {
										tempElem.AbortedTransactions[i] = item.(FetchResponseAbortedTransaction)
									}
								} else {
									length, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									if length == -1 {
										tempElem.AbortedTransactions = nil
										return nil
									}
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem FetchResponseAbortedTransaction
										// ProducerId
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
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
									tempElem.AbortedTransactions = make([]FetchResponseAbortedTransaction, len(decoded))
									for i, item := range decoded {
										tempElem.AbortedTransactions[i] = item.(FetchResponseAbortedTransaction)
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PreferredReadReplica = val
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
							// HighWatermark
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.HighWatermark); err != nil {
									return err
								}
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LastStableOffset); err != nil {
									return err
								}
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogStartOffset); err != nil {
									return err
								}
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								if tempElem.AbortedTransactions == nil {
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
										length := uint32(len(tempElem.AbortedTransactions) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.AbortedTransactions))); err != nil {
											return err
										}
									}
									for i := range tempElem.AbortedTransactions {
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.AbortedTransactions[i].ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.AbortedTransactions[i].FirstOffset); err != nil {
												return err
											}
										}
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PreferredReadReplica); err != nil {
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
						tempElem.Partitions = make([]FetchResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(FetchResponsePartitionData)
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
							var tempElem FetchResponsePartitionData
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
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.HighWatermark = val
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LastStableOffset = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LogStartOffset = val
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem FetchResponseAbortedTransaction
									elemR := bytes.NewReader(data)
									// ProducerId
									if version >= 4 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ProducerId = val
									}
									// FirstOffset
									if version >= 4 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
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
										tempElem.AbortedTransactions = nil
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
										var tempElem FetchResponseAbortedTransaction
										// ProducerId
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
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
									tempElem.AbortedTransactions = make([]FetchResponseAbortedTransaction, len(decoded))
									for i, item := range decoded {
										tempElem.AbortedTransactions[i] = item.(FetchResponseAbortedTransaction)
									}
								} else {
									length, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									if length == -1 {
										tempElem.AbortedTransactions = nil
										return nil
									}
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem FetchResponseAbortedTransaction
										// ProducerId
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.ProducerId = val
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
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
									tempElem.AbortedTransactions = make([]FetchResponseAbortedTransaction, len(decoded))
									for i, item := range decoded {
										tempElem.AbortedTransactions[i] = item.(FetchResponseAbortedTransaction)
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PreferredReadReplica = val
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
							// HighWatermark
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.HighWatermark); err != nil {
									return err
								}
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LastStableOffset); err != nil {
									return err
								}
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LogStartOffset); err != nil {
									return err
								}
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								if tempElem.AbortedTransactions == nil {
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
										length := uint32(len(tempElem.AbortedTransactions) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.AbortedTransactions))); err != nil {
											return err
										}
									}
									for i := range tempElem.AbortedTransactions {
										// ProducerId
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.AbortedTransactions[i].ProducerId); err != nil {
												return err
											}
										}
										// FirstOffset
										if version >= 4 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.AbortedTransactions[i].FirstOffset); err != nil {
												return err
											}
										}
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PreferredReadReplica); err != nil {
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
						tempElem.Partitions = make([]FetchResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(FetchResponsePartitionData)
						}
					}
				}
				// Topic
				if version >= 0 && version <= 12 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Topic); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Topic); err != nil {
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
						// HighWatermark
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].HighWatermark); err != nil {
								return err
							}
						}
						// LastStableOffset
						if version >= 4 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].LastStableOffset); err != nil {
								return err
							}
						}
						// LogStartOffset
						if version >= 5 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].LogStartOffset); err != nil {
								return err
							}
						}
						// DivergingEpoch
						if version >= 12 && version <= 999 {
						}
						// CurrentLeader
						if version >= 12 && version <= 999 {
						}
						// SnapshotId
						if version >= 12 && version <= 999 {
						}
						// AbortedTransactions
						if version >= 4 && version <= 999 {
							if tempElem.Partitions[i].AbortedTransactions == nil {
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
									length := uint32(len(tempElem.Partitions[i].AbortedTransactions) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].AbortedTransactions))); err != nil {
										return err
									}
								}
								for i := range tempElem.Partitions[i].AbortedTransactions {
									// ProducerId
									if version >= 4 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AbortedTransactions[i].ProducerId); err != nil {
											return err
										}
									}
									// FirstOffset
									if version >= 4 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AbortedTransactions[i].FirstOffset); err != nil {
											return err
										}
									}
								}
							}
						}
						// PreferredReadReplica
						if version >= 11 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PreferredReadReplica); err != nil {
								return err
							}
						}
						// Records
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Partitions[i].Records); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableBytes(elemW, tempElem.Partitions[i].Records); err != nil {
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
			m.Responses = make([]FetchResponseFetchableTopicResponse, len(decoded))
			for i, item := range decoded {
				m.Responses[i] = item.(FetchResponseFetchableTopicResponse)
			}
		}
	}
	// NodeEndpoints
	if version >= 16 && version <= 999 {
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// FetchResponseFetchableTopicResponse represents The response topics..
type FetchResponseFetchableTopicResponse struct {
	// The topic name.
	Topic string `json:"topic" versions:"0-12"`
	// The unique topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"13-999"`
	// The topic partitions.
	Partitions []FetchResponsePartitionData `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for FetchResponseFetchableTopicResponse.
func (m *FetchResponseFetchableTopicResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for FetchResponseFetchableTopicResponse.
func (m *FetchResponseFetchableTopicResponse) readTaggedFields(r io.Reader, version int16) error {
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

// FetchResponsePartitionData represents The topic partitions..
type FetchResponsePartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The error code, or 0 if there was no fetch error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The current high water mark.
	HighWatermark int64 `json:"highwatermark" versions:"0-999"`
	// The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
	LastStableOffset int64 `json:"laststableoffset" versions:"4-999"`
	// The current log start offset.
	LogStartOffset int64 `json:"logstartoffset" versions:"5-999"`
	// In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
	DivergingEpoch FetchResponseEpochEndOffset `json:"divergingepoch" versions:"12-999" tag:"0"`
	// The current leader of the partition.
	CurrentLeader FetchResponseLeaderIdAndEpoch `json:"currentleader" versions:"12-999" tag:"1"`
	// In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
	SnapshotId FetchResponseSnapshotId `json:"snapshotid" versions:"12-999" tag:"2"`
	// The aborted transactions.
	AbortedTransactions []FetchResponseAbortedTransaction `json:"abortedtransactions" versions:"4-999"`
	// The preferred read replica for the consumer to use on its next fetch request.
	PreferredReadReplica int32 `json:"preferredreadreplica" versions:"11-999"`
	// The record data.
	Records *[]byte `json:"records" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for FetchResponsePartitionData.
func (m *FetchResponsePartitionData) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	// DivergingEpoch (tag 0)
	if version >= 12 {
		if true {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(0)); err != nil {
				return err
			}
			// Epoch
			if version >= 12 && version <= 999 {
				if err := protocol.WriteInt32(w, m.DivergingEpoch.Epoch); err != nil {
					return err
				}
			}
			// EndOffset
			if version >= 12 && version <= 999 {
				if err := protocol.WriteInt64(w, m.DivergingEpoch.EndOffset); err != nil {
					return err
				}
			}
			taggedFieldsCount++
		}
	}

	// CurrentLeader (tag 1)
	if version >= 12 {
		if true {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(1)); err != nil {
				return err
			}
			// LeaderId
			if version >= 12 && version <= 999 {
				if err := protocol.WriteInt32(w, m.CurrentLeader.LeaderId); err != nil {
					return err
				}
			}
			// LeaderEpoch
			if version >= 12 && version <= 999 {
				if err := protocol.WriteInt32(w, m.CurrentLeader.LeaderEpoch); err != nil {
					return err
				}
			}
			taggedFieldsCount++
		}
	}

	// SnapshotId (tag 2)
	if version >= 12 {
		if true {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(2)); err != nil {
				return err
			}
			// EndOffset
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(w, m.SnapshotId.EndOffset); err != nil {
					return err
				}
			}
			// Epoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.SnapshotId.Epoch); err != nil {
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

// readTaggedFields reads tagged fields for FetchResponsePartitionData.
func (m *FetchResponsePartitionData) readTaggedFields(r io.Reader, version int16) error {
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
		case 0: // DivergingEpoch
			if version >= 12 {
				// Epoch
				if version >= 12 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.DivergingEpoch.Epoch = val
				}
				// EndOffset
				if version >= 12 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.DivergingEpoch.EndOffset = val
				}
			}
		case 1: // CurrentLeader
			if version >= 12 {
				// LeaderId
				if version >= 12 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.CurrentLeader.LeaderId = val
				}
				// LeaderEpoch
				if version >= 12 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.CurrentLeader.LeaderEpoch = val
				}
			}
		case 2: // SnapshotId
			if version >= 12 {
				// EndOffset
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.SnapshotId.EndOffset = val
				}
				// Epoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.SnapshotId.Epoch = val
				}
			}
		default:
			// Unknown tag, skip it
		}
	}

	return nil
}

// FetchResponseEpochEndOffset represents In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge..
type FetchResponseEpochEndOffset struct {
	// The largest epoch.
	Epoch int32 `json:"epoch" versions:"12-999"`
	// The end offset of the epoch.
	EndOffset int64 `json:"endoffset" versions:"12-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for FetchResponseEpochEndOffset.
func (m *FetchResponseEpochEndOffset) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for FetchResponseEpochEndOffset.
func (m *FetchResponseEpochEndOffset) readTaggedFields(r io.Reader, version int16) error {
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

// FetchResponseLeaderIdAndEpoch represents The current leader of the partition..
type FetchResponseLeaderIdAndEpoch struct {
	// The ID of the current leader or -1 if the leader is unknown.
	LeaderId int32 `json:"leaderid" versions:"12-999"`
	// The latest known leader epoch.
	LeaderEpoch int32 `json:"leaderepoch" versions:"12-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for FetchResponseLeaderIdAndEpoch.
func (m *FetchResponseLeaderIdAndEpoch) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for FetchResponseLeaderIdAndEpoch.
func (m *FetchResponseLeaderIdAndEpoch) readTaggedFields(r io.Reader, version int16) error {
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

// FetchResponseSnapshotId represents In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request..
type FetchResponseSnapshotId struct {
	// The end offset of the epoch.
	EndOffset int64 `json:"endoffset" versions:"0-999"`
	// The largest epoch.
	Epoch int32 `json:"epoch" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for FetchResponseSnapshotId.
func (m *FetchResponseSnapshotId) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for FetchResponseSnapshotId.
func (m *FetchResponseSnapshotId) readTaggedFields(r io.Reader, version int16) error {
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

// FetchResponseAbortedTransaction represents The aborted transactions..
type FetchResponseAbortedTransaction struct {
	// The producer id associated with the aborted transaction.
	ProducerId int64 `json:"producerid" versions:"4-999"`
	// The first offset in the aborted transaction.
	FirstOffset int64 `json:"firstoffset" versions:"4-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for FetchResponseAbortedTransaction.
func (m *FetchResponseAbortedTransaction) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for FetchResponseAbortedTransaction.
func (m *FetchResponseAbortedTransaction) readTaggedFields(r io.Reader, version int16) error {
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

// FetchResponseNodeEndpoint represents Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH..
type FetchResponseNodeEndpoint struct {
	// The ID of the associated node.
	NodeId int32 `json:"nodeid" versions:"16-999"`
	// The node's hostname.
	Host string `json:"host" versions:"16-999"`
	// The node's port.
	Port int32 `json:"port" versions:"16-999"`
	// The rack of the node, or null if it has not been assigned to a rack.
	Rack *string `json:"rack" versions:"16-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for FetchResponseNodeEndpoint.
func (m *FetchResponseNodeEndpoint) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for FetchResponseNodeEndpoint.
func (m *FetchResponseNodeEndpoint) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for FetchResponse.
func (m *FetchResponse) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	isFlexible := version >= 12

	// NodeEndpoints (tag 0)
	if version >= 16 {
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
				if version >= 16 && version <= 999 {
					if err := protocol.WriteInt32(w, m.NodeEndpoints[i].NodeId); err != nil {
						return err
					}
				}
				// Host
				if version >= 16 && version <= 999 {
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
				if version >= 16 && version <= 999 {
					if err := protocol.WriteInt32(w, m.NodeEndpoints[i].Port); err != nil {
						return err
					}
				}
				// Rack
				if version >= 16 && version <= 999 {
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

// readTaggedFields reads tagged fields for FetchResponse.
func (m *FetchResponse) readTaggedFields(r io.Reader, version int16) error {
	isFlexible := version >= 12

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
			if version >= 16 {
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
					m.NodeEndpoints = make([]FetchResponseNodeEndpoint, length-1)
					for i := uint32(0); i < length-1; i++ {
						// NodeId
						if version >= 16 && version <= 999 {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.NodeEndpoints[i].NodeId = val
						}
						// Host
						if version >= 16 && version <= 999 {
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
						if version >= 16 && version <= 999 {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.NodeEndpoints[i].Port = val
						}
						// Rack
						if version >= 16 && version <= 999 {
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
