package describesharegroupoffsets

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeShareGroupOffsetsResponseApiKey        = 90
	DescribeShareGroupOffsetsResponseHeaderVersion = 1
)

// DescribeShareGroupOffsetsResponse represents a response message.
type DescribeShareGroupOffsetsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The results for each group.
	Groups []DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup `json:"groups" versions:"0-999"`
}

// Encode encodes a DescribeShareGroupOffsetsResponse to a byte slice for the given version.
func (m *DescribeShareGroupOffsetsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeShareGroupOffsetsResponse from a byte slice for the given version.
func (m *DescribeShareGroupOffsetsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeShareGroupOffsetsResponse to an io.Writer for the given version.
func (m *DescribeShareGroupOffsetsResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
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
	// Groups
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// GroupId
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.GroupId); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.GroupId); err != nil {
						return nil, err
					}
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
					// TopicName
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Topics[i].TopicName); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Topics[i].TopicName); err != nil {
								return nil, err
							}
						}
					}
					// TopicId
					if version >= 0 && version <= 999 {
						if err := protocol.WriteUUID(elemW, structItem.Topics[i].TopicId); err != nil {
							return nil, err
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
							// StartOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Topics[i].Partitions[i].StartOffset); err != nil {
									return nil, err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Topics[i].Partitions[i].LeaderEpoch); err != nil {
									return nil, err
								}
							}
							// Lag
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Topics[i].Partitions[i].Lag); err != nil {
									return nil, err
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, structItem.Topics[i].Partitions[i].ErrorCode); err != nil {
									return nil, err
								}
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, structItem.Topics[i].Partitions[i].ErrorMessage); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, structItem.Topics[i].Partitions[i].ErrorMessage); err != nil {
										return nil, err
									}
								}
							}
						}
					}
				}
			}
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ErrorCode); err != nil {
					return nil, err
				}
			}
			// ErrorMessage
			if version >= 0 && version <= 999 {
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
			// Write tagged fields if flexible
			if isFlexible {
				if err := structItem.writeTaggedFields(elemW, version); err != nil {
					return nil, err
				}
			}
			return elemBuf.Bytes(), nil
		}
		items := make([]interface{}, len(m.Groups))
		for i := range m.Groups {
			items[i] = m.Groups[i]
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

// Read reads a DescribeShareGroupOffsetsResponse from an io.Reader for the given version.
func (m *DescribeShareGroupOffsetsResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
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
	// Groups
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup
			elemR := bytes.NewReader(data)
			// GroupId
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupId = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupId = val
				}
			}
			// Topics
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
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
				var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					}
				}
				// Topics
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic
						elemR := bytes.NewReader(data)
						// TopicName
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.TopicName = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.TopicName = val
							}
						}
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
							var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								}
							}
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
									var elem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// StartOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.StartOffset = val
									}
									// LeaderEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LeaderEpoch = val
									}
									// Lag
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Lag = val
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
										var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.StartOffset = val
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LeaderEpoch = val
										}
										// Lag
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Lag = val
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
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
												return err
											}
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
												return err
											}
										}
										// Lag
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Lag); err != nil {
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
									tempElem.Partitions = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition)
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
										var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.StartOffset = val
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LeaderEpoch = val
										}
										// Lag
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Lag = val
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
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
												return err
											}
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
												return err
											}
										}
										// Lag
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Lag); err != nil {
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
									tempElem.Partitions = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition)
									}
								}
							}
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.TopicName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.TopicName); err != nil {
										return err
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
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// StartOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].StartOffset); err != nil {
											return err
										}
									}
									// LeaderEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
											return err
										}
									}
									// Lag
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Lag); err != nil {
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
						tempElem.Topics = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic)
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
							var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								}
							}
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
									var elem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// StartOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.StartOffset = val
									}
									// LeaderEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LeaderEpoch = val
									}
									// Lag
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Lag = val
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
										var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.StartOffset = val
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LeaderEpoch = val
										}
										// Lag
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Lag = val
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
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
												return err
											}
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
												return err
											}
										}
										// Lag
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Lag); err != nil {
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
									tempElem.Partitions = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition)
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
										var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.StartOffset = val
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LeaderEpoch = val
										}
										// Lag
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Lag = val
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
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
												return err
											}
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
												return err
											}
										}
										// Lag
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Lag); err != nil {
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
									tempElem.Partitions = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition)
									}
								}
							}
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.TopicName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.TopicName); err != nil {
										return err
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
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// StartOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].StartOffset); err != nil {
											return err
										}
									}
									// LeaderEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
											return err
										}
									}
									// Lag
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Lag); err != nil {
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
						tempElem.Topics = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic)
						}
					}
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
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupId); err != nil {
							return err
						}
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
						// TopicName
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Topics[i].TopicName); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Topics[i].TopicName); err != nil {
									return err
								}
							}
						}
						// TopicId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteUUID(elemW, tempElem.Topics[i].TopicId); err != nil {
								return err
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
								// StartOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Topics[i].Partitions[i].StartOffset); err != nil {
										return err
									}
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Topics[i].Partitions[i].LeaderEpoch); err != nil {
										return err
									}
								}
								// Lag
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Topics[i].Partitions[i].Lag); err != nil {
										return err
									}
								}
								// ErrorCode
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Topics[i].Partitions[i].ErrorCode); err != nil {
										return err
									}
								}
								// ErrorMessage
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactNullableString(elemW, tempElem.Topics[i].Partitions[i].ErrorMessage); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteNullableString(elemW, tempElem.Topics[i].Partitions[i].ErrorMessage); err != nil {
											return err
										}
									}
								}
							}
						}
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
			m.Groups = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup)
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
				var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					}
				}
				// Topics
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic
						elemR := bytes.NewReader(data)
						// TopicName
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.TopicName = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.TopicName = val
							}
						}
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
							var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								}
							}
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
									var elem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// StartOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.StartOffset = val
									}
									// LeaderEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LeaderEpoch = val
									}
									// Lag
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Lag = val
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
										var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.StartOffset = val
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LeaderEpoch = val
										}
										// Lag
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Lag = val
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
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
												return err
											}
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
												return err
											}
										}
										// Lag
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Lag); err != nil {
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
									tempElem.Partitions = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition)
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
										var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.StartOffset = val
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LeaderEpoch = val
										}
										// Lag
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Lag = val
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
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
												return err
											}
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
												return err
											}
										}
										// Lag
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Lag); err != nil {
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
									tempElem.Partitions = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition)
									}
								}
							}
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.TopicName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.TopicName); err != nil {
										return err
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
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// StartOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].StartOffset); err != nil {
											return err
										}
									}
									// LeaderEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
											return err
										}
									}
									// Lag
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Lag); err != nil {
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
						tempElem.Topics = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic)
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
							var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.TopicName = val
								}
							}
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
									var elem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// StartOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.StartOffset = val
									}
									// LeaderEpoch
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LeaderEpoch = val
									}
									// Lag
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Lag = val
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
										var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.StartOffset = val
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LeaderEpoch = val
										}
										// Lag
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Lag = val
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
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
												return err
											}
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
												return err
											}
										}
										// Lag
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Lag); err != nil {
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
									tempElem.Partitions = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition)
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
										var tempElem DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.StartOffset = val
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.LeaderEpoch = val
										}
										// Lag
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Lag = val
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
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// StartOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.StartOffset); err != nil {
												return err
											}
										}
										// LeaderEpoch
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
												return err
											}
										}
										// Lag
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Lag); err != nil {
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
									tempElem.Partitions = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition)
									}
								}
							}
							// TopicName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.TopicName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.TopicName); err != nil {
										return err
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
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// StartOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].StartOffset); err != nil {
											return err
										}
									}
									// LeaderEpoch
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
											return err
										}
									}
									// Lag
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Lag); err != nil {
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
						tempElem.Topics = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic)
						}
					}
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
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupId); err != nil {
							return err
						}
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
						// TopicName
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Topics[i].TopicName); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Topics[i].TopicName); err != nil {
									return err
								}
							}
						}
						// TopicId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteUUID(elemW, tempElem.Topics[i].TopicId); err != nil {
								return err
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
								// StartOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Topics[i].Partitions[i].StartOffset); err != nil {
										return err
									}
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Topics[i].Partitions[i].LeaderEpoch); err != nil {
										return err
									}
								}
								// Lag
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Topics[i].Partitions[i].Lag); err != nil {
										return err
									}
								}
								// ErrorCode
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Topics[i].Partitions[i].ErrorCode); err != nil {
										return err
									}
								}
								// ErrorMessage
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactNullableString(elemW, tempElem.Topics[i].Partitions[i].ErrorMessage); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteNullableString(elemW, tempElem.Topics[i].Partitions[i].ErrorMessage); err != nil {
											return err
										}
									}
								}
							}
						}
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
			m.Groups = make([]DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup)
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

// DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup represents The results for each group..
type DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup struct {
	// The group identifier.
	GroupId string `json:"groupid" versions:"0-999"`
	// The results for each topic.
	Topics []DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic `json:"topics" versions:"0-999"`
	// The group-level error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The group-level error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup.
func (m *DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup.
func (m *DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic represents The results for each topic..
type DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic struct {
	// The topic name.
	TopicName string `json:"topicname" versions:"0-999"`
	// The unique topic ID.
	TopicId    uuid.UUID                                                                     `json:"topicid" versions:"0-999"`
	Partitions []DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic.
func (m *DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic.
func (m *DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition represents .
type DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The share-partition start offset.
	StartOffset int64 `json:"startoffset" versions:"0-999"`
	// The leader epoch of the partition.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// The share-partition lag.
	Lag int64 `json:"lag" versions:"1-999"`
	// The partition-level error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The partition-level error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition.
func (m *DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition.
func (m *DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for DescribeShareGroupOffsetsResponse.
func (m *DescribeShareGroupOffsetsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeShareGroupOffsetsResponse.
func (m *DescribeShareGroupOffsetsResponse) readTaggedFields(r io.Reader, version int16) error {
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
