package describequorum

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeQuorumResponseApiKey        = 55
	DescribeQuorumResponseHeaderVersion = 1
)

// DescribeQuorumResponse represents a response message.
type DescribeQuorumResponse struct {
	// The top level error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"2-999"`
	// The response from the describe quorum API.
	Topics []DescribeQuorumResponseTopicData `json:"topics" versions:"0-999"`
	// The nodes in the quorum.
	Nodes []DescribeQuorumResponseNode `json:"nodes" versions:"2-999"`
}

// Encode encodes a DescribeQuorumResponse to a byte slice for the given version.
func (m *DescribeQuorumResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeQuorumResponse from a byte slice for the given version.
func (m *DescribeQuorumResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeQuorumResponse to an io.Writer for the given version.
func (m *DescribeQuorumResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ErrorCode
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// ErrorMessage
	if version >= 2 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ErrorMessage); err != nil {
				return err
			}
		}
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DescribeQuorumResponseTopicData)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// TopicName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.TopicName); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.TopicName); err != nil {
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
					if version >= 2 && version <= 999 {
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
					// LeaderId
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].LeaderId); err != nil {
							return nil, err
						}
					}
					// LeaderEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].LeaderEpoch); err != nil {
							return nil, err
						}
					}
					// HighWatermark
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(elemW, structItem.Partitions[i].HighWatermark); err != nil {
							return nil, err
						}
					}
					// CurrentVoters
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Partitions[i].CurrentVoters) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions[i].CurrentVoters))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Partitions[i].CurrentVoters {
							// ReplicaId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Partitions[i].CurrentVoters[i].ReplicaId); err != nil {
									return nil, err
								}
							}
							// ReplicaDirectoryId
							if version >= 2 && version <= 999 {
								if err := protocol.WriteUUID(elemW, structItem.Partitions[i].CurrentVoters[i].ReplicaDirectoryId); err != nil {
									return nil, err
								}
							}
							// LogEndOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].CurrentVoters[i].LogEndOffset); err != nil {
									return nil, err
								}
							}
							// LastFetchTimestamp
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].CurrentVoters[i].LastFetchTimestamp); err != nil {
									return nil, err
								}
							}
							// LastCaughtUpTimestamp
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp); err != nil {
									return nil, err
								}
							}
						}
					}
					// Observers
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Partitions[i].Observers) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions[i].Observers))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Partitions[i].Observers {
							// ReplicaId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Partitions[i].Observers[i].ReplicaId); err != nil {
									return nil, err
								}
							}
							// ReplicaDirectoryId
							if version >= 2 && version <= 999 {
								if err := protocol.WriteUUID(elemW, structItem.Partitions[i].Observers[i].ReplicaDirectoryId); err != nil {
									return nil, err
								}
							}
							// LogEndOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].Observers[i].LogEndOffset); err != nil {
									return nil, err
								}
							}
							// LastFetchTimestamp
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].Observers[i].LastFetchTimestamp); err != nil {
									return nil, err
								}
							}
							// LastCaughtUpTimestamp
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].Observers[i].LastCaughtUpTimestamp); err != nil {
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
	// Nodes
	if version >= 2 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DescribeQuorumResponseNode)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// NodeId
			if version >= 2 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.NodeId); err != nil {
					return nil, err
				}
			}
			// Listeners
			if version >= 2 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Listeners) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Listeners))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Listeners {
					// Name
					if version >= 2 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Listeners[i].Name); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Listeners[i].Name); err != nil {
								return nil, err
							}
						}
					}
					// Host
					if version >= 2 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Listeners[i].Host); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Listeners[i].Host); err != nil {
								return nil, err
							}
						}
					}
					// Port
					if version >= 2 && version <= 999 {
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
		items := make([]interface{}, len(m.Nodes))
		for i := range m.Nodes {
			items[i] = m.Nodes[i]
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

// Read reads a DescribeQuorumResponse from an io.Reader for the given version.
func (m *DescribeQuorumResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ErrorCode
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// ErrorMessage
	if version >= 2 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ErrorMessage = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ErrorMessage = val
		}
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DescribeQuorumResponseTopicData
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
				var tempElem DescribeQuorumResponseTopicData
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
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeQuorumResponsePartitionData
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
						if version >= 2 && version <= 999 {
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
						// LeaderId
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderId = val
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderEpoch = val
						}
						// HighWatermark
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.HighWatermark = val
						}
						// CurrentVoters
						if version >= 0 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// Observers
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
							var tempElem DescribeQuorumResponsePartitionData
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
							if version >= 2 && version <= 999 {
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
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.HighWatermark = val
							}
							// CurrentVoters
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReplicaState
									elemR := bytes.NewReader(data)
									// ReplicaId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaId = val
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaDirectoryId = val
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LogEndOffset = val
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastFetchTimestamp = val
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastCaughtUpTimestamp = val
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.CurrentVoters = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.CurrentVoters[i] = item.(ReplicaState)
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.CurrentVoters = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.CurrentVoters[i] = item.(ReplicaState)
									}
								}
							}
							// Observers
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReplicaState
									elemR := bytes.NewReader(data)
									// ReplicaId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaId = val
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaDirectoryId = val
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LogEndOffset = val
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastFetchTimestamp = val
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastCaughtUpTimestamp = val
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.Observers = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.Observers[i] = item.(ReplicaState)
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.Observers = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.Observers[i] = item.(ReplicaState)
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
							if version >= 2 && version <= 999 {
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
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.HighWatermark); err != nil {
									return err
								}
							}
							// CurrentVoters
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.CurrentVoters) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.CurrentVoters))); err != nil {
										return err
									}
								}
								for i := range tempElem.CurrentVoters {
									// ReplicaId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.CurrentVoters[i].ReplicaId); err != nil {
											return err
										}
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.CurrentVoters[i].ReplicaDirectoryId); err != nil {
											return err
										}
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LogEndOffset); err != nil {
											return err
										}
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LastFetchTimestamp); err != nil {
											return err
										}
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LastCaughtUpTimestamp); err != nil {
											return err
										}
									}
								}
							}
							// Observers
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.Observers) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.Observers))); err != nil {
										return err
									}
								}
								for i := range tempElem.Observers {
									// ReplicaId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Observers[i].ReplicaId); err != nil {
											return err
										}
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.Observers[i].ReplicaDirectoryId); err != nil {
											return err
										}
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LogEndOffset); err != nil {
											return err
										}
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LastFetchTimestamp); err != nil {
											return err
										}
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LastCaughtUpTimestamp); err != nil {
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
						tempElem.Partitions = make([]DescribeQuorumResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeQuorumResponsePartitionData)
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
							var tempElem DescribeQuorumResponsePartitionData
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
							if version >= 2 && version <= 999 {
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
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.HighWatermark = val
							}
							// CurrentVoters
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReplicaState
									elemR := bytes.NewReader(data)
									// ReplicaId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaId = val
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaDirectoryId = val
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LogEndOffset = val
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastFetchTimestamp = val
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastCaughtUpTimestamp = val
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.CurrentVoters = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.CurrentVoters[i] = item.(ReplicaState)
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.CurrentVoters = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.CurrentVoters[i] = item.(ReplicaState)
									}
								}
							}
							// Observers
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReplicaState
									elemR := bytes.NewReader(data)
									// ReplicaId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaId = val
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaDirectoryId = val
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LogEndOffset = val
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastFetchTimestamp = val
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastCaughtUpTimestamp = val
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.Observers = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.Observers[i] = item.(ReplicaState)
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.Observers = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.Observers[i] = item.(ReplicaState)
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
							if version >= 2 && version <= 999 {
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
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.HighWatermark); err != nil {
									return err
								}
							}
							// CurrentVoters
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.CurrentVoters) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.CurrentVoters))); err != nil {
										return err
									}
								}
								for i := range tempElem.CurrentVoters {
									// ReplicaId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.CurrentVoters[i].ReplicaId); err != nil {
											return err
										}
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.CurrentVoters[i].ReplicaDirectoryId); err != nil {
											return err
										}
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LogEndOffset); err != nil {
											return err
										}
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LastFetchTimestamp); err != nil {
											return err
										}
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LastCaughtUpTimestamp); err != nil {
											return err
										}
									}
								}
							}
							// Observers
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.Observers) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.Observers))); err != nil {
										return err
									}
								}
								for i := range tempElem.Observers {
									// ReplicaId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Observers[i].ReplicaId); err != nil {
											return err
										}
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.Observers[i].ReplicaDirectoryId); err != nil {
											return err
										}
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LogEndOffset); err != nil {
											return err
										}
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LastFetchTimestamp); err != nil {
											return err
										}
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LastCaughtUpTimestamp); err != nil {
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
						tempElem.Partitions = make([]DescribeQuorumResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeQuorumResponsePartitionData)
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
						if version >= 2 && version <= 999 {
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
						// LeaderId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderId); err != nil {
								return err
							}
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
								return err
							}
						}
						// HighWatermark
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].HighWatermark); err != nil {
								return err
							}
						}
						// CurrentVoters
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].CurrentVoters) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].CurrentVoters))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].CurrentVoters {
								// ReplicaId
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CurrentVoters[i].ReplicaId); err != nil {
										return err
									}
								}
								// ReplicaDirectoryId
								if version >= 2 && version <= 999 {
									if err := protocol.WriteUUID(elemW, tempElem.Partitions[i].CurrentVoters[i].ReplicaDirectoryId); err != nil {
										return err
									}
								}
								// LogEndOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CurrentVoters[i].LogEndOffset); err != nil {
										return err
									}
								}
								// LastFetchTimestamp
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CurrentVoters[i].LastFetchTimestamp); err != nil {
										return err
									}
								}
								// LastCaughtUpTimestamp
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp); err != nil {
										return err
									}
								}
							}
						}
						// Observers
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].Observers) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].Observers))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].Observers {
								// ReplicaId
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].Observers[i].ReplicaId); err != nil {
										return err
									}
								}
								// ReplicaDirectoryId
								if version >= 2 && version <= 999 {
									if err := protocol.WriteUUID(elemW, tempElem.Partitions[i].Observers[i].ReplicaDirectoryId); err != nil {
										return err
									}
								}
								// LogEndOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Observers[i].LogEndOffset); err != nil {
										return err
									}
								}
								// LastFetchTimestamp
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Observers[i].LastFetchTimestamp); err != nil {
										return err
									}
								}
								// LastCaughtUpTimestamp
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Observers[i].LastCaughtUpTimestamp); err != nil {
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
			m.Topics = make([]DescribeQuorumResponseTopicData, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(DescribeQuorumResponseTopicData)
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
				var tempElem DescribeQuorumResponseTopicData
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
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeQuorumResponsePartitionData
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
						if version >= 2 && version <= 999 {
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
						// LeaderId
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderId = val
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderEpoch = val
						}
						// HighWatermark
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.HighWatermark = val
						}
						// CurrentVoters
						if version >= 0 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// Observers
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
							var tempElem DescribeQuorumResponsePartitionData
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
							if version >= 2 && version <= 999 {
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
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.HighWatermark = val
							}
							// CurrentVoters
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReplicaState
									elemR := bytes.NewReader(data)
									// ReplicaId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaId = val
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaDirectoryId = val
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LogEndOffset = val
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastFetchTimestamp = val
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastCaughtUpTimestamp = val
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.CurrentVoters = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.CurrentVoters[i] = item.(ReplicaState)
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.CurrentVoters = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.CurrentVoters[i] = item.(ReplicaState)
									}
								}
							}
							// Observers
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReplicaState
									elemR := bytes.NewReader(data)
									// ReplicaId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaId = val
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaDirectoryId = val
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LogEndOffset = val
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastFetchTimestamp = val
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastCaughtUpTimestamp = val
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.Observers = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.Observers[i] = item.(ReplicaState)
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.Observers = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.Observers[i] = item.(ReplicaState)
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
							if version >= 2 && version <= 999 {
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
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.HighWatermark); err != nil {
									return err
								}
							}
							// CurrentVoters
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.CurrentVoters) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.CurrentVoters))); err != nil {
										return err
									}
								}
								for i := range tempElem.CurrentVoters {
									// ReplicaId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.CurrentVoters[i].ReplicaId); err != nil {
											return err
										}
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.CurrentVoters[i].ReplicaDirectoryId); err != nil {
											return err
										}
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LogEndOffset); err != nil {
											return err
										}
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LastFetchTimestamp); err != nil {
											return err
										}
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LastCaughtUpTimestamp); err != nil {
											return err
										}
									}
								}
							}
							// Observers
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.Observers) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.Observers))); err != nil {
										return err
									}
								}
								for i := range tempElem.Observers {
									// ReplicaId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Observers[i].ReplicaId); err != nil {
											return err
										}
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.Observers[i].ReplicaDirectoryId); err != nil {
											return err
										}
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LogEndOffset); err != nil {
											return err
										}
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LastFetchTimestamp); err != nil {
											return err
										}
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LastCaughtUpTimestamp); err != nil {
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
						tempElem.Partitions = make([]DescribeQuorumResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeQuorumResponsePartitionData)
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
							var tempElem DescribeQuorumResponsePartitionData
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
							if version >= 2 && version <= 999 {
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
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.HighWatermark = val
							}
							// CurrentVoters
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReplicaState
									elemR := bytes.NewReader(data)
									// ReplicaId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaId = val
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaDirectoryId = val
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LogEndOffset = val
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastFetchTimestamp = val
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastCaughtUpTimestamp = val
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.CurrentVoters = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.CurrentVoters[i] = item.(ReplicaState)
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.CurrentVoters = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.CurrentVoters[i] = item.(ReplicaState)
									}
								}
							}
							// Observers
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ReplicaState
									elemR := bytes.NewReader(data)
									// ReplicaId
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaId = val
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ReplicaDirectoryId = val
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LogEndOffset = val
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastFetchTimestamp = val
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastCaughtUpTimestamp = val
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.Observers = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.Observers[i] = item.(ReplicaState)
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
										var tempElem ReplicaState
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastCaughtUpTimestamp = val
										}
										// ReplicaId
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
												return err
											}
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
												return err
											}
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LogEndOffset); err != nil {
												return err
											}
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastFetchTimestamp); err != nil {
												return err
											}
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastCaughtUpTimestamp); err != nil {
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
									tempElem.Observers = make([]ReplicaState, len(decoded))
									for i, item := range decoded {
										tempElem.Observers[i] = item.(ReplicaState)
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
							if version >= 2 && version <= 999 {
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
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.HighWatermark); err != nil {
									return err
								}
							}
							// CurrentVoters
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.CurrentVoters) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.CurrentVoters))); err != nil {
										return err
									}
								}
								for i := range tempElem.CurrentVoters {
									// ReplicaId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.CurrentVoters[i].ReplicaId); err != nil {
											return err
										}
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.CurrentVoters[i].ReplicaDirectoryId); err != nil {
											return err
										}
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LogEndOffset); err != nil {
											return err
										}
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LastFetchTimestamp); err != nil {
											return err
										}
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.CurrentVoters[i].LastCaughtUpTimestamp); err != nil {
											return err
										}
									}
								}
							}
							// Observers
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.Observers) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.Observers))); err != nil {
										return err
									}
								}
								for i := range tempElem.Observers {
									// ReplicaId
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Observers[i].ReplicaId); err != nil {
											return err
										}
									}
									// ReplicaDirectoryId
									if version >= 2 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.Observers[i].ReplicaDirectoryId); err != nil {
											return err
										}
									}
									// LogEndOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LogEndOffset); err != nil {
											return err
										}
									}
									// LastFetchTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LastFetchTimestamp); err != nil {
											return err
										}
									}
									// LastCaughtUpTimestamp
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Observers[i].LastCaughtUpTimestamp); err != nil {
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
						tempElem.Partitions = make([]DescribeQuorumResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeQuorumResponsePartitionData)
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
						if version >= 2 && version <= 999 {
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
						// LeaderId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderId); err != nil {
								return err
							}
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
								return err
							}
						}
						// HighWatermark
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].HighWatermark); err != nil {
								return err
							}
						}
						// CurrentVoters
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].CurrentVoters) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].CurrentVoters))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].CurrentVoters {
								// ReplicaId
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CurrentVoters[i].ReplicaId); err != nil {
										return err
									}
								}
								// ReplicaDirectoryId
								if version >= 2 && version <= 999 {
									if err := protocol.WriteUUID(elemW, tempElem.Partitions[i].CurrentVoters[i].ReplicaDirectoryId); err != nil {
										return err
									}
								}
								// LogEndOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CurrentVoters[i].LogEndOffset); err != nil {
										return err
									}
								}
								// LastFetchTimestamp
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CurrentVoters[i].LastFetchTimestamp); err != nil {
										return err
									}
								}
								// LastCaughtUpTimestamp
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp); err != nil {
										return err
									}
								}
							}
						}
						// Observers
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].Observers) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].Observers))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].Observers {
								// ReplicaId
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].Observers[i].ReplicaId); err != nil {
										return err
									}
								}
								// ReplicaDirectoryId
								if version >= 2 && version <= 999 {
									if err := protocol.WriteUUID(elemW, tempElem.Partitions[i].Observers[i].ReplicaDirectoryId); err != nil {
										return err
									}
								}
								// LogEndOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Observers[i].LogEndOffset); err != nil {
										return err
									}
								}
								// LastFetchTimestamp
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Observers[i].LastFetchTimestamp); err != nil {
										return err
									}
								}
								// LastCaughtUpTimestamp
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Observers[i].LastCaughtUpTimestamp); err != nil {
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
			m.Topics = make([]DescribeQuorumResponseTopicData, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(DescribeQuorumResponseTopicData)
			}
		}
	}
	// Nodes
	if version >= 2 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DescribeQuorumResponseNode
			elemR := bytes.NewReader(data)
			// NodeId
			if version >= 2 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.NodeId = val
			}
			// Listeners
			if version >= 2 && version <= 999 {
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
				var tempElem DescribeQuorumResponseNode
				// NodeId
				if version >= 2 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.NodeId = val
				}
				// Listeners
				if version >= 2 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeQuorumResponseListener
						elemR := bytes.NewReader(data)
						// Name
						if version >= 2 && version <= 999 {
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
						// Host
						if version >= 2 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Host = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Host = val
							}
						}
						// Port
						if version >= 2 && version <= 999 {
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
							var tempElem DescribeQuorumResponseListener
							// Name
							if version >= 2 && version <= 999 {
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
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								}
							}
							// Port
							if version >= 2 && version <= 999 {
							}
							// Name
							if version >= 2 && version <= 999 {
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
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
										return err
									}
								}
							}
							// Port
							if version >= 2 && version <= 999 {
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
						tempElem.Listeners = make([]DescribeQuorumResponseListener, len(decoded))
						for i, item := range decoded {
							tempElem.Listeners[i] = item.(DescribeQuorumResponseListener)
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
							var tempElem DescribeQuorumResponseListener
							// Name
							if version >= 2 && version <= 999 {
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
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								}
							}
							// Port
							if version >= 2 && version <= 999 {
							}
							// Name
							if version >= 2 && version <= 999 {
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
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
										return err
									}
								}
							}
							// Port
							if version >= 2 && version <= 999 {
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
						tempElem.Listeners = make([]DescribeQuorumResponseListener, len(decoded))
						for i, item := range decoded {
							tempElem.Listeners[i] = item.(DescribeQuorumResponseListener)
						}
					}
				}
				// NodeId
				if version >= 2 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.NodeId); err != nil {
						return err
					}
				}
				// Listeners
				if version >= 2 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Listeners) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Listeners))); err != nil {
							return err
						}
					}
					for i := range tempElem.Listeners {
						// Name
						if version >= 2 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Listeners[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Listeners[i].Name); err != nil {
									return err
								}
							}
						}
						// Host
						if version >= 2 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Listeners[i].Host); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Listeners[i].Host); err != nil {
									return err
								}
							}
						}
						// Port
						if version >= 2 && version <= 999 {
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
			m.Nodes = make([]DescribeQuorumResponseNode, len(decoded))
			for i, item := range decoded {
				m.Nodes[i] = item.(DescribeQuorumResponseNode)
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
				var tempElem DescribeQuorumResponseNode
				// NodeId
				if version >= 2 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.NodeId = val
				}
				// Listeners
				if version >= 2 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeQuorumResponseListener
						elemR := bytes.NewReader(data)
						// Name
						if version >= 2 && version <= 999 {
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
						// Host
						if version >= 2 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Host = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Host = val
							}
						}
						// Port
						if version >= 2 && version <= 999 {
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
							var tempElem DescribeQuorumResponseListener
							// Name
							if version >= 2 && version <= 999 {
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
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								}
							}
							// Port
							if version >= 2 && version <= 999 {
							}
							// Name
							if version >= 2 && version <= 999 {
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
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
										return err
									}
								}
							}
							// Port
							if version >= 2 && version <= 999 {
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
						tempElem.Listeners = make([]DescribeQuorumResponseListener, len(decoded))
						for i, item := range decoded {
							tempElem.Listeners[i] = item.(DescribeQuorumResponseListener)
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
							var tempElem DescribeQuorumResponseListener
							// Name
							if version >= 2 && version <= 999 {
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
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								}
							}
							// Port
							if version >= 2 && version <= 999 {
							}
							// Name
							if version >= 2 && version <= 999 {
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
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
										return err
									}
								}
							}
							// Port
							if version >= 2 && version <= 999 {
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
						tempElem.Listeners = make([]DescribeQuorumResponseListener, len(decoded))
						for i, item := range decoded {
							tempElem.Listeners[i] = item.(DescribeQuorumResponseListener)
						}
					}
				}
				// NodeId
				if version >= 2 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.NodeId); err != nil {
						return err
					}
				}
				// Listeners
				if version >= 2 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Listeners) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Listeners))); err != nil {
							return err
						}
					}
					for i := range tempElem.Listeners {
						// Name
						if version >= 2 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Listeners[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Listeners[i].Name); err != nil {
									return err
								}
							}
						}
						// Host
						if version >= 2 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Listeners[i].Host); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Listeners[i].Host); err != nil {
									return err
								}
							}
						}
						// Port
						if version >= 2 && version <= 999 {
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
			m.Nodes = make([]DescribeQuorumResponseNode, len(decoded))
			for i, item := range decoded {
				m.Nodes[i] = item.(DescribeQuorumResponseNode)
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

// DescribeQuorumResponseTopicData represents The response from the describe quorum API..
type DescribeQuorumResponseTopicData struct {
	// The topic name.
	TopicName string `json:"topicname" versions:"0-999"`
	// The partition data.
	Partitions []DescribeQuorumResponsePartitionData `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeQuorumResponseTopicData.
func (m *DescribeQuorumResponseTopicData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeQuorumResponseTopicData.
func (m *DescribeQuorumResponseTopicData) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeQuorumResponsePartitionData represents The partition data..
type DescribeQuorumResponsePartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The partition error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"2-999"`
	// The ID of the current leader or -1 if the leader is unknown.
	LeaderId int32 `json:"leaderid" versions:"0-999"`
	// The latest known leader epoch.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// The high water mark.
	HighWatermark int64 `json:"highwatermark" versions:"0-999"`
	// The current voters of the partition.
	CurrentVoters []ReplicaState `json:"currentvoters" versions:"0-999"`
	// The observers of the partition.
	Observers []ReplicaState `json:"observers" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeQuorumResponsePartitionData.
func (m *DescribeQuorumResponsePartitionData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeQuorumResponsePartitionData.
func (m *DescribeQuorumResponsePartitionData) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeQuorumResponseNode represents The nodes in the quorum..
type DescribeQuorumResponseNode struct {
	// The ID of the associated node.
	NodeId int32 `json:"nodeid" versions:"2-999"`
	// The listeners of this controller.
	Listeners []DescribeQuorumResponseListener `json:"listeners" versions:"2-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeQuorumResponseNode.
func (m *DescribeQuorumResponseNode) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeQuorumResponseNode.
func (m *DescribeQuorumResponseNode) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeQuorumResponseListener represents The listeners of this controller..
type DescribeQuorumResponseListener struct {
	// The name of the endpoint.
	Name string `json:"name" versions:"2-999"`
	// The hostname.
	Host string `json:"host" versions:"2-999"`
	// The port.
	Port uint16 `json:"port" versions:"2-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeQuorumResponseListener.
func (m *DescribeQuorumResponseListener) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeQuorumResponseListener.
func (m *DescribeQuorumResponseListener) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for DescribeQuorumResponse.
func (m *DescribeQuorumResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeQuorumResponse.
func (m *DescribeQuorumResponse) readTaggedFields(r io.Reader, version int16) error {
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
