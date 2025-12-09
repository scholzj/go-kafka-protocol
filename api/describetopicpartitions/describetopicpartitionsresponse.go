package describetopicpartitions

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeTopicPartitionsResponseApiKey        = 75
	DescribeTopicPartitionsResponseHeaderVersion = 1
)

// DescribeTopicPartitionsResponse represents a response message.
type DescribeTopicPartitionsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// Each topic in the response.
	Topics []DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic `json:"topics" versions:"0-999"`
	// The next topic and partition index to fetch details for.
	NextCursor DescribeTopicPartitionsResponseCursor `json:"nextcursor" versions:"0-999"`
}

// Encode encodes a DescribeTopicPartitionsResponse to a byte slice for the given version.
func (m *DescribeTopicPartitionsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeTopicPartitionsResponse from a byte slice for the given version.
func (m *DescribeTopicPartitionsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeTopicPartitionsResponse to an io.Writer for the given version.
func (m *DescribeTopicPartitionsResponse) Write(w io.Writer, version int16) error {
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
			structItem, ok := item.(DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ErrorCode); err != nil {
					return nil, err
				}
			}
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				}
			}
			// TopicId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteUUID(elemW, structItem.TopicId); err != nil {
					return nil, err
				}
			}
			// IsInternal
			if version >= 0 && version <= 999 {
				if err := protocol.WriteBool(elemW, structItem.IsInternal); err != nil {
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
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(elemW, structItem.Partitions[i].ErrorCode); err != nil {
							return nil, err
						}
					}
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].PartitionIndex); err != nil {
							return nil, err
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
					// ReplicaNodes
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions[i].ReplicaNodes); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Partitions[i].ReplicaNodes); err != nil {
								return nil, err
							}
						}
					}
					// IsrNodes
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions[i].IsrNodes); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Partitions[i].IsrNodes); err != nil {
								return nil, err
							}
						}
					}
					// EligibleLeaderReplicas
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions[i].EligibleLeaderReplicas); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Partitions[i].EligibleLeaderReplicas); err != nil {
								return nil, err
							}
						}
					}
					// LastKnownElr
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions[i].LastKnownElr); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Partitions[i].LastKnownElr); err != nil {
								return nil, err
							}
						}
					}
					// OfflineReplicas
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions[i].OfflineReplicas); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Partitions[i].OfflineReplicas); err != nil {
								return nil, err
							}
						}
					}
				}
			}
			// TopicAuthorizedOperations
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.TopicAuthorizedOperations); err != nil {
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
	// NextCursor
	if version >= 0 && version <= 999 {
		// TopicName
		if version >= 0 && version <= 999 {
			if isFlexible {
				if err := protocol.WriteCompactString(w, m.NextCursor.TopicName); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteString(w, m.NextCursor.TopicName); err != nil {
					return err
				}
			}
		}
		// PartitionIndex
		if version >= 0 && version <= 999 {
			if err := protocol.WriteInt32(w, m.NextCursor.PartitionIndex); err != nil {
				return err
			}
		}
		// Write tagged fields if flexible
		if isFlexible {
			if err := m.NextCursor.writeTaggedFields(w, version); err != nil {
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

// Read reads a DescribeTopicPartitionsResponse from an io.Reader for the given version.
func (m *DescribeTopicPartitionsResponse) Read(r io.Reader, version int16) error {
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
			var elem DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic
			elemR := bytes.NewReader(data)
			// ErrorCode
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ErrorCode = val
			}
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
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
			// IsInternal
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadBool(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.IsInternal = val
			}
			// Partitions
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// TopicAuthorizedOperations
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.TopicAuthorizedOperations = val
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
				var tempElem DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
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
				// IsInternal
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					tempElem.IsInternal = val
				}
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition
						elemR := bytes.NewReader(data)
						// ErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ErrorCode = val
						}
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
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
						// ReplicaNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ReplicaNodes = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ReplicaNodes = val
							}
						}
						// IsrNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.IsrNodes = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.IsrNodes = val
							}
						}
						// EligibleLeaderReplicas
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EligibleLeaderReplicas = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EligibleLeaderReplicas = val
							}
						}
						// LastKnownElr
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.LastKnownElr = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.LastKnownElr = val
							}
						}
						// OfflineReplicas
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.OfflineReplicas = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.OfflineReplicas = val
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
							var tempElem DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition
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
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
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
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								}
							}
							// EligibleLeaderReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.EligibleLeaderReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.EligibleLeaderReplicas = val
								}
							}
							// LastKnownElr
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.LastKnownElr = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.LastKnownElr = val
								}
							}
							// OfflineReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
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
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								}
							}
							// EligibleLeaderReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.EligibleLeaderReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.EligibleLeaderReplicas); err != nil {
										return err
									}
								}
							}
							// LastKnownElr
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.LastKnownElr); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.LastKnownElr); err != nil {
										return err
									}
								}
							}
							// OfflineReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
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
						tempElem.Partitions = make([]DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition)
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
							var tempElem DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition
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
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
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
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								}
							}
							// EligibleLeaderReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.EligibleLeaderReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.EligibleLeaderReplicas = val
								}
							}
							// LastKnownElr
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.LastKnownElr = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.LastKnownElr = val
								}
							}
							// OfflineReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
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
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								}
							}
							// EligibleLeaderReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.EligibleLeaderReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.EligibleLeaderReplicas); err != nil {
										return err
									}
								}
							}
							// LastKnownElr
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.LastKnownElr); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.LastKnownElr); err != nil {
										return err
									}
								}
							}
							// OfflineReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
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
						tempElem.Partitions = make([]DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition)
						}
					}
				}
				// TopicAuthorizedOperations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.TopicAuthorizedOperations = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.Name); err != nil {
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
				// IsInternal
				if version >= 0 && version <= 999 {
					if err := protocol.WriteBool(elemW, tempElem.IsInternal); err != nil {
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
						// ErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
								return err
							}
						}
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
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
						// ReplicaNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].ReplicaNodes); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].ReplicaNodes); err != nil {
									return err
								}
							}
						}
						// IsrNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].IsrNodes); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].IsrNodes); err != nil {
									return err
								}
							}
						}
						// EligibleLeaderReplicas
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].EligibleLeaderReplicas); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].EligibleLeaderReplicas); err != nil {
									return err
								}
							}
						}
						// LastKnownElr
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].LastKnownElr); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].LastKnownElr); err != nil {
									return err
								}
							}
						}
						// OfflineReplicas
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].OfflineReplicas); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].OfflineReplicas); err != nil {
									return err
								}
							}
						}
					}
				}
				// TopicAuthorizedOperations
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.TopicAuthorizedOperations); err != nil {
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
			m.Topics = make([]DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic)
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
				var tempElem DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
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
				// IsInternal
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					tempElem.IsInternal = val
				}
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition
						elemR := bytes.NewReader(data)
						// ErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ErrorCode = val
						}
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
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
						// ReplicaNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ReplicaNodes = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ReplicaNodes = val
							}
						}
						// IsrNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.IsrNodes = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.IsrNodes = val
							}
						}
						// EligibleLeaderReplicas
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EligibleLeaderReplicas = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EligibleLeaderReplicas = val
							}
						}
						// LastKnownElr
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.LastKnownElr = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.LastKnownElr = val
							}
						}
						// OfflineReplicas
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.OfflineReplicas = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.OfflineReplicas = val
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
							var tempElem DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition
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
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
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
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								}
							}
							// EligibleLeaderReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.EligibleLeaderReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.EligibleLeaderReplicas = val
								}
							}
							// LastKnownElr
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.LastKnownElr = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.LastKnownElr = val
								}
							}
							// OfflineReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
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
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								}
							}
							// EligibleLeaderReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.EligibleLeaderReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.EligibleLeaderReplicas); err != nil {
										return err
									}
								}
							}
							// LastKnownElr
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.LastKnownElr); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.LastKnownElr); err != nil {
										return err
									}
								}
							}
							// OfflineReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
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
						tempElem.Partitions = make([]DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition)
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
							var tempElem DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition
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
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
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
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								}
							}
							// EligibleLeaderReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.EligibleLeaderReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.EligibleLeaderReplicas = val
								}
							}
							// LastKnownElr
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.LastKnownElr = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.LastKnownElr = val
								}
							}
							// OfflineReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
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
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								}
							}
							// EligibleLeaderReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.EligibleLeaderReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.EligibleLeaderReplicas); err != nil {
										return err
									}
								}
							}
							// LastKnownElr
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.LastKnownElr); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.LastKnownElr); err != nil {
										return err
									}
								}
							}
							// OfflineReplicas
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
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
						tempElem.Partitions = make([]DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition)
						}
					}
				}
				// TopicAuthorizedOperations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.TopicAuthorizedOperations = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.Name); err != nil {
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
				// IsInternal
				if version >= 0 && version <= 999 {
					if err := protocol.WriteBool(elemW, tempElem.IsInternal); err != nil {
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
						// ErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
								return err
							}
						}
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
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
						// ReplicaNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].ReplicaNodes); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].ReplicaNodes); err != nil {
									return err
								}
							}
						}
						// IsrNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].IsrNodes); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].IsrNodes); err != nil {
									return err
								}
							}
						}
						// EligibleLeaderReplicas
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].EligibleLeaderReplicas); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].EligibleLeaderReplicas); err != nil {
									return err
								}
							}
						}
						// LastKnownElr
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].LastKnownElr); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].LastKnownElr); err != nil {
									return err
								}
							}
						}
						// OfflineReplicas
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].OfflineReplicas); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].OfflineReplicas); err != nil {
									return err
								}
							}
						}
					}
				}
				// TopicAuthorizedOperations
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.TopicAuthorizedOperations); err != nil {
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
			m.Topics = make([]DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic)
			}
		}
	}
	// NextCursor
	if version >= 0 && version <= 999 {
		// TopicName
		if version >= 0 && version <= 999 {
			if isFlexible {
				val, err := protocol.ReadCompactString(r)
				if err != nil {
					return err
				}
				m.NextCursor.TopicName = val
			} else {
				val, err := protocol.ReadString(r)
				if err != nil {
					return err
				}
				m.NextCursor.TopicName = val
			}
		}
		// PartitionIndex
		if version >= 0 && version <= 999 {
			val, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.NextCursor.PartitionIndex = val
		}
		// Read tagged fields if flexible
		if isFlexible {
			if err := m.NextCursor.readTaggedFields(r, version); err != nil {
				return err
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

// DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic represents Each topic in the response..
type DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic struct {
	// The topic error, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The topic name.
	Name *string `json:"name" versions:"0-999"`
	// The topic id.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// True if the topic is internal.
	IsInternal bool `json:"isinternal" versions:"0-999"`
	// Each partition in the topic.
	Partitions []DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition `json:"partitions" versions:"0-999"`
	// 32-bit bitfield to represent authorized operations for this topic.
	TopicAuthorizedOperations int32 `json:"topicauthorizedoperations" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic.
func (m *DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic.
func (m *DescribeTopicPartitionsResponseDescribeTopicPartitionsResponseTopic) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition represents Each partition in the topic..
type DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition struct {
	// The partition error, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The ID of the leader broker.
	LeaderId int32 `json:"leaderid" versions:"0-999"`
	// The leader epoch of this partition.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// The set of all nodes that host this partition.
	ReplicaNodes []int32 `json:"replicanodes" versions:"0-999"`
	// The set of nodes that are in sync with the leader for this partition.
	IsrNodes []int32 `json:"isrnodes" versions:"0-999"`
	// The new eligible leader replicas otherwise.
	EligibleLeaderReplicas []int32 `json:"eligibleleaderreplicas" versions:"0-999"`
	// The last known ELR.
	LastKnownElr []int32 `json:"lastknownelr" versions:"0-999"`
	// The set of offline replicas of this partition.
	OfflineReplicas []int32 `json:"offlinereplicas" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition.
func (m *DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition.
func (m *DescribeTopicPartitionsResponseDescribeTopicPartitionsResponsePartition) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeTopicPartitionsResponseCursor represents The next topic and partition index to fetch details for..
type DescribeTopicPartitionsResponseCursor struct {
	// The name for the first topic to process.
	TopicName string `json:"topicname" versions:"0-999"`
	// The partition index to start with.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeTopicPartitionsResponseCursor.
func (m *DescribeTopicPartitionsResponseCursor) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeTopicPartitionsResponseCursor.
func (m *DescribeTopicPartitionsResponseCursor) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for DescribeTopicPartitionsResponse.
func (m *DescribeTopicPartitionsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeTopicPartitionsResponse.
func (m *DescribeTopicPartitionsResponse) readTaggedFields(r io.Reader, version int16) error {
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
