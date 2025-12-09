package vote

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	VoteRequestApiKey        = 52
	VoteRequestHeaderVersion = 1
)

// VoteRequest represents a request message.
type VoteRequest struct {
	// The cluster id.
	ClusterId *string `json:"clusterid" versions:"0-999"`
	// The replica id of the voter receiving the request.
	VoterId int32 `json:"voterid" versions:"1-999"`
	// The topic data.
	Topics []VoteRequestTopicData `json:"topics" versions:"0-999"`
}

// Encode encodes a VoteRequest to a byte slice for the given version.
func (m *VoteRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a VoteRequest from a byte slice for the given version.
func (m *VoteRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a VoteRequest to an io.Writer for the given version.
func (m *VoteRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ClusterId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ClusterId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ClusterId); err != nil {
				return err
			}
		}
	}
	// VoterId
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.VoterId); err != nil {
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
			structItem, ok := item.(VoteRequestTopicData)
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
					// ReplicaEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].ReplicaEpoch); err != nil {
							return nil, err
						}
					}
					// ReplicaId
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].ReplicaId); err != nil {
							return nil, err
						}
					}
					// ReplicaDirectoryId
					if version >= 1 && version <= 999 {
						if err := protocol.WriteUUID(elemW, structItem.Partitions[i].ReplicaDirectoryId); err != nil {
							return nil, err
						}
					}
					// VoterDirectoryId
					if version >= 1 && version <= 999 {
						if err := protocol.WriteUUID(elemW, structItem.Partitions[i].VoterDirectoryId); err != nil {
							return nil, err
						}
					}
					// LastOffsetEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].LastOffsetEpoch); err != nil {
							return nil, err
						}
					}
					// LastOffset
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(elemW, structItem.Partitions[i].LastOffset); err != nil {
							return nil, err
						}
					}
					// PreVote
					if version >= 2 && version <= 999 {
						if err := protocol.WriteBool(elemW, structItem.Partitions[i].PreVote); err != nil {
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

// Read reads a VoteRequest from an io.Reader for the given version.
func (m *VoteRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ClusterId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		}
	}
	// VoterId
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.VoterId = val
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem VoteRequestTopicData
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
				var tempElem VoteRequestTopicData
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
						var elem VoteRequestPartitionData
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// ReplicaEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ReplicaEpoch = val
						}
						// ReplicaId
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ReplicaId = val
						}
						// ReplicaDirectoryId
						if version >= 1 && version <= 999 {
							val, err := protocol.ReadUUID(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ReplicaDirectoryId = val
						}
						// VoterDirectoryId
						if version >= 1 && version <= 999 {
							val, err := protocol.ReadUUID(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.VoterDirectoryId = val
						}
						// LastOffsetEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LastOffsetEpoch = val
						}
						// LastOffset
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LastOffset = val
						}
						// PreVote
						if version >= 2 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PreVote = val
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
							var tempElem VoteRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaEpoch = val
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaId = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaDirectoryId = val
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.VoterDirectoryId = val
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LastOffsetEpoch = val
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LastOffset = val
							}
							// PreVote
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.PreVote = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.ReplicaEpoch); err != nil {
									return err
								}
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
									return err
								}
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
									return err
								}
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.VoterDirectoryId); err != nil {
									return err
								}
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LastOffsetEpoch); err != nil {
									return err
								}
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
									return err
								}
							}
							// PreVote
							if version >= 2 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.PreVote); err != nil {
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
						tempElem.Partitions = make([]VoteRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(VoteRequestPartitionData)
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
							var tempElem VoteRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaEpoch = val
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaId = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaDirectoryId = val
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.VoterDirectoryId = val
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LastOffsetEpoch = val
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LastOffset = val
							}
							// PreVote
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.PreVote = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.ReplicaEpoch); err != nil {
									return err
								}
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
									return err
								}
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
									return err
								}
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.VoterDirectoryId); err != nil {
									return err
								}
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LastOffsetEpoch); err != nil {
									return err
								}
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
									return err
								}
							}
							// PreVote
							if version >= 2 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.PreVote); err != nil {
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
						tempElem.Partitions = make([]VoteRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(VoteRequestPartitionData)
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
						// ReplicaEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].ReplicaEpoch); err != nil {
								return err
							}
						}
						// ReplicaId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].ReplicaId); err != nil {
								return err
							}
						}
						// ReplicaDirectoryId
						if version >= 1 && version <= 999 {
							if err := protocol.WriteUUID(elemW, tempElem.Partitions[i].ReplicaDirectoryId); err != nil {
								return err
							}
						}
						// VoterDirectoryId
						if version >= 1 && version <= 999 {
							if err := protocol.WriteUUID(elemW, tempElem.Partitions[i].VoterDirectoryId); err != nil {
								return err
							}
						}
						// LastOffsetEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LastOffsetEpoch); err != nil {
								return err
							}
						}
						// LastOffset
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].LastOffset); err != nil {
								return err
							}
						}
						// PreVote
						if version >= 2 && version <= 999 {
							if err := protocol.WriteBool(elemW, tempElem.Partitions[i].PreVote); err != nil {
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
			m.Topics = make([]VoteRequestTopicData, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(VoteRequestTopicData)
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
				var tempElem VoteRequestTopicData
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
						var elem VoteRequestPartitionData
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// ReplicaEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ReplicaEpoch = val
						}
						// ReplicaId
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ReplicaId = val
						}
						// ReplicaDirectoryId
						if version >= 1 && version <= 999 {
							val, err := protocol.ReadUUID(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ReplicaDirectoryId = val
						}
						// VoterDirectoryId
						if version >= 1 && version <= 999 {
							val, err := protocol.ReadUUID(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.VoterDirectoryId = val
						}
						// LastOffsetEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LastOffsetEpoch = val
						}
						// LastOffset
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LastOffset = val
						}
						// PreVote
						if version >= 2 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PreVote = val
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
							var tempElem VoteRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaEpoch = val
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaId = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaDirectoryId = val
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.VoterDirectoryId = val
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LastOffsetEpoch = val
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LastOffset = val
							}
							// PreVote
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.PreVote = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.ReplicaEpoch); err != nil {
									return err
								}
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
									return err
								}
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
									return err
								}
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.VoterDirectoryId); err != nil {
									return err
								}
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LastOffsetEpoch); err != nil {
									return err
								}
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
									return err
								}
							}
							// PreVote
							if version >= 2 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.PreVote); err != nil {
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
						tempElem.Partitions = make([]VoteRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(VoteRequestPartitionData)
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
							var tempElem VoteRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaEpoch = val
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaId = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.ReplicaDirectoryId = val
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.VoterDirectoryId = val
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LastOffsetEpoch = val
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.LastOffset = val
							}
							// PreVote
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.PreVote = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.ReplicaEpoch); err != nil {
									return err
								}
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.ReplicaId); err != nil {
									return err
								}
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.ReplicaDirectoryId); err != nil {
									return err
								}
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.VoterDirectoryId); err != nil {
									return err
								}
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LastOffsetEpoch); err != nil {
									return err
								}
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
									return err
								}
							}
							// PreVote
							if version >= 2 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.PreVote); err != nil {
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
						tempElem.Partitions = make([]VoteRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(VoteRequestPartitionData)
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
						// ReplicaEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].ReplicaEpoch); err != nil {
								return err
							}
						}
						// ReplicaId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].ReplicaId); err != nil {
								return err
							}
						}
						// ReplicaDirectoryId
						if version >= 1 && version <= 999 {
							if err := protocol.WriteUUID(elemW, tempElem.Partitions[i].ReplicaDirectoryId); err != nil {
								return err
							}
						}
						// VoterDirectoryId
						if version >= 1 && version <= 999 {
							if err := protocol.WriteUUID(elemW, tempElem.Partitions[i].VoterDirectoryId); err != nil {
								return err
							}
						}
						// LastOffsetEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LastOffsetEpoch); err != nil {
								return err
							}
						}
						// LastOffset
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].LastOffset); err != nil {
								return err
							}
						}
						// PreVote
						if version >= 2 && version <= 999 {
							if err := protocol.WriteBool(elemW, tempElem.Partitions[i].PreVote); err != nil {
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
			m.Topics = make([]VoteRequestTopicData, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(VoteRequestTopicData)
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

// VoteRequestTopicData represents The topic data..
type VoteRequestTopicData struct {
	// The topic name.
	TopicName string `json:"topicname" versions:"0-999"`
	// The partition data.
	Partitions []VoteRequestPartitionData `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for VoteRequestTopicData.
func (m *VoteRequestTopicData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for VoteRequestTopicData.
func (m *VoteRequestTopicData) readTaggedFields(r io.Reader, version int16) error {
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

// VoteRequestPartitionData represents The partition data..
type VoteRequestPartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The epoch of the voter sending the request
	ReplicaEpoch int32 `json:"replicaepoch" versions:"0-999"`
	// The replica id of the voter sending the request
	ReplicaId int32 `json:"replicaid" versions:"0-999"`
	// The directory id of the voter sending the request
	ReplicaDirectoryId uuid.UUID `json:"replicadirectoryid" versions:"1-999"`
	// The directory id of the voter receiving the request
	VoterDirectoryId uuid.UUID `json:"voterdirectoryid" versions:"1-999"`
	// The epoch of the last record written to the metadata log.
	LastOffsetEpoch int32 `json:"lastoffsetepoch" versions:"0-999"`
	// The log end offset of the metadata log of the voter sending the request.
	LastOffset int64 `json:"lastoffset" versions:"0-999"`
	// Whether the request is a PreVote request (not persisted) or not.
	PreVote bool `json:"prevote" versions:"2-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for VoteRequestPartitionData.
func (m *VoteRequestPartitionData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for VoteRequestPartitionData.
func (m *VoteRequestPartitionData) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for VoteRequest.
func (m *VoteRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for VoteRequest.
func (m *VoteRequest) readTaggedFields(r io.Reader, version int16) error {
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
