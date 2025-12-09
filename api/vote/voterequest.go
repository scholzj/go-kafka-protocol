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
		if isFlexible {
			length := uint32(len(m.Topics) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Topics))); err != nil {
				return err
			}
		}
		for i := range m.Topics {
			// TopicName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Topics[i].TopicName); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Topics[i].TopicName); err != nil {
						return err
					}
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Topics[i].Partitions) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions))); err != nil {
						return err
					}
				}
				for i := range m.Topics[i].Partitions {
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].PartitionIndex); err != nil {
							return err
						}
					}
					// ReplicaEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].ReplicaEpoch); err != nil {
							return err
						}
					}
					// ReplicaId
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].ReplicaId); err != nil {
							return err
						}
					}
					// ReplicaDirectoryId
					if version >= 1 && version <= 999 {
						if err := protocol.WriteUUID(w, m.Topics[i].Partitions[i].ReplicaDirectoryId); err != nil {
							return err
						}
					}
					// VoterDirectoryId
					if version >= 1 && version <= 999 {
						if err := protocol.WriteUUID(w, m.Topics[i].Partitions[i].VoterDirectoryId); err != nil {
							return err
						}
					}
					// LastOffsetEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].LastOffsetEpoch); err != nil {
							return err
						}
					}
					// LastOffset
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].LastOffset); err != nil {
							return err
						}
					}
					// PreVote
					if version >= 2 && version <= 999 {
						if err := protocol.WriteBool(w, m.Topics[i].Partitions[i].PreVote); err != nil {
							return err
						}
					}
				}
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
		var length int32
		if isFlexible {
			var lengthUint uint32
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint < 1 {
				return errors.New("invalid compact array length")
			}
			length = int32(lengthUint - 1)
			m.Topics = make([]VoteRequestTopicData, length)
			for i := int32(0); i < length; i++ {
				// TopicName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Topics[i].TopicName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Topics[i].TopicName = val
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					var length int32
					if isFlexible {
						var lengthUint uint32
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint < 1 {
							return errors.New("invalid compact array length")
						}
						length = int32(lengthUint - 1)
						m.Topics[i].Partitions = make([]VoteRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaEpoch = val
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaId = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaDirectoryId = val
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].VoterDirectoryId = val
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastOffsetEpoch = val
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastOffset = val
							}
							// PreVote
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PreVote = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]VoteRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaEpoch = val
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaId = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaDirectoryId = val
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].VoterDirectoryId = val
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastOffsetEpoch = val
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastOffset = val
							}
							// PreVote
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PreVote = val
							}
						}
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Topics = make([]VoteRequestTopicData, length)
			for i := int32(0); i < length; i++ {
				// TopicName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Topics[i].TopicName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Topics[i].TopicName = val
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					var length int32
					if isFlexible {
						var lengthUint uint32
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint < 1 {
							return errors.New("invalid compact array length")
						}
						length = int32(lengthUint - 1)
						m.Topics[i].Partitions = make([]VoteRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaEpoch = val
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaId = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaDirectoryId = val
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].VoterDirectoryId = val
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastOffsetEpoch = val
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastOffset = val
							}
							// PreVote
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PreVote = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]VoteRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ReplicaEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaEpoch = val
							}
							// ReplicaId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaId = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ReplicaDirectoryId = val
							}
							// VoterDirectoryId
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].VoterDirectoryId = val
							}
							// LastOffsetEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastOffsetEpoch = val
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastOffset = val
							}
							// PreVote
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PreVote = val
							}
						}
					}
				}
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
