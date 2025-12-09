package offsetcommit

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	OffsetCommitRequestApiKey        = 8
	OffsetCommitRequestHeaderVersion = 1
)

// OffsetCommitRequest represents a request message.
type OffsetCommitRequest struct {
	// The unique group identifier.
	GroupId string `json:"groupid" versions:"0-999"`
	// The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
	GenerationIdOrMemberEpoch int32 `json:"generationidormemberepoch" versions:"1-999"`
	// The member ID assigned by the group coordinator.
	MemberId string `json:"memberid" versions:"1-999"`
	// The unique identifier of the consumer instance provided by end user.
	GroupInstanceId *string `json:"groupinstanceid" versions:"7-999"`
	// The time period in ms to retain the offset.
	RetentionTimeMs int64 `json:"retentiontimems" versions:"2-4"`
	// The topics to commit offsets for.
	Topics []OffsetCommitRequestOffsetCommitRequestTopic `json:"topics" versions:"0-999"`
}

// Encode encodes a OffsetCommitRequest to a byte slice for the given version.
func (m *OffsetCommitRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a OffsetCommitRequest from a byte slice for the given version.
func (m *OffsetCommitRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a OffsetCommitRequest to an io.Writer for the given version.
func (m *OffsetCommitRequest) Write(w io.Writer, version int16) error {
	if version < 2 || version > 10 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 8 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.GroupId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.GroupId); err != nil {
				return err
			}
		}
	}
	// GenerationIdOrMemberEpoch
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.GenerationIdOrMemberEpoch); err != nil {
			return err
		}
	}
	// MemberId
	if version >= 1 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.MemberId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.MemberId); err != nil {
				return err
			}
		}
	}
	// GroupInstanceId
	if version >= 7 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.GroupInstanceId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.GroupInstanceId); err != nil {
				return err
			}
		}
	}
	// RetentionTimeMs
	if version >= 2 && version <= 4 {
		if err := protocol.WriteInt64(w, m.RetentionTimeMs); err != nil {
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
			// Name
			if version >= 0 && version <= 9 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Topics[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Topics[i].Name); err != nil {
						return err
					}
				}
			}
			// TopicId
			if version >= 10 && version <= 999 {
				if err := protocol.WriteUUID(w, m.Topics[i].TopicId); err != nil {
					return err
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
					// CommittedOffset
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].CommittedOffset); err != nil {
							return err
						}
					}
					// CommittedLeaderEpoch
					if version >= 6 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].CommittedLeaderEpoch); err != nil {
							return err
						}
					}
					// CommittedMetadata
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Topics[i].Partitions[i].CommittedMetadata); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Topics[i].Partitions[i].CommittedMetadata); err != nil {
								return err
							}
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

// Read reads a OffsetCommitRequest from an io.Reader for the given version.
func (m *OffsetCommitRequest) Read(r io.Reader, version int16) error {
	if version < 2 || version > 10 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 8 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		}
	}
	// GenerationIdOrMemberEpoch
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.GenerationIdOrMemberEpoch = val
	}
	// MemberId
	if version >= 1 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.MemberId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.MemberId = val
		}
	}
	// GroupInstanceId
	if version >= 7 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.GroupInstanceId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.GroupInstanceId = val
		}
	}
	// RetentionTimeMs
	if version >= 2 && version <= 4 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.RetentionTimeMs = val
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
			m.Topics = make([]OffsetCommitRequestOffsetCommitRequestTopic, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 9 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Name = val
					}
				}
				// TopicId
				if version >= 10 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Topics[i].TopicId = val
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
						m.Topics[i].Partitions = make([]OffsetCommitRequestOffsetCommitRequestPartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// CommittedOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CommittedOffset = val
							}
							// CommittedLeaderEpoch
							if version >= 6 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CommittedLeaderEpoch = val
							}
							// CommittedMetadata
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CommittedMetadata = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CommittedMetadata = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]OffsetCommitRequestOffsetCommitRequestPartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// CommittedOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CommittedOffset = val
							}
							// CommittedLeaderEpoch
							if version >= 6 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CommittedLeaderEpoch = val
							}
							// CommittedMetadata
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CommittedMetadata = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CommittedMetadata = val
								}
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
			m.Topics = make([]OffsetCommitRequestOffsetCommitRequestTopic, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 9 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Name = val
					}
				}
				// TopicId
				if version >= 10 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Topics[i].TopicId = val
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
						m.Topics[i].Partitions = make([]OffsetCommitRequestOffsetCommitRequestPartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// CommittedOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CommittedOffset = val
							}
							// CommittedLeaderEpoch
							if version >= 6 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CommittedLeaderEpoch = val
							}
							// CommittedMetadata
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CommittedMetadata = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CommittedMetadata = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]OffsetCommitRequestOffsetCommitRequestPartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// CommittedOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CommittedOffset = val
							}
							// CommittedLeaderEpoch
							if version >= 6 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CommittedLeaderEpoch = val
							}
							// CommittedMetadata
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CommittedMetadata = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CommittedMetadata = val
								}
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

// OffsetCommitRequestOffsetCommitRequestTopic represents The topics to commit offsets for..
type OffsetCommitRequestOffsetCommitRequestTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-9"`
	// The topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"10-999"`
	// Each partition to commit offsets for.
	Partitions []OffsetCommitRequestOffsetCommitRequestPartition `json:"partitions" versions:"0-999"`
}

// OffsetCommitRequestOffsetCommitRequestPartition represents Each partition to commit offsets for..
type OffsetCommitRequestOffsetCommitRequestPartition struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The message offset to be committed.
	CommittedOffset int64 `json:"committedoffset" versions:"0-999"`
	// The leader epoch of this partition.
	CommittedLeaderEpoch int32 `json:"committedleaderepoch" versions:"6-999"`
	// Any associated metadata the client wants to keep.
	CommittedMetadata *string `json:"committedmetadata" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for OffsetCommitRequest.
func (m *OffsetCommitRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetCommitRequest.
func (m *OffsetCommitRequest) readTaggedFields(r io.Reader, version int16) error {
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
