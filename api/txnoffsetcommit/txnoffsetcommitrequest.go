package txnoffsetcommit

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	TxnOffsetCommitRequestApiKey        = 28
	TxnOffsetCommitRequestHeaderVersion = 1
)

// TxnOffsetCommitRequest represents a request message.
type TxnOffsetCommitRequest struct {
	// The ID of the transaction.
	TransactionalId string `json:"transactionalid" versions:"0-999"`
	// The ID of the group.
	GroupId string `json:"groupid" versions:"0-999"`
	// The current producer ID in use by the transactional ID.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// The current epoch associated with the producer ID.
	ProducerEpoch int16 `json:"producerepoch" versions:"0-999"`
	// The generation of the consumer.
	GenerationId int32 `json:"generationid" versions:"3-999"`
	// The member ID assigned by the group coordinator.
	MemberId string `json:"memberid" versions:"3-999"`
	// The unique identifier of the consumer instance provided by end user.
	GroupInstanceId *string `json:"groupinstanceid" versions:"3-999"`
	// Each topic that we want to commit offsets for.
	Topics []TxnOffsetCommitRequestTxnOffsetCommitRequestTopic `json:"topics" versions:"0-999"`
}

// Encode encodes a TxnOffsetCommitRequest to a byte slice for the given version.
func (m *TxnOffsetCommitRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a TxnOffsetCommitRequest from a byte slice for the given version.
func (m *TxnOffsetCommitRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a TxnOffsetCommitRequest to an io.Writer for the given version.
func (m *TxnOffsetCommitRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// TransactionalId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.TransactionalId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.TransactionalId); err != nil {
				return err
			}
		}
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
	// ProducerId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.ProducerId); err != nil {
			return err
		}
	}
	// ProducerEpoch
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ProducerEpoch); err != nil {
			return err
		}
	}
	// GenerationId
	if version >= 3 && version <= 999 {
		if err := protocol.WriteInt32(w, m.GenerationId); err != nil {
			return err
		}
	}
	// MemberId
	if version >= 3 && version <= 999 {
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
	if version >= 3 && version <= 999 {
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
			if version >= 0 && version <= 999 {
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
					if version >= 2 && version <= 999 {
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

// Read reads a TxnOffsetCommitRequest from an io.Reader for the given version.
func (m *TxnOffsetCommitRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// TransactionalId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.TransactionalId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.TransactionalId = val
		}
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
	// ProducerId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.ProducerId = val
	}
	// ProducerEpoch
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ProducerEpoch = val
	}
	// GenerationId
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.GenerationId = val
	}
	// MemberId
	if version >= 3 && version <= 999 {
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
	if version >= 3 && version <= 999 {
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
			m.Topics = make([]TxnOffsetCommitRequestTxnOffsetCommitRequestTopic, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
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
						m.Topics[i].Partitions = make([]TxnOffsetCommitRequestTxnOffsetCommitRequestPartition, length)
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
							if version >= 2 && version <= 999 {
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
						m.Topics[i].Partitions = make([]TxnOffsetCommitRequestTxnOffsetCommitRequestPartition, length)
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
							if version >= 2 && version <= 999 {
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
			m.Topics = make([]TxnOffsetCommitRequestTxnOffsetCommitRequestTopic, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
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
						m.Topics[i].Partitions = make([]TxnOffsetCommitRequestTxnOffsetCommitRequestPartition, length)
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
							if version >= 2 && version <= 999 {
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
						m.Topics[i].Partitions = make([]TxnOffsetCommitRequestTxnOffsetCommitRequestPartition, length)
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
							if version >= 2 && version <= 999 {
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

// TxnOffsetCommitRequestTxnOffsetCommitRequestTopic represents Each topic that we want to commit offsets for..
type TxnOffsetCommitRequestTxnOffsetCommitRequestTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The partitions inside the topic that we want to commit offsets for.
	Partitions []TxnOffsetCommitRequestTxnOffsetCommitRequestPartition `json:"partitions" versions:"0-999"`
}

// TxnOffsetCommitRequestTxnOffsetCommitRequestPartition represents The partitions inside the topic that we want to commit offsets for..
type TxnOffsetCommitRequestTxnOffsetCommitRequestPartition struct {
	// The index of the partition within the topic.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The message offset to be committed.
	CommittedOffset int64 `json:"committedoffset" versions:"0-999"`
	// The leader epoch of the last consumed record.
	CommittedLeaderEpoch int32 `json:"committedleaderepoch" versions:"2-999"`
	// Any associated metadata the client wants to keep.
	CommittedMetadata *string `json:"committedmetadata" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for TxnOffsetCommitRequest.
func (m *TxnOffsetCommitRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for TxnOffsetCommitRequest.
func (m *TxnOffsetCommitRequest) readTaggedFields(r io.Reader, version int16) error {
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
