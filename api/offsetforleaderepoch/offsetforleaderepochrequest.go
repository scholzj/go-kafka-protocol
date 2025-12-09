package offsetforleaderepoch

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	OffsetForLeaderEpochRequestApiKey        = 23
	OffsetForLeaderEpochRequestHeaderVersion = 1
)

// OffsetForLeaderEpochRequest represents a request message.
type OffsetForLeaderEpochRequest struct {
	// The broker ID of the follower, of -1 if this request is from a consumer.
	ReplicaId int32 `json:"replicaid" versions:"3-999"`
	// Each topic to get offsets for.
	Topics []OffsetForLeaderEpochRequestOffsetForLeaderTopic `json:"topics" versions:"0-999"`
}

// Encode encodes a OffsetForLeaderEpochRequest to a byte slice for the given version.
func (m *OffsetForLeaderEpochRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a OffsetForLeaderEpochRequest from a byte slice for the given version.
func (m *OffsetForLeaderEpochRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a OffsetForLeaderEpochRequest to an io.Writer for the given version.
func (m *OffsetForLeaderEpochRequest) Write(w io.Writer, version int16) error {
	if version < 2 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// ReplicaId
	if version >= 3 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ReplicaId); err != nil {
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
			// Topic
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Topics[i].Topic); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Topics[i].Topic); err != nil {
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
					// Partition
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].Partition); err != nil {
							return err
						}
					}
					// CurrentLeaderEpoch
					if version >= 2 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].CurrentLeaderEpoch); err != nil {
							return err
						}
					}
					// LeaderEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].LeaderEpoch); err != nil {
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

// Read reads a OffsetForLeaderEpochRequest from an io.Reader for the given version.
func (m *OffsetForLeaderEpochRequest) Read(r io.Reader, version int16) error {
	if version < 2 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// ReplicaId
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ReplicaId = val
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
			m.Topics = make([]OffsetForLeaderEpochRequestOffsetForLeaderTopic, length)
			for i := int32(0); i < length; i++ {
				// Topic
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Topic = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Topic = val
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
						m.Topics[i].Partitions = make([]OffsetForLeaderEpochRequestOffsetForLeaderPartition, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// CurrentLeaderEpoch
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CurrentLeaderEpoch = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]OffsetForLeaderEpochRequestOffsetForLeaderPartition, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// CurrentLeaderEpoch
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CurrentLeaderEpoch = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
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
			m.Topics = make([]OffsetForLeaderEpochRequestOffsetForLeaderTopic, length)
			for i := int32(0); i < length; i++ {
				// Topic
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Topic = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Topic = val
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
						m.Topics[i].Partitions = make([]OffsetForLeaderEpochRequestOffsetForLeaderPartition, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// CurrentLeaderEpoch
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CurrentLeaderEpoch = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]OffsetForLeaderEpochRequestOffsetForLeaderPartition, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// CurrentLeaderEpoch
							if version >= 2 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CurrentLeaderEpoch = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
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

// OffsetForLeaderEpochRequestOffsetForLeaderTopic represents Each topic to get offsets for..
type OffsetForLeaderEpochRequestOffsetForLeaderTopic struct {
	// The topic name.
	Topic string `json:"topic" versions:"0-999"`
	// Each partition to get offsets for.
	Partitions []OffsetForLeaderEpochRequestOffsetForLeaderPartition `json:"partitions" versions:"0-999"`
}

// OffsetForLeaderEpochRequestOffsetForLeaderPartition represents Each partition to get offsets for..
type OffsetForLeaderEpochRequestOffsetForLeaderPartition struct {
	// The partition index.
	Partition int32 `json:"partition" versions:"0-999"`
	// An epoch used to fence consumers/replicas with old metadata. If the epoch provided by the client is larger than the current epoch known to the broker, then the UNKNOWN_LEADER_EPOCH error code will be returned. If the provided epoch is smaller, then the FENCED_LEADER_EPOCH error code will be returned.
	CurrentLeaderEpoch int32 `json:"currentleaderepoch" versions:"2-999"`
	// The epoch to look up an offset for.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for OffsetForLeaderEpochRequest.
func (m *OffsetForLeaderEpochRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetForLeaderEpochRequest.
func (m *OffsetForLeaderEpochRequest) readTaggedFields(r io.Reader, version int16) error {
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
