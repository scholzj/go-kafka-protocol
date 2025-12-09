package offsetforleaderepoch

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	OffsetForLeaderEpochResponseApiKey        = 23
	OffsetForLeaderEpochResponseHeaderVersion = 1
)

// OffsetForLeaderEpochResponse represents a response message.
type OffsetForLeaderEpochResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"2-999"`
	// Each topic we fetched offsets for.
	Topics []OffsetForLeaderEpochResponseOffsetForLeaderTopicResult `json:"topics" versions:"0-999"`
}

// Encode encodes a OffsetForLeaderEpochResponse to a byte slice for the given version.
func (m *OffsetForLeaderEpochResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a OffsetForLeaderEpochResponse from a byte slice for the given version.
func (m *OffsetForLeaderEpochResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a OffsetForLeaderEpochResponse to an io.Writer for the given version.
func (m *OffsetForLeaderEpochResponse) Write(w io.Writer, version int16) error {
	if version < 2 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 2 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
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
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(w, m.Topics[i].Partitions[i].ErrorCode); err != nil {
							return err
						}
					}
					// Partition
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].Partition); err != nil {
							return err
						}
					}
					// LeaderEpoch
					if version >= 1 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].LeaderEpoch); err != nil {
							return err
						}
					}
					// EndOffset
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].EndOffset); err != nil {
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

// Read reads a OffsetForLeaderEpochResponse from an io.Reader for the given version.
func (m *OffsetForLeaderEpochResponse) Read(r io.Reader, version int16) error {
	if version < 2 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 2 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
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
			m.Topics = make([]OffsetForLeaderEpochResponseOffsetForLeaderTopicResult, length)
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
						m.Topics[i].Partitions = make([]OffsetForLeaderEpochResponseEpochEndOffset, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// LeaderEpoch
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// EndOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].EndOffset = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]OffsetForLeaderEpochResponseEpochEndOffset, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// LeaderEpoch
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// EndOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].EndOffset = val
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
			m.Topics = make([]OffsetForLeaderEpochResponseOffsetForLeaderTopicResult, length)
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
						m.Topics[i].Partitions = make([]OffsetForLeaderEpochResponseEpochEndOffset, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// LeaderEpoch
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// EndOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].EndOffset = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]OffsetForLeaderEpochResponseEpochEndOffset, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// LeaderEpoch
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// EndOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].EndOffset = val
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

// OffsetForLeaderEpochResponseOffsetForLeaderTopicResult represents Each topic we fetched offsets for..
type OffsetForLeaderEpochResponseOffsetForLeaderTopicResult struct {
	// The topic name.
	Topic string `json:"topic" versions:"0-999"`
	// Each partition in the topic we fetched offsets for.
	Partitions []OffsetForLeaderEpochResponseEpochEndOffset `json:"partitions" versions:"0-999"`
}

// OffsetForLeaderEpochResponseEpochEndOffset represents Each partition in the topic we fetched offsets for..
type OffsetForLeaderEpochResponseEpochEndOffset struct {
	// The error code 0, or if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The partition index.
	Partition int32 `json:"partition" versions:"0-999"`
	// The leader epoch of the partition.
	LeaderEpoch int32 `json:"leaderepoch" versions:"1-999"`
	// The end offset of the epoch.
	EndOffset int64 `json:"endoffset" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for OffsetForLeaderEpochResponse.
func (m *OffsetForLeaderEpochResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetForLeaderEpochResponse.
func (m *OffsetForLeaderEpochResponse) readTaggedFields(r io.Reader, version int16) error {
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
