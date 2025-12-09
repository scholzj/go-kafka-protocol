package fetchsnapshot

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	FetchSnapshotResponseApiKey        = 59
	FetchSnapshotResponseHeaderVersion = 1
)

// FetchSnapshotResponse represents a response message.
type FetchSnapshotResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The top level response error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The topics to fetch.
	Topics []FetchSnapshotResponseTopicSnapshot `json:"topics" versions:"0-999"`
	// Endpoints for all current-leaders enumerated in PartitionSnapshot.
	NodeEndpoints []FetchSnapshotResponseNodeEndpoint `json:"nodeendpoints" versions:"1-999" tag:"0"`
}

// Encode encodes a FetchSnapshotResponse to a byte slice for the given version.
func (m *FetchSnapshotResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a FetchSnapshotResponse from a byte slice for the given version.
func (m *FetchSnapshotResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a FetchSnapshotResponse to an io.Writer for the given version.
func (m *FetchSnapshotResponse) Write(w io.Writer, version int16) error {
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
	// ErrorCode
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
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
					// Index
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].Index); err != nil {
							return err
						}
					}
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(w, m.Topics[i].Partitions[i].ErrorCode); err != nil {
							return err
						}
					}
					// SnapshotId
					if version >= 0 && version <= 999 {
						// EndOffset
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].SnapshotId.EndOffset); err != nil {
								return err
							}
						}
						// Epoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].SnapshotId.Epoch); err != nil {
								return err
							}
						}
					}
					// CurrentLeader
					if version >= 0 && version <= 999 {
					}
					// Size
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].Size); err != nil {
							return err
						}
					}
					// Position
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].Position); err != nil {
							return err
						}
					}
					// UnalignedRecords
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableBytes(w, m.Topics[i].Partitions[i].UnalignedRecords); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableBytes(w, m.Topics[i].Partitions[i].UnalignedRecords); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}
	// NodeEndpoints
	if version >= 1 && version <= 999 {
	}
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a FetchSnapshotResponse from an io.Reader for the given version.
func (m *FetchSnapshotResponse) Read(r io.Reader, version int16) error {
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
	// ErrorCode
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
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
			m.Topics = make([]FetchSnapshotResponseTopicSnapshot, length)
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
						m.Topics[i].Partitions = make([]FetchSnapshotResponsePartitionSnapshot, length)
						for i := int32(0); i < length; i++ {
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Index = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt64(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].SnapshotId.EndOffset = val
								}
								// Epoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].SnapshotId.Epoch = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
							}
							// Size
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Size = val
							}
							// Position
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Position = val
							}
							// UnalignedRecords
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].UnalignedRecords = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].UnalignedRecords = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]FetchSnapshotResponsePartitionSnapshot, length)
						for i := int32(0); i < length; i++ {
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Index = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt64(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].SnapshotId.EndOffset = val
								}
								// Epoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].SnapshotId.Epoch = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
							}
							// Size
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Size = val
							}
							// Position
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Position = val
							}
							// UnalignedRecords
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].UnalignedRecords = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].UnalignedRecords = val
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
			m.Topics = make([]FetchSnapshotResponseTopicSnapshot, length)
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
						m.Topics[i].Partitions = make([]FetchSnapshotResponsePartitionSnapshot, length)
						for i := int32(0); i < length; i++ {
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Index = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt64(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].SnapshotId.EndOffset = val
								}
								// Epoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].SnapshotId.Epoch = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
							}
							// Size
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Size = val
							}
							// Position
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Position = val
							}
							// UnalignedRecords
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].UnalignedRecords = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].UnalignedRecords = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]FetchSnapshotResponsePartitionSnapshot, length)
						for i := int32(0); i < length; i++ {
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Index = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt64(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].SnapshotId.EndOffset = val
								}
								// Epoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].SnapshotId.Epoch = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
							}
							// Size
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Size = val
							}
							// Position
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Position = val
							}
							// UnalignedRecords
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].UnalignedRecords = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].UnalignedRecords = val
								}
							}
						}
					}
				}
			}
		}
	}
	// NodeEndpoints
	if version >= 1 && version <= 999 {
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// FetchSnapshotResponseTopicSnapshot represents The topics to fetch..
type FetchSnapshotResponseTopicSnapshot struct {
	// The name of the topic to fetch.
	Name string `json:"name" versions:"0-999"`
	// The partitions to fetch.
	Partitions []FetchSnapshotResponsePartitionSnapshot `json:"partitions" versions:"0-999"`
}

// FetchSnapshotResponsePartitionSnapshot represents The partitions to fetch..
type FetchSnapshotResponsePartitionSnapshot struct {
	// The partition index.
	Index int32 `json:"index" versions:"0-999"`
	// The error code, or 0 if there was no fetch error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The snapshot endOffset and epoch fetched.
	SnapshotId FetchSnapshotResponseSnapshotId `json:"snapshotid" versions:"0-999"`
	// The leader of the partition at the time of the snapshot.
	CurrentLeader FetchSnapshotResponseLeaderIdAndEpoch `json:"currentleader" versions:"0-999" tag:"0"`
	// The total size of the snapshot.
	Size int64 `json:"size" versions:"0-999"`
	// The starting byte position within the snapshot included in the Bytes field.
	Position int64 `json:"position" versions:"0-999"`
	// Snapshot data in records format which may not be aligned on an offset boundary.
	UnalignedRecords *[]byte `json:"unalignedrecords" versions:"0-999"`
}

// FetchSnapshotResponseSnapshotId represents The snapshot endOffset and epoch fetched..
type FetchSnapshotResponseSnapshotId struct {
	// The snapshot end offset.
	EndOffset int64 `json:"endoffset" versions:"0-999"`
	// The snapshot epoch.
	Epoch int32 `json:"epoch" versions:"0-999"`
}

// FetchSnapshotResponseLeaderIdAndEpoch represents The leader of the partition at the time of the snapshot..
type FetchSnapshotResponseLeaderIdAndEpoch struct {
	// The ID of the current leader or -1 if the leader is unknown.
	LeaderId int32 `json:"leaderid" versions:"0-999"`
	// The latest known leader epoch.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
}

// FetchSnapshotResponseNodeEndpoint represents Endpoints for all current-leaders enumerated in PartitionSnapshot..
type FetchSnapshotResponseNodeEndpoint struct {
	// The ID of the associated node.
	NodeId int32 `json:"nodeid" versions:"1-999"`
	// The node's hostname.
	Host string `json:"host" versions:"1-999"`
	// The node's port.
	Port uint16 `json:"port" versions:"1-999"`
}

// writeTaggedFields writes tagged fields for FetchSnapshotResponse.
func (m *FetchSnapshotResponse) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	isFlexible := version >= 0

	// NodeEndpoints (tag 0)
	if version >= 1 {
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
				if version >= 1 && version <= 999 {
					if err := protocol.WriteInt32(w, m.NodeEndpoints[i].NodeId); err != nil {
						return err
					}
				}
				// Host
				if version >= 1 && version <= 999 {
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
				if version >= 1 && version <= 999 {
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

// readTaggedFields reads tagged fields for FetchSnapshotResponse.
func (m *FetchSnapshotResponse) readTaggedFields(r io.Reader, version int16) error {
	isFlexible := version >= 0

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
			if version >= 1 {
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
					m.NodeEndpoints = make([]FetchSnapshotResponseNodeEndpoint, length-1)
					for i := uint32(0); i < length-1; i++ {
						// NodeId
						if version >= 1 && version <= 999 {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.NodeEndpoints[i].NodeId = val
						}
						// Host
						if version >= 1 && version <= 999 {
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
						if version >= 1 && version <= 999 {
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
