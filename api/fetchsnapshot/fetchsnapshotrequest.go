package fetchsnapshot

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	FetchSnapshotRequestApiKey        = 59
	FetchSnapshotRequestHeaderVersion = 1
)

// FetchSnapshotRequest represents a request message.
type FetchSnapshotRequest struct {
	// The clusterId if known, this is used to validate metadata fetches prior to broker registration.
	ClusterId *string `json:"clusterid" versions:"0-999" tag:"0"`
	// The broker ID of the follower.
	ReplicaId int32 `json:"replicaid" versions:"0-999"`
	// The maximum bytes to fetch from all of the snapshots.
	MaxBytes int32 `json:"maxbytes" versions:"0-999"`
	// The topics to fetch.
	Topics []FetchSnapshotRequestTopicSnapshot `json:"topics" versions:"0-999"`
}

// Encode encodes a FetchSnapshotRequest to a byte slice for the given version.
func (m *FetchSnapshotRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a FetchSnapshotRequest from a byte slice for the given version.
func (m *FetchSnapshotRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a FetchSnapshotRequest to an io.Writer for the given version.
func (m *FetchSnapshotRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ClusterId
	if version >= 0 && version <= 999 {
	}
	// ReplicaId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ReplicaId); err != nil {
			return err
		}
	}
	// MaxBytes
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.MaxBytes); err != nil {
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
			structItem, ok := item.(FetchSnapshotRequestTopicSnapshot)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Name); err != nil {
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
					// Partition
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].Partition); err != nil {
							return nil, err
						}
					}
					// CurrentLeaderEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].CurrentLeaderEpoch); err != nil {
							return nil, err
						}
					}
					// SnapshotId
					if version >= 0 && version <= 999 {
						// EndOffset
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, structItem.Partitions[i].SnapshotId.EndOffset); err != nil {
								return nil, err
							}
						}
						// Epoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, structItem.Partitions[i].SnapshotId.Epoch); err != nil {
								return nil, err
							}
						}
					}
					// Position
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(elemW, structItem.Partitions[i].Position); err != nil {
							return nil, err
						}
					}
					// ReplicaDirectoryId
					if version >= 1 && version <= 999 {
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

// Read reads a FetchSnapshotRequest from an io.Reader for the given version.
func (m *FetchSnapshotRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ClusterId
	if version >= 0 && version <= 999 {
	}
	// ReplicaId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ReplicaId = val
	}
	// MaxBytes
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.MaxBytes = val
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem FetchSnapshotRequestTopicSnapshot
			elemR := bytes.NewReader(data)
			// Name
			if version >= 0 && version <= 999 {
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
				var tempElem FetchSnapshotRequestTopicSnapshot
				// Name
				if version >= 0 && version <= 999 {
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
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem FetchSnapshotRequestPartitionSnapshot
						elemR := bytes.NewReader(data)
						// Partition
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Partition = val
						}
						// CurrentLeaderEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.CurrentLeaderEpoch = val
						}
						// SnapshotId
						if version >= 0 && version <= 999 {
							// EndOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.SnapshotId.EndOffset = val
							}
							// Epoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.SnapshotId.Epoch = val
							}
						}
						// Position
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Position = val
						}
						// ReplicaDirectoryId
						if version >= 1 && version <= 999 {
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
							var tempElem FetchSnapshotRequestPartitionSnapshot
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
							}
							// CurrentLeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CurrentLeaderEpoch = val
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt64(r)
									if err != nil {
										return err
									}
									tempElem.SnapshotId.EndOffset = val
								}
								// Epoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.SnapshotId.Epoch = val
								}
							}
							// Position
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.Position = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
									return err
								}
							}
							// CurrentLeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.CurrentLeaderEpoch); err != nil {
									return err
								}
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.SnapshotId.EndOffset); err != nil {
										return err
									}
								}
								// Epoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.SnapshotId.Epoch); err != nil {
										return err
									}
								}
							}
							// Position
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.Position); err != nil {
									return err
								}
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
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
						tempElem.Partitions = make([]FetchSnapshotRequestPartitionSnapshot, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(FetchSnapshotRequestPartitionSnapshot)
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
							var tempElem FetchSnapshotRequestPartitionSnapshot
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
							}
							// CurrentLeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CurrentLeaderEpoch = val
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt64(r)
									if err != nil {
										return err
									}
									tempElem.SnapshotId.EndOffset = val
								}
								// Epoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.SnapshotId.Epoch = val
								}
							}
							// Position
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.Position = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
									return err
								}
							}
							// CurrentLeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.CurrentLeaderEpoch); err != nil {
									return err
								}
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.SnapshotId.EndOffset); err != nil {
										return err
									}
								}
								// Epoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.SnapshotId.Epoch); err != nil {
										return err
									}
								}
							}
							// Position
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.Position); err != nil {
									return err
								}
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
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
						tempElem.Partitions = make([]FetchSnapshotRequestPartitionSnapshot, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(FetchSnapshotRequestPartitionSnapshot)
						}
					}
				}
				// Name
				if version >= 0 && version <= 999 {
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
						// Partition
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].Partition); err != nil {
								return err
							}
						}
						// CurrentLeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CurrentLeaderEpoch); err != nil {
								return err
							}
						}
						// SnapshotId
						if version >= 0 && version <= 999 {
							// EndOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].SnapshotId.EndOffset); err != nil {
									return err
								}
							}
							// Epoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].SnapshotId.Epoch); err != nil {
									return err
								}
							}
						}
						// Position
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Position); err != nil {
								return err
							}
						}
						// ReplicaDirectoryId
						if version >= 1 && version <= 999 {
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
			m.Topics = make([]FetchSnapshotRequestTopicSnapshot, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(FetchSnapshotRequestTopicSnapshot)
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
				var tempElem FetchSnapshotRequestTopicSnapshot
				// Name
				if version >= 0 && version <= 999 {
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
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem FetchSnapshotRequestPartitionSnapshot
						elemR := bytes.NewReader(data)
						// Partition
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Partition = val
						}
						// CurrentLeaderEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.CurrentLeaderEpoch = val
						}
						// SnapshotId
						if version >= 0 && version <= 999 {
							// EndOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.SnapshotId.EndOffset = val
							}
							// Epoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.SnapshotId.Epoch = val
							}
						}
						// Position
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt64(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Position = val
						}
						// ReplicaDirectoryId
						if version >= 1 && version <= 999 {
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
							var tempElem FetchSnapshotRequestPartitionSnapshot
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
							}
							// CurrentLeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CurrentLeaderEpoch = val
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt64(r)
									if err != nil {
										return err
									}
									tempElem.SnapshotId.EndOffset = val
								}
								// Epoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.SnapshotId.Epoch = val
								}
							}
							// Position
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.Position = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
									return err
								}
							}
							// CurrentLeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.CurrentLeaderEpoch); err != nil {
									return err
								}
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.SnapshotId.EndOffset); err != nil {
										return err
									}
								}
								// Epoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.SnapshotId.Epoch); err != nil {
										return err
									}
								}
							}
							// Position
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.Position); err != nil {
									return err
								}
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
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
						tempElem.Partitions = make([]FetchSnapshotRequestPartitionSnapshot, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(FetchSnapshotRequestPartitionSnapshot)
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
							var tempElem FetchSnapshotRequestPartitionSnapshot
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Partition = val
							}
							// CurrentLeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.CurrentLeaderEpoch = val
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt64(r)
									if err != nil {
										return err
									}
									tempElem.SnapshotId.EndOffset = val
								}
								// Epoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.SnapshotId.Epoch = val
								}
							}
							// Position
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								tempElem.Position = val
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
									return err
								}
							}
							// CurrentLeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.CurrentLeaderEpoch); err != nil {
									return err
								}
							}
							// SnapshotId
							if version >= 0 && version <= 999 {
								// EndOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.SnapshotId.EndOffset); err != nil {
										return err
									}
								}
								// Epoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.SnapshotId.Epoch); err != nil {
										return err
									}
								}
							}
							// Position
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.Position); err != nil {
									return err
								}
							}
							// ReplicaDirectoryId
							if version >= 1 && version <= 999 {
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
						tempElem.Partitions = make([]FetchSnapshotRequestPartitionSnapshot, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(FetchSnapshotRequestPartitionSnapshot)
						}
					}
				}
				// Name
				if version >= 0 && version <= 999 {
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
						// Partition
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].Partition); err != nil {
								return err
							}
						}
						// CurrentLeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CurrentLeaderEpoch); err != nil {
								return err
							}
						}
						// SnapshotId
						if version >= 0 && version <= 999 {
							// EndOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].SnapshotId.EndOffset); err != nil {
									return err
								}
							}
							// Epoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].SnapshotId.Epoch); err != nil {
									return err
								}
							}
						}
						// Position
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].Position); err != nil {
								return err
							}
						}
						// ReplicaDirectoryId
						if version >= 1 && version <= 999 {
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
			m.Topics = make([]FetchSnapshotRequestTopicSnapshot, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(FetchSnapshotRequestTopicSnapshot)
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

// FetchSnapshotRequestTopicSnapshot represents The topics to fetch..
type FetchSnapshotRequestTopicSnapshot struct {
	// The name of the topic to fetch.
	Name string `json:"name" versions:"0-999"`
	// The partitions to fetch.
	Partitions []FetchSnapshotRequestPartitionSnapshot `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for FetchSnapshotRequestTopicSnapshot.
func (m *FetchSnapshotRequestTopicSnapshot) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for FetchSnapshotRequestTopicSnapshot.
func (m *FetchSnapshotRequestTopicSnapshot) readTaggedFields(r io.Reader, version int16) error {
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

// FetchSnapshotRequestPartitionSnapshot represents The partitions to fetch..
type FetchSnapshotRequestPartitionSnapshot struct {
	// The partition index.
	Partition int32 `json:"partition" versions:"0-999"`
	// The current leader epoch of the partition, -1 for unknown leader epoch.
	CurrentLeaderEpoch int32 `json:"currentleaderepoch" versions:"0-999"`
	// The snapshot endOffset and epoch to fetch.
	SnapshotId FetchSnapshotRequestSnapshotId `json:"snapshotid" versions:"0-999"`
	// The byte position within the snapshot to start fetching from.
	Position int64 `json:"position" versions:"0-999"`
	// The directory id of the follower fetching.
	ReplicaDirectoryId uuid.UUID `json:"replicadirectoryid" versions:"1-999" tag:"0"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for FetchSnapshotRequestPartitionSnapshot.
func (m *FetchSnapshotRequestPartitionSnapshot) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	// ReplicaDirectoryId (tag 0)
	if version >= 1 {
		if true {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(0)); err != nil {
				return err
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

// readTaggedFields reads tagged fields for FetchSnapshotRequestPartitionSnapshot.
func (m *FetchSnapshotRequestPartitionSnapshot) readTaggedFields(r io.Reader, version int16) error {
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
		case 0: // ReplicaDirectoryId
			if version >= 1 {
			}
		default:
			// Unknown tag, skip it
		}
	}

	return nil
}

// FetchSnapshotRequestSnapshotId represents The snapshot endOffset and epoch to fetch..
type FetchSnapshotRequestSnapshotId struct {
	// The end offset of the snapshot.
	EndOffset int64 `json:"endoffset" versions:"0-999"`
	// The epoch of the snapshot.
	Epoch int32 `json:"epoch" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for FetchSnapshotRequestSnapshotId.
func (m *FetchSnapshotRequestSnapshotId) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for FetchSnapshotRequestSnapshotId.
func (m *FetchSnapshotRequestSnapshotId) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for FetchSnapshotRequest.
func (m *FetchSnapshotRequest) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	isFlexible := version >= 0

	// ClusterId (tag 0)
	if version >= 0 {
		if m.ClusterId != nil && *m.ClusterId != "" {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(0)); err != nil {
				return err
			}
			if err := protocol.WriteCompactNullableString(&taggedFieldsBuf, m.ClusterId); err != nil {
				return err
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

// readTaggedFields reads tagged fields for FetchSnapshotRequest.
func (m *FetchSnapshotRequest) readTaggedFields(r io.Reader, version int16) error {
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
		case 0: // ClusterId
			if version >= 0 {
				val, err := protocol.ReadCompactNullableString(r)
				if err != nil {
					return err
				}
				m.ClusterId = val
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
