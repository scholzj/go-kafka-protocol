package writesharegroupstate

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	WriteShareGroupStateRequestApiKey        = 85
	WriteShareGroupStateRequestHeaderVersion = 1
)

// WriteShareGroupStateRequest represents a request message.
type WriteShareGroupStateRequest struct {
	// The group identifier.
	GroupId string `json:"groupid" versions:"0-999"`
	// The data for the topics.
	Topics []WriteShareGroupStateRequestWriteStateData `json:"topics" versions:"0-999"`
}

// Encode encodes a WriteShareGroupStateRequest to a byte slice for the given version.
func (m *WriteShareGroupStateRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a WriteShareGroupStateRequest from a byte slice for the given version.
func (m *WriteShareGroupStateRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a WriteShareGroupStateRequest to an io.Writer for the given version.
func (m *WriteShareGroupStateRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
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
			// TopicId
			if version >= 0 && version <= 999 {
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
					// Partition
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].Partition); err != nil {
							return err
						}
					}
					// StateEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].StateEpoch); err != nil {
							return err
						}
					}
					// LeaderEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].LeaderEpoch); err != nil {
							return err
						}
					}
					// StartOffset
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].StartOffset); err != nil {
							return err
						}
					}
					// DeliveryCompleteCount
					if version >= 1 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].DeliveryCompleteCount); err != nil {
							return err
						}
					}
					// StateBatches
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].StateBatches) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].StateBatches))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].StateBatches {
							// FirstOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].StateBatches[i].FirstOffset); err != nil {
									return err
								}
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].StateBatches[i].LastOffset); err != nil {
									return err
								}
							}
							// DeliveryState
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt8(w, m.Topics[i].Partitions[i].StateBatches[i].DeliveryState); err != nil {
									return err
								}
							}
							// DeliveryCount
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(w, m.Topics[i].Partitions[i].StateBatches[i].DeliveryCount); err != nil {
									return err
								}
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

// Read reads a WriteShareGroupStateRequest from an io.Reader for the given version.
func (m *WriteShareGroupStateRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
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
			m.Topics = make([]WriteShareGroupStateRequestWriteStateData, length)
			for i := int32(0); i < length; i++ {
				// TopicId
				if version >= 0 && version <= 999 {
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
						m.Topics[i].Partitions = make([]WriteShareGroupStateRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].StateEpoch = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].StartOffset = val
							}
							// DeliveryCompleteCount
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].DeliveryCompleteCount = val
							}
							// StateBatches
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
									m.Topics[i].Partitions[i].StateBatches = make([]WriteShareGroupStateRequestStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryCount = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].StateBatches = make([]WriteShareGroupStateRequestStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryCount = val
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
						m.Topics[i].Partitions = make([]WriteShareGroupStateRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].StateEpoch = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].StartOffset = val
							}
							// DeliveryCompleteCount
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].DeliveryCompleteCount = val
							}
							// StateBatches
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
									m.Topics[i].Partitions[i].StateBatches = make([]WriteShareGroupStateRequestStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryCount = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].StateBatches = make([]WriteShareGroupStateRequestStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryCount = val
										}
									}
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
			m.Topics = make([]WriteShareGroupStateRequestWriteStateData, length)
			for i := int32(0); i < length; i++ {
				// TopicId
				if version >= 0 && version <= 999 {
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
						m.Topics[i].Partitions = make([]WriteShareGroupStateRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].StateEpoch = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].StartOffset = val
							}
							// DeliveryCompleteCount
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].DeliveryCompleteCount = val
							}
							// StateBatches
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
									m.Topics[i].Partitions[i].StateBatches = make([]WriteShareGroupStateRequestStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryCount = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].StateBatches = make([]WriteShareGroupStateRequestStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryCount = val
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
						m.Topics[i].Partitions = make([]WriteShareGroupStateRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].Partition = val
							}
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].StateEpoch = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].StartOffset = val
							}
							// DeliveryCompleteCount
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].DeliveryCompleteCount = val
							}
							// StateBatches
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
									m.Topics[i].Partitions[i].StateBatches = make([]WriteShareGroupStateRequestStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryCount = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].StateBatches = make([]WriteShareGroupStateRequestStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].StateBatches[i].DeliveryCount = val
										}
									}
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

// WriteShareGroupStateRequestWriteStateData represents The data for the topics..
type WriteShareGroupStateRequestWriteStateData struct {
	// The topic identifier.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// The data for the partitions.
	Partitions []WriteShareGroupStateRequestPartitionData `json:"partitions" versions:"0-999"`
}

// WriteShareGroupStateRequestPartitionData represents The data for the partitions..
type WriteShareGroupStateRequestPartitionData struct {
	// The partition index.
	Partition int32 `json:"partition" versions:"0-999"`
	// The state epoch of the share-partition.
	StateEpoch int32 `json:"stateepoch" versions:"0-999"`
	// The leader epoch of the share-partition.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// The share-partition start offset, or -1 if the start offset is not being written.
	StartOffset int64 `json:"startoffset" versions:"0-999"`
	// The number of offsets greater than or equal to share-partition start offset for which delivery has been completed.
	DeliveryCompleteCount int32 `json:"deliverycompletecount" versions:"1-999"`
	// The state batches for the share-partition.
	StateBatches []WriteShareGroupStateRequestStateBatch `json:"statebatches" versions:"0-999"`
}

// WriteShareGroupStateRequestStateBatch represents The state batches for the share-partition..
type WriteShareGroupStateRequestStateBatch struct {
	// The first offset of this state batch.
	FirstOffset int64 `json:"firstoffset" versions:"0-999"`
	// The last offset of this state batch.
	LastOffset int64 `json:"lastoffset" versions:"0-999"`
	// The delivery state - 0:Available,2:Acked,4:Archived.
	DeliveryState int8 `json:"deliverystate" versions:"0-999"`
	// The delivery count.
	DeliveryCount int16 `json:"deliverycount" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for WriteShareGroupStateRequest.
func (m *WriteShareGroupStateRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for WriteShareGroupStateRequest.
func (m *WriteShareGroupStateRequest) readTaggedFields(r io.Reader, version int16) error {
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
