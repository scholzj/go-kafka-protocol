package describequorum

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeQuorumResponseApiKey        = 55
	DescribeQuorumResponseHeaderVersion = 1
)

// DescribeQuorumResponse represents a response message.
type DescribeQuorumResponse struct {
	// The top level error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"2-999"`
	// The response from the describe quorum API.
	Topics []DescribeQuorumResponseTopicData `json:"topics" versions:"0-999"`
	// The nodes in the quorum.
	Nodes []DescribeQuorumResponseNode `json:"nodes" versions:"2-999"`
}

// Encode encodes a DescribeQuorumResponse to a byte slice for the given version.
func (m *DescribeQuorumResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeQuorumResponse from a byte slice for the given version.
func (m *DescribeQuorumResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeQuorumResponse to an io.Writer for the given version.
func (m *DescribeQuorumResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ErrorCode
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// ErrorMessage
	if version >= 2 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ErrorMessage); err != nil {
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
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(w, m.Topics[i].Partitions[i].ErrorCode); err != nil {
							return err
						}
					}
					// ErrorMessage
					if version >= 2 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Topics[i].Partitions[i].ErrorMessage); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Topics[i].Partitions[i].ErrorMessage); err != nil {
								return err
							}
						}
					}
					// LeaderId
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].LeaderId); err != nil {
							return err
						}
					}
					// LeaderEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].LeaderEpoch); err != nil {
							return err
						}
					}
					// HighWatermark
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].HighWatermark); err != nil {
							return err
						}
					}
					// CurrentVoters
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].CurrentVoters) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].CurrentVoters))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].CurrentVoters {
							// ReplicaId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaId); err != nil {
									return err
								}
							}
							// ReplicaDirectoryId
							if version >= 2 && version <= 999 {
								if err := protocol.WriteUUID(w, m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaDirectoryId); err != nil {
									return err
								}
							}
							// LogEndOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].CurrentVoters[i].LogEndOffset); err != nil {
									return err
								}
							}
							// LastFetchTimestamp
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].CurrentVoters[i].LastFetchTimestamp); err != nil {
									return err
								}
							}
							// LastCaughtUpTimestamp
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp); err != nil {
									return err
								}
							}
						}
					}
					// Observers
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].Observers) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].Observers))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].Observers {
							// ReplicaId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].Observers[i].ReplicaId); err != nil {
									return err
								}
							}
							// ReplicaDirectoryId
							if version >= 2 && version <= 999 {
								if err := protocol.WriteUUID(w, m.Topics[i].Partitions[i].Observers[i].ReplicaDirectoryId); err != nil {
									return err
								}
							}
							// LogEndOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].Observers[i].LogEndOffset); err != nil {
									return err
								}
							}
							// LastFetchTimestamp
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].Observers[i].LastFetchTimestamp); err != nil {
									return err
								}
							}
							// LastCaughtUpTimestamp
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].Observers[i].LastCaughtUpTimestamp); err != nil {
									return err
								}
							}
						}
					}
				}
			}
		}
	}
	// Nodes
	if version >= 2 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Nodes) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Nodes))); err != nil {
				return err
			}
		}
		for i := range m.Nodes {
			// NodeId
			if version >= 2 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Nodes[i].NodeId); err != nil {
					return err
				}
			}
			// Listeners
			if version >= 2 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Nodes[i].Listeners) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Nodes[i].Listeners))); err != nil {
						return err
					}
				}
				for i := range m.Nodes[i].Listeners {
					// Name
					if version >= 2 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Nodes[i].Listeners[i].Name); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Nodes[i].Listeners[i].Name); err != nil {
								return err
							}
						}
					}
					// Host
					if version >= 2 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Nodes[i].Listeners[i].Host); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Nodes[i].Listeners[i].Host); err != nil {
								return err
							}
						}
					}
					// Port
					if version >= 2 && version <= 999 {
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

// Read reads a DescribeQuorumResponse from an io.Reader for the given version.
func (m *DescribeQuorumResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ErrorCode
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// ErrorMessage
	if version >= 2 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ErrorMessage = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ErrorMessage = val
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
			m.Topics = make([]DescribeQuorumResponseTopicData, length)
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
						m.Topics[i].Partitions = make([]DescribeQuorumResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].HighWatermark = val
							}
							// CurrentVoters
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
									m.Topics[i].Partitions[i].CurrentVoters = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CurrentVoters = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp = val
										}
									}
								}
							}
							// Observers
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
									m.Topics[i].Partitions[i].Observers = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastCaughtUpTimestamp = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].Observers = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastCaughtUpTimestamp = val
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
						m.Topics[i].Partitions = make([]DescribeQuorumResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].HighWatermark = val
							}
							// CurrentVoters
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
									m.Topics[i].Partitions[i].CurrentVoters = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CurrentVoters = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp = val
										}
									}
								}
							}
							// Observers
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
									m.Topics[i].Partitions[i].Observers = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastCaughtUpTimestamp = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].Observers = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastCaughtUpTimestamp = val
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
			m.Topics = make([]DescribeQuorumResponseTopicData, length)
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
						m.Topics[i].Partitions = make([]DescribeQuorumResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].HighWatermark = val
							}
							// CurrentVoters
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
									m.Topics[i].Partitions[i].CurrentVoters = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CurrentVoters = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp = val
										}
									}
								}
							}
							// Observers
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
									m.Topics[i].Partitions[i].Observers = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastCaughtUpTimestamp = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].Observers = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastCaughtUpTimestamp = val
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
						m.Topics[i].Partitions = make([]DescribeQuorumResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].HighWatermark = val
							}
							// CurrentVoters
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
									m.Topics[i].Partitions[i].CurrentVoters = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].CurrentVoters = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].CurrentVoters[i].LastCaughtUpTimestamp = val
										}
									}
								}
							}
							// Observers
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
									m.Topics[i].Partitions[i].Observers = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastCaughtUpTimestamp = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].Observers = make([]ReplicaState, length)
									for i := int32(0); i < length; i++ {
										// ReplicaId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaId = val
										}
										// ReplicaDirectoryId
										if version >= 2 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].ReplicaDirectoryId = val
										}
										// LogEndOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LogEndOffset = val
										}
										// LastFetchTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastFetchTimestamp = val
										}
										// LastCaughtUpTimestamp
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Observers[i].LastCaughtUpTimestamp = val
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
	// Nodes
	if version >= 2 && version <= 999 {
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
			m.Nodes = make([]DescribeQuorumResponseNode, length)
			for i := int32(0); i < length; i++ {
				// NodeId
				if version >= 2 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Nodes[i].NodeId = val
				}
				// Listeners
				if version >= 2 && version <= 999 {
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
						m.Nodes[i].Listeners = make([]DescribeQuorumResponseListener, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Name = val
								}
							}
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Host = val
								}
							}
							// Port
							if version >= 2 && version <= 999 {
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Nodes[i].Listeners = make([]DescribeQuorumResponseListener, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Name = val
								}
							}
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Host = val
								}
							}
							// Port
							if version >= 2 && version <= 999 {
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
			m.Nodes = make([]DescribeQuorumResponseNode, length)
			for i := int32(0); i < length; i++ {
				// NodeId
				if version >= 2 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Nodes[i].NodeId = val
				}
				// Listeners
				if version >= 2 && version <= 999 {
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
						m.Nodes[i].Listeners = make([]DescribeQuorumResponseListener, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Name = val
								}
							}
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Host = val
								}
							}
							// Port
							if version >= 2 && version <= 999 {
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Nodes[i].Listeners = make([]DescribeQuorumResponseListener, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Name = val
								}
							}
							// Host
							if version >= 2 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Nodes[i].Listeners[i].Host = val
								}
							}
							// Port
							if version >= 2 && version <= 999 {
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

// DescribeQuorumResponseTopicData represents The response from the describe quorum API..
type DescribeQuorumResponseTopicData struct {
	// The topic name.
	TopicName string `json:"topicname" versions:"0-999"`
	// The partition data.
	Partitions []DescribeQuorumResponsePartitionData `json:"partitions" versions:"0-999"`
}

// DescribeQuorumResponsePartitionData represents The partition data..
type DescribeQuorumResponsePartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The partition error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"2-999"`
	// The ID of the current leader or -1 if the leader is unknown.
	LeaderId int32 `json:"leaderid" versions:"0-999"`
	// The latest known leader epoch.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// The high water mark.
	HighWatermark int64 `json:"highwatermark" versions:"0-999"`
	// The current voters of the partition.
	CurrentVoters []ReplicaState `json:"currentvoters" versions:"0-999"`
	// The observers of the partition.
	Observers []ReplicaState `json:"observers" versions:"0-999"`
}

// DescribeQuorumResponseNode represents The nodes in the quorum..
type DescribeQuorumResponseNode struct {
	// The ID of the associated node.
	NodeId int32 `json:"nodeid" versions:"2-999"`
	// The listeners of this controller.
	Listeners []DescribeQuorumResponseListener `json:"listeners" versions:"2-999"`
}

// DescribeQuorumResponseListener represents The listeners of this controller..
type DescribeQuorumResponseListener struct {
	// The name of the endpoint.
	Name string `json:"name" versions:"2-999"`
	// The hostname.
	Host string `json:"host" versions:"2-999"`
	// The port.
	Port uint16 `json:"port" versions:"2-999"`
}

// writeTaggedFields writes tagged fields for DescribeQuorumResponse.
func (m *DescribeQuorumResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeQuorumResponse.
func (m *DescribeQuorumResponse) readTaggedFields(r io.Reader, version int16) error {
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
