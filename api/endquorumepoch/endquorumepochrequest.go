package endquorumepoch

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	EndQuorumEpochRequestApiKey        = 54
	EndQuorumEpochRequestHeaderVersion = 1
)

// EndQuorumEpochRequest represents a request message.
type EndQuorumEpochRequest struct {
	// The cluster id.
	ClusterId *string `json:"clusterid" versions:"0-999"`
	// The topics.
	Topics []EndQuorumEpochRequestTopicData `json:"topics" versions:"0-999"`
	// Endpoints for the leader.
	LeaderEndpoints []EndQuorumEpochRequestLeaderEndpoint `json:"leaderendpoints" versions:"1-999"`
}

// Encode encodes a EndQuorumEpochRequest to a byte slice for the given version.
func (m *EndQuorumEpochRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a EndQuorumEpochRequest from a byte slice for the given version.
func (m *EndQuorumEpochRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a EndQuorumEpochRequest to an io.Writer for the given version.
func (m *EndQuorumEpochRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
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
					// PreferredSuccessors
					if version >= 0 && version <= 0 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].PreferredSuccessors) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].PreferredSuccessors))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].PreferredSuccessors {
							if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].PreferredSuccessors[i]); err != nil {
								return err
							}
							_ = i
						}
					}
					// PreferredCandidates
					if version >= 1 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].PreferredCandidates) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].PreferredCandidates))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].PreferredCandidates {
							// CandidateId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateId); err != nil {
									return err
								}
							}
							// CandidateDirectoryId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteUUID(w, m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateDirectoryId); err != nil {
									return err
								}
							}
						}
					}
				}
			}
		}
	}
	// LeaderEndpoints
	if version >= 1 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.LeaderEndpoints) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.LeaderEndpoints))); err != nil {
				return err
			}
		}
		for i := range m.LeaderEndpoints {
			// Name
			if version >= 1 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.LeaderEndpoints[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.LeaderEndpoints[i].Name); err != nil {
						return err
					}
				}
			}
			// Host
			if version >= 1 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.LeaderEndpoints[i].Host); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.LeaderEndpoints[i].Host); err != nil {
						return err
					}
				}
			}
			// Port
			if version >= 1 && version <= 999 {
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

// Read reads a EndQuorumEpochRequest from an io.Reader for the given version.
func (m *EndQuorumEpochRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
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
			m.Topics = make([]EndQuorumEpochRequestTopicData, length)
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
						m.Topics[i].Partitions = make([]EndQuorumEpochRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
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
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
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
									m.Topics[i].Partitions[i].PreferredSuccessors = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].PreferredSuccessors[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].PreferredSuccessors = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].PreferredSuccessors[i] = val
									}
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
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
									m.Topics[i].Partitions[i].PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, length)
									for i := int32(0); i < length; i++ {
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateDirectoryId = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, length)
									for i := int32(0); i < length; i++ {
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateDirectoryId = val
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
						m.Topics[i].Partitions = make([]EndQuorumEpochRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
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
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
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
									m.Topics[i].Partitions[i].PreferredSuccessors = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].PreferredSuccessors[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].PreferredSuccessors = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].PreferredSuccessors[i] = val
									}
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
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
									m.Topics[i].Partitions[i].PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, length)
									for i := int32(0); i < length; i++ {
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateDirectoryId = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, length)
									for i := int32(0); i < length; i++ {
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateDirectoryId = val
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
			m.Topics = make([]EndQuorumEpochRequestTopicData, length)
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
						m.Topics[i].Partitions = make([]EndQuorumEpochRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
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
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
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
									m.Topics[i].Partitions[i].PreferredSuccessors = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].PreferredSuccessors[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].PreferredSuccessors = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].PreferredSuccessors[i] = val
									}
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
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
									m.Topics[i].Partitions[i].PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, length)
									for i := int32(0); i < length; i++ {
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateDirectoryId = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, length)
									for i := int32(0); i < length; i++ {
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateDirectoryId = val
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
						m.Topics[i].Partitions = make([]EndQuorumEpochRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
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
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
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
									m.Topics[i].Partitions[i].PreferredSuccessors = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].PreferredSuccessors[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].PreferredSuccessors = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].PreferredSuccessors[i] = val
									}
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
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
									m.Topics[i].Partitions[i].PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, length)
									for i := int32(0); i < length; i++ {
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateDirectoryId = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, length)
									for i := int32(0); i < length; i++ {
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].PreferredCandidates[i].CandidateDirectoryId = val
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
	// LeaderEndpoints
	if version >= 1 && version <= 999 {
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
			m.LeaderEndpoints = make([]EndQuorumEpochRequestLeaderEndpoint, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.LeaderEndpoints[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.LeaderEndpoints[i].Name = val
					}
				}
				// Host
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.LeaderEndpoints[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.LeaderEndpoints[i].Host = val
					}
				}
				// Port
				if version >= 1 && version <= 999 {
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.LeaderEndpoints = make([]EndQuorumEpochRequestLeaderEndpoint, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.LeaderEndpoints[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.LeaderEndpoints[i].Name = val
					}
				}
				// Host
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.LeaderEndpoints[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.LeaderEndpoints[i].Host = val
					}
				}
				// Port
				if version >= 1 && version <= 999 {
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

// EndQuorumEpochRequestTopicData represents The topics..
type EndQuorumEpochRequestTopicData struct {
	// The topic name.
	TopicName string `json:"topicname" versions:"0-999"`
	// The partitions.
	Partitions []EndQuorumEpochRequestPartitionData `json:"partitions" versions:"0-999"`
}

// EndQuorumEpochRequestPartitionData represents The partitions..
type EndQuorumEpochRequestPartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The current leader ID that is resigning.
	LeaderId int32 `json:"leaderid" versions:"0-999"`
	// The current epoch.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// A sorted list of preferred successors to start the election.
	PreferredSuccessors []int32 `json:"preferredsuccessors" versions:"0"`
	// A sorted list of preferred candidates to start the election.
	PreferredCandidates []EndQuorumEpochRequestReplicaInfo `json:"preferredcandidates" versions:"1-999"`
}

// EndQuorumEpochRequestReplicaInfo represents A sorted list of preferred candidates to start the election..
type EndQuorumEpochRequestReplicaInfo struct {
	// The ID of the candidate replica.
	CandidateId int32 `json:"candidateid" versions:"1-999"`
	// The directory ID of the candidate replica.
	CandidateDirectoryId uuid.UUID `json:"candidatedirectoryid" versions:"1-999"`
}

// EndQuorumEpochRequestLeaderEndpoint represents Endpoints for the leader..
type EndQuorumEpochRequestLeaderEndpoint struct {
	// The name of the endpoint.
	Name string `json:"name" versions:"1-999"`
	// The node's hostname.
	Host string `json:"host" versions:"1-999"`
	// The node's port.
	Port uint16 `json:"port" versions:"1-999"`
}

// writeTaggedFields writes tagged fields for EndQuorumEpochRequest.
func (m *EndQuorumEpochRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for EndQuorumEpochRequest.
func (m *EndQuorumEpochRequest) readTaggedFields(r io.Reader, version int16) error {
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
