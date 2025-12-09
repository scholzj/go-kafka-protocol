package metadata

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	MetadataResponseApiKey        = 3
	MetadataResponseHeaderVersion = 1
)

// MetadataResponse represents a response message.
type MetadataResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"3-999"`
	// A list of brokers present in the cluster.
	Brokers []MetadataResponseMetadataResponseBroker `json:"brokers" versions:"0-999"`
	// The cluster ID that responding broker belongs to.
	ClusterId *string `json:"clusterid" versions:"2-999"`
	// The ID of the controller broker.
	ControllerId int32 `json:"controllerid" versions:"1-999"`
	// Each topic in the response.
	Topics []MetadataResponseMetadataResponseTopic `json:"topics" versions:"0-999"`
	// 32-bit bitfield to represent authorized operations for this cluster.
	ClusterAuthorizedOperations int32 `json:"clusterauthorizedoperations" versions:"8-10"`
	// The top-level error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"13-999"`
}

// Encode encodes a MetadataResponse to a byte slice for the given version.
func (m *MetadataResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a MetadataResponse from a byte slice for the given version.
func (m *MetadataResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a MetadataResponse to an io.Writer for the given version.
func (m *MetadataResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 3 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// Brokers
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Brokers) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Brokers))); err != nil {
				return err
			}
		}
		for i := range m.Brokers {
			// NodeId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Brokers[i].NodeId); err != nil {
					return err
				}
			}
			// Host
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Brokers[i].Host); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Brokers[i].Host); err != nil {
						return err
					}
				}
			}
			// Port
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Brokers[i].Port); err != nil {
					return err
				}
			}
			// Rack
			if version >= 1 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Brokers[i].Rack); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Brokers[i].Rack); err != nil {
						return err
					}
				}
			}
		}
	}
	// ClusterId
	if version >= 2 && version <= 999 {
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
	// ControllerId
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ControllerId); err != nil {
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
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Topics[i].ErrorCode); err != nil {
					return err
				}
			}
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Topics[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Topics[i].Name); err != nil {
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
			// IsInternal
			if version >= 1 && version <= 999 {
				if err := protocol.WriteBool(w, m.Topics[i].IsInternal); err != nil {
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
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(w, m.Topics[i].Partitions[i].ErrorCode); err != nil {
							return err
						}
					}
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
					if version >= 7 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].LeaderEpoch); err != nil {
							return err
						}
					}
					// ReplicaNodes
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].ReplicaNodes) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].ReplicaNodes))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].ReplicaNodes {
							if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].ReplicaNodes[i]); err != nil {
								return err
							}
							_ = i
						}
					}
					// IsrNodes
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].IsrNodes) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].IsrNodes))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].IsrNodes {
							if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].IsrNodes[i]); err != nil {
								return err
							}
							_ = i
						}
					}
					// OfflineReplicas
					if version >= 5 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].OfflineReplicas) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].OfflineReplicas))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].OfflineReplicas {
							if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].OfflineReplicas[i]); err != nil {
								return err
							}
							_ = i
						}
					}
				}
			}
			// TopicAuthorizedOperations
			if version >= 8 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Topics[i].TopicAuthorizedOperations); err != nil {
					return err
				}
			}
		}
	}
	// ClusterAuthorizedOperations
	if version >= 8 && version <= 10 {
		if err := protocol.WriteInt32(w, m.ClusterAuthorizedOperations); err != nil {
			return err
		}
	}
	// ErrorCode
	if version >= 13 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
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

// Read reads a MetadataResponse from an io.Reader for the given version.
func (m *MetadataResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// Brokers
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
			m.Brokers = make([]MetadataResponseMetadataResponseBroker, length)
			for i := int32(0); i < length; i++ {
				// NodeId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Brokers[i].NodeId = val
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Host = val
					}
				}
				// Port
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Brokers[i].Port = val
				}
				// Rack
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Rack = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Rack = val
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Brokers = make([]MetadataResponseMetadataResponseBroker, length)
			for i := int32(0); i < length; i++ {
				// NodeId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Brokers[i].NodeId = val
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Host = val
					}
				}
				// Port
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Brokers[i].Port = val
				}
				// Rack
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Rack = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Rack = val
					}
				}
			}
		}
	}
	// ClusterId
	if version >= 2 && version <= 999 {
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
	// ControllerId
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ControllerId = val
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
			m.Topics = make([]MetadataResponseMetadataResponseTopic, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Topics[i].ErrorCode = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
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
				// IsInternal
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					m.Topics[i].IsInternal = val
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
						m.Topics[i].Partitions = make([]MetadataResponseMetadataResponsePartition, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
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
							if version >= 7 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// ReplicaNodes
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
									m.Topics[i].Partitions[i].ReplicaNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].ReplicaNodes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ReplicaNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].ReplicaNodes[i] = val
									}
								}
							}
							// IsrNodes
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
									m.Topics[i].Partitions[i].IsrNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].IsrNodes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].IsrNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].IsrNodes[i] = val
									}
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
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
									m.Topics[i].Partitions[i].OfflineReplicas = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].OfflineReplicas[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].OfflineReplicas = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].OfflineReplicas[i] = val
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
						m.Topics[i].Partitions = make([]MetadataResponseMetadataResponsePartition, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
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
							if version >= 7 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// ReplicaNodes
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
									m.Topics[i].Partitions[i].ReplicaNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].ReplicaNodes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ReplicaNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].ReplicaNodes[i] = val
									}
								}
							}
							// IsrNodes
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
									m.Topics[i].Partitions[i].IsrNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].IsrNodes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].IsrNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].IsrNodes[i] = val
									}
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
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
									m.Topics[i].Partitions[i].OfflineReplicas = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].OfflineReplicas[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].OfflineReplicas = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].OfflineReplicas[i] = val
									}
								}
							}
						}
					}
				}
				// TopicAuthorizedOperations
				if version >= 8 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Topics[i].TopicAuthorizedOperations = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Topics = make([]MetadataResponseMetadataResponseTopic, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Topics[i].ErrorCode = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
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
				// IsInternal
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					m.Topics[i].IsInternal = val
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
						m.Topics[i].Partitions = make([]MetadataResponseMetadataResponsePartition, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
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
							if version >= 7 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// ReplicaNodes
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
									m.Topics[i].Partitions[i].ReplicaNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].ReplicaNodes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ReplicaNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].ReplicaNodes[i] = val
									}
								}
							}
							// IsrNodes
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
									m.Topics[i].Partitions[i].IsrNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].IsrNodes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].IsrNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].IsrNodes[i] = val
									}
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
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
									m.Topics[i].Partitions[i].OfflineReplicas = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].OfflineReplicas[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].OfflineReplicas = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].OfflineReplicas[i] = val
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
						m.Topics[i].Partitions = make([]MetadataResponseMetadataResponsePartition, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
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
							if version >= 7 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// ReplicaNodes
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
									m.Topics[i].Partitions[i].ReplicaNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].ReplicaNodes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ReplicaNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].ReplicaNodes[i] = val
									}
								}
							}
							// IsrNodes
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
									m.Topics[i].Partitions[i].IsrNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].IsrNodes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].IsrNodes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].IsrNodes[i] = val
									}
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
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
									m.Topics[i].Partitions[i].OfflineReplicas = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].OfflineReplicas[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].OfflineReplicas = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].OfflineReplicas[i] = val
									}
								}
							}
						}
					}
				}
				// TopicAuthorizedOperations
				if version >= 8 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Topics[i].TopicAuthorizedOperations = val
				}
			}
		}
	}
	// ClusterAuthorizedOperations
	if version >= 8 && version <= 10 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ClusterAuthorizedOperations = val
	}
	// ErrorCode
	if version >= 13 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// MetadataResponseMetadataResponseBroker represents A list of brokers present in the cluster..
type MetadataResponseMetadataResponseBroker struct {
	// The broker ID.
	NodeId int32 `json:"nodeid" versions:"0-999"`
	// The broker hostname.
	Host string `json:"host" versions:"0-999"`
	// The broker port.
	Port int32 `json:"port" versions:"0-999"`
	// The rack of the broker, or null if it has not been assigned to a rack.
	Rack *string `json:"rack" versions:"1-999"`
}

// MetadataResponseMetadataResponseTopic represents Each topic in the response..
type MetadataResponseMetadataResponseTopic struct {
	// The topic error, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
	Name *string `json:"name" versions:"0-999"`
	// The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
	TopicId uuid.UUID `json:"topicid" versions:"10-999"`
	// True if the topic is internal.
	IsInternal bool `json:"isinternal" versions:"1-999"`
	// Each partition in the topic.
	Partitions []MetadataResponseMetadataResponsePartition `json:"partitions" versions:"0-999"`
	// 32-bit bitfield to represent authorized operations for this topic.
	TopicAuthorizedOperations int32 `json:"topicauthorizedoperations" versions:"8-999"`
}

// MetadataResponseMetadataResponsePartition represents Each partition in the topic..
type MetadataResponseMetadataResponsePartition struct {
	// The partition error, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The ID of the leader broker.
	LeaderId int32 `json:"leaderid" versions:"0-999"`
	// The leader epoch of this partition.
	LeaderEpoch int32 `json:"leaderepoch" versions:"7-999"`
	// The set of all nodes that host this partition.
	ReplicaNodes []int32 `json:"replicanodes" versions:"0-999"`
	// The set of nodes that are in sync with the leader for this partition.
	IsrNodes []int32 `json:"isrnodes" versions:"0-999"`
	// The set of offline replicas of this partition.
	OfflineReplicas []int32 `json:"offlinereplicas" versions:"5-999"`
}

// writeTaggedFields writes tagged fields for MetadataResponse.
func (m *MetadataResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for MetadataResponse.
func (m *MetadataResponse) readTaggedFields(r io.Reader, version int16) error {
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
