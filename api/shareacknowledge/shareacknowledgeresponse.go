package shareacknowledge

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ShareAcknowledgeResponseApiKey        = 79
	ShareAcknowledgeResponseHeaderVersion = 1
)

// ShareAcknowledgeResponse represents a response message.
type ShareAcknowledgeResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The top level response error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The top-level error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The time in milliseconds for which the acquired records are locked.
	AcquisitionLockTimeoutMs int32 `json:"acquisitionlocktimeoutms" versions:"2-999"`
	// The response topics.
	Responses []ShareAcknowledgeResponseShareAcknowledgeTopicResponse `json:"responses" versions:"0-999"`
	// Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
	NodeEndpoints []ShareAcknowledgeResponseNodeEndpoint `json:"nodeendpoints" versions:"0-999"`
}

// Encode encodes a ShareAcknowledgeResponse to a byte slice for the given version.
func (m *ShareAcknowledgeResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ShareAcknowledgeResponse from a byte slice for the given version.
func (m *ShareAcknowledgeResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ShareAcknowledgeResponse to an io.Writer for the given version.
func (m *ShareAcknowledgeResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 2 {
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
	// ErrorMessage
	if version >= 0 && version <= 999 {
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
	// AcquisitionLockTimeoutMs
	if version >= 2 && version <= 999 {
		if err := protocol.WriteInt32(w, m.AcquisitionLockTimeoutMs); err != nil {
			return err
		}
	}
	// Responses
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Responses) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Responses))); err != nil {
				return err
			}
		}
		for i := range m.Responses {
			// TopicId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteUUID(w, m.Responses[i].TopicId); err != nil {
					return err
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Responses[i].Partitions) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Responses[i].Partitions))); err != nil {
						return err
					}
				}
				for i := range m.Responses[i].Partitions {
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Responses[i].Partitions[i].PartitionIndex); err != nil {
							return err
						}
					}
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(w, m.Responses[i].Partitions[i].ErrorCode); err != nil {
							return err
						}
					}
					// ErrorMessage
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Responses[i].Partitions[i].ErrorMessage); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Responses[i].Partitions[i].ErrorMessage); err != nil {
								return err
							}
						}
					}
					// CurrentLeader
					if version >= 0 && version <= 999 {
						// LeaderId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(w, m.Responses[i].Partitions[i].CurrentLeader.LeaderId); err != nil {
								return err
							}
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(w, m.Responses[i].Partitions[i].CurrentLeader.LeaderEpoch); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}
	// NodeEndpoints
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.NodeEndpoints) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.NodeEndpoints))); err != nil {
				return err
			}
		}
		for i := range m.NodeEndpoints {
			// NodeId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.NodeEndpoints[i].NodeId); err != nil {
					return err
				}
			}
			// Host
			if version >= 0 && version <= 999 {
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
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.NodeEndpoints[i].Port); err != nil {
					return err
				}
			}
			// Rack
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.NodeEndpoints[i].Rack); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.NodeEndpoints[i].Rack); err != nil {
						return err
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

// Read reads a ShareAcknowledgeResponse from an io.Reader for the given version.
func (m *ShareAcknowledgeResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 2 {
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
	// ErrorMessage
	if version >= 0 && version <= 999 {
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
	// AcquisitionLockTimeoutMs
	if version >= 2 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.AcquisitionLockTimeoutMs = val
	}
	// Responses
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
			m.Responses = make([]ShareAcknowledgeResponseShareAcknowledgeTopicResponse, length)
			for i := int32(0); i < length; i++ {
				// TopicId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Responses[i].TopicId = val
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
						m.Responses[i].Partitions = make([]ShareAcknowledgeResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].CurrentLeader.LeaderId = val
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].CurrentLeader.LeaderEpoch = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Responses[i].Partitions = make([]ShareAcknowledgeResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].CurrentLeader.LeaderId = val
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].CurrentLeader.LeaderEpoch = val
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
			m.Responses = make([]ShareAcknowledgeResponseShareAcknowledgeTopicResponse, length)
			for i := int32(0); i < length; i++ {
				// TopicId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Responses[i].TopicId = val
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
						m.Responses[i].Partitions = make([]ShareAcknowledgeResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].CurrentLeader.LeaderId = val
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].CurrentLeader.LeaderEpoch = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Responses[i].Partitions = make([]ShareAcknowledgeResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].CurrentLeader.LeaderId = val
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].CurrentLeader.LeaderEpoch = val
								}
							}
						}
					}
				}
			}
		}
	}
	// NodeEndpoints
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
			m.NodeEndpoints = make([]ShareAcknowledgeResponseNodeEndpoint, length)
			for i := int32(0); i < length; i++ {
				// NodeId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.NodeEndpoints[i].NodeId = val
				}
				// Host
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.NodeEndpoints[i].Port = val
				}
				// Rack
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.NodeEndpoints[i].Rack = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.NodeEndpoints[i].Rack = val
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.NodeEndpoints = make([]ShareAcknowledgeResponseNodeEndpoint, length)
			for i := int32(0); i < length; i++ {
				// NodeId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.NodeEndpoints[i].NodeId = val
				}
				// Host
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.NodeEndpoints[i].Port = val
				}
				// Rack
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.NodeEndpoints[i].Rack = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.NodeEndpoints[i].Rack = val
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

// ShareAcknowledgeResponseShareAcknowledgeTopicResponse represents The response topics..
type ShareAcknowledgeResponseShareAcknowledgeTopicResponse struct {
	// The unique topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// The topic partitions.
	Partitions []ShareAcknowledgeResponsePartitionData `json:"partitions" versions:"0-999"`
}

// ShareAcknowledgeResponsePartitionData represents The topic partitions..
type ShareAcknowledgeResponsePartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The current leader of the partition.
	CurrentLeader ShareAcknowledgeResponseLeaderIdAndEpoch `json:"currentleader" versions:"0-999"`
}

// ShareAcknowledgeResponseLeaderIdAndEpoch represents The current leader of the partition..
type ShareAcknowledgeResponseLeaderIdAndEpoch struct {
	// The ID of the current leader or -1 if the leader is unknown.
	LeaderId int32 `json:"leaderid" versions:"0-999"`
	// The latest known leader epoch.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
}

// ShareAcknowledgeResponseNodeEndpoint represents Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER..
type ShareAcknowledgeResponseNodeEndpoint struct {
	// The ID of the associated node.
	NodeId int32 `json:"nodeid" versions:"0-999"`
	// The node's hostname.
	Host string `json:"host" versions:"0-999"`
	// The node's port.
	Port int32 `json:"port" versions:"0-999"`
	// The rack of the node, or null if it has not been assigned to a rack.
	Rack *string `json:"rack" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for ShareAcknowledgeResponse.
func (m *ShareAcknowledgeResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareAcknowledgeResponse.
func (m *ShareAcknowledgeResponse) readTaggedFields(r io.Reader, version int16) error {
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
