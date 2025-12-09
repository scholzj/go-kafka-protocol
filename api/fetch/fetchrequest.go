package fetch

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	FetchRequestApiKey        = 1
	FetchRequestHeaderVersion = 1
)

// FetchRequest represents a request message.
type FetchRequest struct {
	// The clusterId if known. This is used to validate metadata fetches prior to broker registration.
	ClusterId *string `json:"clusterid" versions:"12-999" tag:"0"`
	// The broker ID of the follower, of -1 if this request is from a consumer.
	ReplicaId int32 `json:"replicaid" versions:"0-14"`
	// The state of the replica in the follower.
	ReplicaState FetchRequestReplicaState `json:"replicastate" versions:"15-999" tag:"1"`
	// The maximum time in milliseconds to wait for the response.
	MaxWaitMs int32 `json:"maxwaitms" versions:"0-999"`
	// The minimum bytes to accumulate in the response.
	MinBytes int32 `json:"minbytes" versions:"0-999"`
	// The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
	MaxBytes int32 `json:"maxbytes" versions:"3-999"`
	// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
	IsolationLevel int8 `json:"isolationlevel" versions:"4-999"`
	// The fetch session ID.
	SessionId int32 `json:"sessionid" versions:"7-999"`
	// The fetch session epoch, which is used for ordering requests in a session.
	SessionEpoch int32 `json:"sessionepoch" versions:"7-999"`
	// The topics to fetch.
	Topics []FetchRequestFetchTopic `json:"topics" versions:"0-999"`
	// In an incremental fetch request, the partitions to remove.
	ForgottenTopicsData []FetchRequestForgottenTopic `json:"forgottentopicsdata" versions:"7-999"`
	// Rack ID of the consumer making this request.
	RackId string `json:"rackid" versions:"11-999"`
}

// Encode encodes a FetchRequest to a byte slice for the given version.
func (m *FetchRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a FetchRequest from a byte slice for the given version.
func (m *FetchRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a FetchRequest to an io.Writer for the given version.
func (m *FetchRequest) Write(w io.Writer, version int16) error {
	if version < 4 || version > 18 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 12 {
		isFlexible = true
	}

	// ClusterId
	if version >= 12 && version <= 999 {
	}
	// ReplicaId
	if version >= 0 && version <= 14 {
		if err := protocol.WriteInt32(w, m.ReplicaId); err != nil {
			return err
		}
	}
	// ReplicaState
	if version >= 15 && version <= 999 {
	}
	// MaxWaitMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.MaxWaitMs); err != nil {
			return err
		}
	}
	// MinBytes
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.MinBytes); err != nil {
			return err
		}
	}
	// MaxBytes
	if version >= 3 && version <= 999 {
		if err := protocol.WriteInt32(w, m.MaxBytes); err != nil {
			return err
		}
	}
	// IsolationLevel
	if version >= 4 && version <= 999 {
		if err := protocol.WriteInt8(w, m.IsolationLevel); err != nil {
			return err
		}
	}
	// SessionId
	if version >= 7 && version <= 999 {
		if err := protocol.WriteInt32(w, m.SessionId); err != nil {
			return err
		}
	}
	// SessionEpoch
	if version >= 7 && version <= 999 {
		if err := protocol.WriteInt32(w, m.SessionEpoch); err != nil {
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
			if version >= 0 && version <= 12 {
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
			// TopicId
			if version >= 13 && version <= 999 {
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
					// CurrentLeaderEpoch
					if version >= 9 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].CurrentLeaderEpoch); err != nil {
							return err
						}
					}
					// FetchOffset
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].FetchOffset); err != nil {
							return err
						}
					}
					// LastFetchedEpoch
					if version >= 12 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].LastFetchedEpoch); err != nil {
							return err
						}
					}
					// LogStartOffset
					if version >= 5 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].LogStartOffset); err != nil {
							return err
						}
					}
					// PartitionMaxBytes
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].PartitionMaxBytes); err != nil {
							return err
						}
					}
					// ReplicaDirectoryId
					if version >= 17 && version <= 999 {
					}
					// HighWatermark
					if version >= 18 && version <= 999 {
					}
				}
			}
		}
	}
	// ForgottenTopicsData
	if version >= 7 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.ForgottenTopicsData) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.ForgottenTopicsData))); err != nil {
				return err
			}
		}
		for i := range m.ForgottenTopicsData {
			// Topic
			if version >= 7 && version <= 12 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.ForgottenTopicsData[i].Topic); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.ForgottenTopicsData[i].Topic); err != nil {
						return err
					}
				}
			}
			// TopicId
			if version >= 13 && version <= 999 {
				if err := protocol.WriteUUID(w, m.ForgottenTopicsData[i].TopicId); err != nil {
					return err
				}
			}
			// Partitions
			if version >= 7 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.ForgottenTopicsData[i].Partitions) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.ForgottenTopicsData[i].Partitions))); err != nil {
						return err
					}
				}
				for i := range m.ForgottenTopicsData[i].Partitions {
					if err := protocol.WriteInt32(w, m.ForgottenTopicsData[i].Partitions[i]); err != nil {
						return err
					}
					_ = i
				}
			}
		}
	}
	// RackId
	if version >= 11 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.RackId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.RackId); err != nil {
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

// Read reads a FetchRequest from an io.Reader for the given version.
func (m *FetchRequest) Read(r io.Reader, version int16) error {
	if version < 4 || version > 18 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 12 {
		isFlexible = true
	}

	// ClusterId
	if version >= 12 && version <= 999 {
	}
	// ReplicaId
	if version >= 0 && version <= 14 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ReplicaId = val
	}
	// ReplicaState
	if version >= 15 && version <= 999 {
	}
	// MaxWaitMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.MaxWaitMs = val
	}
	// MinBytes
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.MinBytes = val
	}
	// MaxBytes
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.MaxBytes = val
	}
	// IsolationLevel
	if version >= 4 && version <= 999 {
		val, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		m.IsolationLevel = val
	}
	// SessionId
	if version >= 7 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.SessionId = val
	}
	// SessionEpoch
	if version >= 7 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.SessionEpoch = val
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
			m.Topics = make([]FetchRequestFetchTopic, length)
			for i := int32(0); i < length; i++ {
				// Topic
				if version >= 0 && version <= 12 {
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
				// TopicId
				if version >= 13 && version <= 999 {
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
						m.Topics[i].Partitions = make([]FetchRequestFetchPartition, length)
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
							if version >= 9 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CurrentLeaderEpoch = val
							}
							// FetchOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].FetchOffset = val
							}
							// LastFetchedEpoch
							if version >= 12 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastFetchedEpoch = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LogStartOffset = val
							}
							// PartitionMaxBytes
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionMaxBytes = val
							}
							// ReplicaDirectoryId
							if version >= 17 && version <= 999 {
							}
							// HighWatermark
							if version >= 18 && version <= 999 {
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]FetchRequestFetchPartition, length)
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
							if version >= 9 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CurrentLeaderEpoch = val
							}
							// FetchOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].FetchOffset = val
							}
							// LastFetchedEpoch
							if version >= 12 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastFetchedEpoch = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LogStartOffset = val
							}
							// PartitionMaxBytes
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionMaxBytes = val
							}
							// ReplicaDirectoryId
							if version >= 17 && version <= 999 {
							}
							// HighWatermark
							if version >= 18 && version <= 999 {
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
			m.Topics = make([]FetchRequestFetchTopic, length)
			for i := int32(0); i < length; i++ {
				// Topic
				if version >= 0 && version <= 12 {
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
				// TopicId
				if version >= 13 && version <= 999 {
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
						m.Topics[i].Partitions = make([]FetchRequestFetchPartition, length)
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
							if version >= 9 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CurrentLeaderEpoch = val
							}
							// FetchOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].FetchOffset = val
							}
							// LastFetchedEpoch
							if version >= 12 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastFetchedEpoch = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LogStartOffset = val
							}
							// PartitionMaxBytes
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionMaxBytes = val
							}
							// ReplicaDirectoryId
							if version >= 17 && version <= 999 {
							}
							// HighWatermark
							if version >= 18 && version <= 999 {
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]FetchRequestFetchPartition, length)
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
							if version >= 9 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].CurrentLeaderEpoch = val
							}
							// FetchOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].FetchOffset = val
							}
							// LastFetchedEpoch
							if version >= 12 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LastFetchedEpoch = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LogStartOffset = val
							}
							// PartitionMaxBytes
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionMaxBytes = val
							}
							// ReplicaDirectoryId
							if version >= 17 && version <= 999 {
							}
							// HighWatermark
							if version >= 18 && version <= 999 {
							}
						}
					}
				}
			}
		}
	}
	// ForgottenTopicsData
	if version >= 7 && version <= 999 {
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
			m.ForgottenTopicsData = make([]FetchRequestForgottenTopic, length)
			for i := int32(0); i < length; i++ {
				// Topic
				if version >= 7 && version <= 12 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.ForgottenTopicsData[i].Topic = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.ForgottenTopicsData[i].Topic = val
					}
				}
				// TopicId
				if version >= 13 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.ForgottenTopicsData[i].TopicId = val
				}
				// Partitions
				if version >= 7 && version <= 999 {
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
						m.ForgottenTopicsData[i].Partitions = make([]int32, length)
						for i := int32(0); i < length; i++ {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.ForgottenTopicsData[i].Partitions[i] = val
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.ForgottenTopicsData[i].Partitions = make([]int32, length)
						for i := int32(0); i < length; i++ {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.ForgottenTopicsData[i].Partitions[i] = val
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
			m.ForgottenTopicsData = make([]FetchRequestForgottenTopic, length)
			for i := int32(0); i < length; i++ {
				// Topic
				if version >= 7 && version <= 12 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.ForgottenTopicsData[i].Topic = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.ForgottenTopicsData[i].Topic = val
					}
				}
				// TopicId
				if version >= 13 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.ForgottenTopicsData[i].TopicId = val
				}
				// Partitions
				if version >= 7 && version <= 999 {
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
						m.ForgottenTopicsData[i].Partitions = make([]int32, length)
						for i := int32(0); i < length; i++ {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.ForgottenTopicsData[i].Partitions[i] = val
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.ForgottenTopicsData[i].Partitions = make([]int32, length)
						for i := int32(0); i < length; i++ {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.ForgottenTopicsData[i].Partitions[i] = val
						}
					}
				}
			}
		}
	}
	// RackId
	if version >= 11 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.RackId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.RackId = val
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

// FetchRequestReplicaState represents The state of the replica in the follower..
type FetchRequestReplicaState struct {
	// The replica ID of the follower, or -1 if this request is from a consumer.
	ReplicaId int32 `json:"replicaid" versions:"15-999"`
	// The epoch of this follower, or -1 if not available.
	ReplicaEpoch int64 `json:"replicaepoch" versions:"15-999"`
}

// FetchRequestFetchTopic represents The topics to fetch..
type FetchRequestFetchTopic struct {
	// The name of the topic to fetch.
	Topic string `json:"topic" versions:"0-12"`
	// The unique topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"13-999"`
	// The partitions to fetch.
	Partitions []FetchRequestFetchPartition `json:"partitions" versions:"0-999"`
}

// FetchRequestFetchPartition represents The partitions to fetch..
type FetchRequestFetchPartition struct {
	// The partition index.
	Partition int32 `json:"partition" versions:"0-999"`
	// The current leader epoch of the partition.
	CurrentLeaderEpoch int32 `json:"currentleaderepoch" versions:"9-999"`
	// The message offset.
	FetchOffset int64 `json:"fetchoffset" versions:"0-999"`
	// The epoch of the last fetched record or -1 if there is none.
	LastFetchedEpoch int32 `json:"lastfetchedepoch" versions:"12-999"`
	// The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
	LogStartOffset int64 `json:"logstartoffset" versions:"5-999"`
	// The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
	PartitionMaxBytes int32 `json:"partitionmaxbytes" versions:"0-999"`
	// The directory id of the follower fetching.
	ReplicaDirectoryId uuid.UUID `json:"replicadirectoryid" versions:"17-999" tag:"0"`
	// The high-watermark known by the replica. -1 if the high-watermark is not known and 9223372036854775807 if the feature is not supported.
	HighWatermark int64 `json:"highwatermark" versions:"18-999" tag:"1"`
}

// FetchRequestForgottenTopic represents In an incremental fetch request, the partitions to remove..
type FetchRequestForgottenTopic struct {
	// The topic name.
	Topic string `json:"topic" versions:"7-12"`
	// The unique topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"13-999"`
	// The partitions indexes to forget.
	Partitions []int32 `json:"partitions" versions:"7-999"`
}

// writeTaggedFields writes tagged fields for FetchRequest.
func (m *FetchRequest) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	isFlexible := version >= 12

	// ClusterId (tag 0)
	if version >= 12 {
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

	// ReplicaState (tag 1)
	if version >= 15 {
		if true {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(1)); err != nil {
				return err
			}
			// ReplicaId
			if version >= 15 && version <= 999 {
				if err := protocol.WriteInt32(w, m.ReplicaState.ReplicaId); err != nil {
					return err
				}
			}
			// ReplicaEpoch
			if version >= 15 && version <= 999 {
				if err := protocol.WriteInt64(w, m.ReplicaState.ReplicaEpoch); err != nil {
					return err
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

// readTaggedFields reads tagged fields for FetchRequest.
func (m *FetchRequest) readTaggedFields(r io.Reader, version int16) error {
	isFlexible := version >= 12

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
			if version >= 12 {
				val, err := protocol.ReadCompactNullableString(r)
				if err != nil {
					return err
				}
				m.ClusterId = val
			}
		case 1: // ReplicaState
			if version >= 15 {
				// ReplicaId
				if version >= 15 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.ReplicaState.ReplicaId = val
				}
				// ReplicaEpoch
				if version >= 15 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.ReplicaState.ReplicaEpoch = val
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
