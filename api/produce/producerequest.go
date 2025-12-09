package produce

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ProduceRequestApiKey        = 0
	ProduceRequestHeaderVersion = 1
)

// ProduceRequest represents a request message.
type ProduceRequest struct {
	// The transactional ID, or null if the producer is not transactional.
	TransactionalId *string `json:"transactionalid" versions:"3-999"`
	// The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
	Acks int16 `json:"acks" versions:"0-999"`
	// The timeout to await a response in milliseconds.
	TimeoutMs int32 `json:"timeoutms" versions:"0-999"`
	// Each topic to produce to.
	TopicData []ProduceRequestTopicProduceData `json:"topicdata" versions:"0-999"`
}

// Encode encodes a ProduceRequest to a byte slice for the given version.
func (m *ProduceRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ProduceRequest from a byte slice for the given version.
func (m *ProduceRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ProduceRequest to an io.Writer for the given version.
func (m *ProduceRequest) Write(w io.Writer, version int16) error {
	if version < 3 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// TransactionalId
	if version >= 3 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.TransactionalId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.TransactionalId); err != nil {
				return err
			}
		}
	}
	// Acks
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.Acks); err != nil {
			return err
		}
	}
	// TimeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TimeoutMs); err != nil {
			return err
		}
	}
	// TopicData
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.TopicData) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.TopicData))); err != nil {
				return err
			}
		}
		for i := range m.TopicData {
			// Name
			if version >= 0 && version <= 12 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.TopicData[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.TopicData[i].Name); err != nil {
						return err
					}
				}
			}
			// TopicId
			if version >= 13 && version <= 999 {
				if err := protocol.WriteUUID(w, m.TopicData[i].TopicId); err != nil {
					return err
				}
			}
			// PartitionData
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.TopicData[i].PartitionData) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.TopicData[i].PartitionData))); err != nil {
						return err
					}
				}
				for i := range m.TopicData[i].PartitionData {
					// Index
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.TopicData[i].PartitionData[i].Index); err != nil {
							return err
						}
					}
					// Records
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableBytes(w, m.TopicData[i].PartitionData[i].Records); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableBytes(w, m.TopicData[i].PartitionData[i].Records); err != nil {
								return err
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

// Read reads a ProduceRequest from an io.Reader for the given version.
func (m *ProduceRequest) Read(r io.Reader, version int16) error {
	if version < 3 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// TransactionalId
	if version >= 3 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.TransactionalId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.TransactionalId = val
		}
	}
	// Acks
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.Acks = val
	}
	// TimeoutMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.TimeoutMs = val
	}
	// TopicData
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
			m.TopicData = make([]ProduceRequestTopicProduceData, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 12 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.TopicData[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.TopicData[i].Name = val
					}
				}
				// TopicId
				if version >= 13 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.TopicData[i].TopicId = val
				}
				// PartitionData
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
						m.TopicData[i].PartitionData = make([]ProduceRequestPartitionProduceData, length)
						for i := int32(0); i < length; i++ {
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.TopicData[i].PartitionData[i].Index = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.TopicData[i].PartitionData[i].Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.TopicData[i].PartitionData[i].Records = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.TopicData[i].PartitionData = make([]ProduceRequestPartitionProduceData, length)
						for i := int32(0); i < length; i++ {
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.TopicData[i].PartitionData[i].Index = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.TopicData[i].PartitionData[i].Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.TopicData[i].PartitionData[i].Records = val
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
			m.TopicData = make([]ProduceRequestTopicProduceData, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 12 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.TopicData[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.TopicData[i].Name = val
					}
				}
				// TopicId
				if version >= 13 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.TopicData[i].TopicId = val
				}
				// PartitionData
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
						m.TopicData[i].PartitionData = make([]ProduceRequestPartitionProduceData, length)
						for i := int32(0); i < length; i++ {
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.TopicData[i].PartitionData[i].Index = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.TopicData[i].PartitionData[i].Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.TopicData[i].PartitionData[i].Records = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.TopicData[i].PartitionData = make([]ProduceRequestPartitionProduceData, length)
						for i := int32(0); i < length; i++ {
							// Index
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.TopicData[i].PartitionData[i].Index = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.TopicData[i].PartitionData[i].Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.TopicData[i].PartitionData[i].Records = val
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

// ProduceRequestTopicProduceData represents Each topic to produce to..
type ProduceRequestTopicProduceData struct {
	// The topic name.
	Name string `json:"name" versions:"0-12"`
	// The unique topic ID
	TopicId uuid.UUID `json:"topicid" versions:"13-999"`
	// Each partition to produce to.
	PartitionData []ProduceRequestPartitionProduceData `json:"partitiondata" versions:"0-999"`
}

// ProduceRequestPartitionProduceData represents Each partition to produce to..
type ProduceRequestPartitionProduceData struct {
	// The partition index.
	Index int32 `json:"index" versions:"0-999"`
	// The record data to be produced.
	Records *[]byte `json:"records" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for ProduceRequest.
func (m *ProduceRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ProduceRequest.
func (m *ProduceRequest) readTaggedFields(r io.Reader, version int16) error {
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
