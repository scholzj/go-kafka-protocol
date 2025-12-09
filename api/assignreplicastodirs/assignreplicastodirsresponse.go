package assignreplicastodirs

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AssignReplicasToDirsResponseApiKey        = 73
	AssignReplicasToDirsResponseHeaderVersion = 1
)

// AssignReplicasToDirsResponse represents a response message.
type AssignReplicasToDirsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The top level response error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The list of directories and their assigned partitions.
	Directories []AssignReplicasToDirsResponseDirectoryData `json:"directories" versions:"0-999"`
}

// Encode encodes a AssignReplicasToDirsResponse to a byte slice for the given version.
func (m *AssignReplicasToDirsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AssignReplicasToDirsResponse from a byte slice for the given version.
func (m *AssignReplicasToDirsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AssignReplicasToDirsResponse to an io.Writer for the given version.
func (m *AssignReplicasToDirsResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
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
	// Directories
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Directories) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Directories))); err != nil {
				return err
			}
		}
		for i := range m.Directories {
			// Id
			if version >= 0 && version <= 999 {
				if err := protocol.WriteUUID(w, m.Directories[i].Id); err != nil {
					return err
				}
			}
			// Topics
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Directories[i].Topics) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Directories[i].Topics))); err != nil {
						return err
					}
				}
				for i := range m.Directories[i].Topics {
					// TopicId
					if version >= 0 && version <= 999 {
						if err := protocol.WriteUUID(w, m.Directories[i].Topics[i].TopicId); err != nil {
							return err
						}
					}
					// Partitions
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Directories[i].Topics[i].Partitions) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Directories[i].Topics[i].Partitions))); err != nil {
								return err
							}
						}
						for i := range m.Directories[i].Topics[i].Partitions {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Directories[i].Topics[i].Partitions[i].PartitionIndex); err != nil {
									return err
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(w, m.Directories[i].Topics[i].Partitions[i].ErrorCode); err != nil {
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

// Read reads a AssignReplicasToDirsResponse from an io.Reader for the given version.
func (m *AssignReplicasToDirsResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
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
	// Directories
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
			m.Directories = make([]AssignReplicasToDirsResponseDirectoryData, length)
			for i := int32(0); i < length; i++ {
				// Id
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Directories[i].Id = val
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
						m.Directories[i].Topics = make([]AssignReplicasToDirsResponseTopicData, length)
						for i := int32(0); i < length; i++ {
							// TopicId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Directories[i].Topics[i].TopicId = val
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
									m.Directories[i].Topics[i].Partitions = make([]AssignReplicasToDirsResponsePartitionData, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].ErrorCode = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Directories[i].Topics[i].Partitions = make([]AssignReplicasToDirsResponsePartitionData, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].ErrorCode = val
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
						m.Directories[i].Topics = make([]AssignReplicasToDirsResponseTopicData, length)
						for i := int32(0); i < length; i++ {
							// TopicId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Directories[i].Topics[i].TopicId = val
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
									m.Directories[i].Topics[i].Partitions = make([]AssignReplicasToDirsResponsePartitionData, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].ErrorCode = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Directories[i].Topics[i].Partitions = make([]AssignReplicasToDirsResponsePartitionData, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].ErrorCode = val
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
			m.Directories = make([]AssignReplicasToDirsResponseDirectoryData, length)
			for i := int32(0); i < length; i++ {
				// Id
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Directories[i].Id = val
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
						m.Directories[i].Topics = make([]AssignReplicasToDirsResponseTopicData, length)
						for i := int32(0); i < length; i++ {
							// TopicId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Directories[i].Topics[i].TopicId = val
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
									m.Directories[i].Topics[i].Partitions = make([]AssignReplicasToDirsResponsePartitionData, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].ErrorCode = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Directories[i].Topics[i].Partitions = make([]AssignReplicasToDirsResponsePartitionData, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].ErrorCode = val
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
						m.Directories[i].Topics = make([]AssignReplicasToDirsResponseTopicData, length)
						for i := int32(0); i < length; i++ {
							// TopicId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								m.Directories[i].Topics[i].TopicId = val
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
									m.Directories[i].Topics[i].Partitions = make([]AssignReplicasToDirsResponsePartitionData, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].ErrorCode = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Directories[i].Topics[i].Partitions = make([]AssignReplicasToDirsResponsePartitionData, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Directories[i].Topics[i].Partitions[i].ErrorCode = val
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

// AssignReplicasToDirsResponseDirectoryData represents The list of directories and their assigned partitions..
type AssignReplicasToDirsResponseDirectoryData struct {
	// The ID of the directory.
	Id uuid.UUID `json:"id" versions:"0-999"`
	// The list of topics and their assigned partitions.
	Topics []AssignReplicasToDirsResponseTopicData `json:"topics" versions:"0-999"`
}

// AssignReplicasToDirsResponseTopicData represents The list of topics and their assigned partitions..
type AssignReplicasToDirsResponseTopicData struct {
	// The ID of the assigned topic.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// The list of assigned partitions.
	Partitions []AssignReplicasToDirsResponsePartitionData `json:"partitions" versions:"0-999"`
}

// AssignReplicasToDirsResponsePartitionData represents The list of assigned partitions..
type AssignReplicasToDirsResponsePartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The partition level error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for AssignReplicasToDirsResponse.
func (m *AssignReplicasToDirsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AssignReplicasToDirsResponse.
func (m *AssignReplicasToDirsResponse) readTaggedFields(r io.Reader, version int16) error {
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
