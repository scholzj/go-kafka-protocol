package alterpartitionreassignments

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AlterPartitionReassignmentsRequestApiKey        = 45
	AlterPartitionReassignmentsRequestHeaderVersion = 1
)

// AlterPartitionReassignmentsRequest represents a request message.
type AlterPartitionReassignmentsRequest struct {
	// The time in ms to wait for the request to complete.
	TimeoutMs int32 `json:"timeoutms" versions:"0-999"`
	// The option indicating whether changing the replication factor of any given partition as part of this request is a valid move.
	AllowReplicationFactorChange bool `json:"allowreplicationfactorchange" versions:"1-999"`
	// The topics to reassign.
	Topics []AlterPartitionReassignmentsRequestReassignableTopic `json:"topics" versions:"0-999"`
}

// Encode encodes a AlterPartitionReassignmentsRequest to a byte slice for the given version.
func (m *AlterPartitionReassignmentsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AlterPartitionReassignmentsRequest from a byte slice for the given version.
func (m *AlterPartitionReassignmentsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AlterPartitionReassignmentsRequest to an io.Writer for the given version.
func (m *AlterPartitionReassignmentsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// TimeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TimeoutMs); err != nil {
			return err
		}
	}
	// AllowReplicationFactorChange
	if version >= 1 && version <= 999 {
		if err := protocol.WriteBool(w, m.AllowReplicationFactorChange); err != nil {
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
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].PartitionIndex); err != nil {
							return err
						}
					}
					// Replicas
					if version >= 0 && version <= 999 {
						if m.Topics[i].Partitions[i].Replicas == nil {
							if isFlexible {
								if err := protocol.WriteVaruint32(w, 0); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(w, -1); err != nil {
									return err
								}
							}
						} else {
							if isFlexible {
								length := uint32(len(m.Topics[i].Partitions[i].Replicas) + 1)
								if err := protocol.WriteVaruint32(w, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].Replicas))); err != nil {
									return err
								}
							}
							for i := range m.Topics[i].Partitions[i].Replicas {
								if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].Replicas[i]); err != nil {
									return err
								}
								_ = i
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

// Read reads a AlterPartitionReassignmentsRequest from an io.Reader for the given version.
func (m *AlterPartitionReassignmentsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// TimeoutMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.TimeoutMs = val
	}
	// AllowReplicationFactorChange
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.AllowReplicationFactorChange = val
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
			m.Topics = make([]AlterPartitionReassignmentsRequestReassignableTopic, length)
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
						m.Topics[i].Partitions = make([]AlterPartitionReassignmentsRequestReassignablePartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// Replicas
							if version >= 0 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint == 0 {
										m.Topics[i].Partitions[i].Replicas = nil
									} else {
										if lengthUint < 1 {
											return errors.New("invalid compact array length")
										}
										length = int32(lengthUint - 1)
										m.Topics[i].Partitions[i].Replicas = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Replicas[i] = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									if length == -1 {
										m.Topics[i].Partitions[i].Replicas = nil
									} else {
										m.Topics[i].Partitions[i].Replicas = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Replicas[i] = val
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
						m.Topics[i].Partitions = make([]AlterPartitionReassignmentsRequestReassignablePartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// Replicas
							if version >= 0 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint == 0 {
										m.Topics[i].Partitions[i].Replicas = nil
									} else {
										if lengthUint < 1 {
											return errors.New("invalid compact array length")
										}
										length = int32(lengthUint - 1)
										m.Topics[i].Partitions[i].Replicas = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Replicas[i] = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									if length == -1 {
										m.Topics[i].Partitions[i].Replicas = nil
									} else {
										m.Topics[i].Partitions[i].Replicas = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Replicas[i] = val
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
			m.Topics = make([]AlterPartitionReassignmentsRequestReassignableTopic, length)
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
						m.Topics[i].Partitions = make([]AlterPartitionReassignmentsRequestReassignablePartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// Replicas
							if version >= 0 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint == 0 {
										m.Topics[i].Partitions[i].Replicas = nil
									} else {
										if lengthUint < 1 {
											return errors.New("invalid compact array length")
										}
										length = int32(lengthUint - 1)
										m.Topics[i].Partitions[i].Replicas = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Replicas[i] = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									if length == -1 {
										m.Topics[i].Partitions[i].Replicas = nil
									} else {
										m.Topics[i].Partitions[i].Replicas = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Replicas[i] = val
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
						m.Topics[i].Partitions = make([]AlterPartitionReassignmentsRequestReassignablePartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// Replicas
							if version >= 0 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint == 0 {
										m.Topics[i].Partitions[i].Replicas = nil
									} else {
										if lengthUint < 1 {
											return errors.New("invalid compact array length")
										}
										length = int32(lengthUint - 1)
										m.Topics[i].Partitions[i].Replicas = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Replicas[i] = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									if length == -1 {
										m.Topics[i].Partitions[i].Replicas = nil
									} else {
										m.Topics[i].Partitions[i].Replicas = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].Replicas[i] = val
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

// AlterPartitionReassignmentsRequestReassignableTopic represents The topics to reassign..
type AlterPartitionReassignmentsRequestReassignableTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The partitions to reassign.
	Partitions []AlterPartitionReassignmentsRequestReassignablePartition `json:"partitions" versions:"0-999"`
}

// AlterPartitionReassignmentsRequestReassignablePartition represents The partitions to reassign..
type AlterPartitionReassignmentsRequestReassignablePartition struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The replicas to place the partitions on, or null to cancel a pending reassignment for this partition.
	Replicas []int32 `json:"replicas" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for AlterPartitionReassignmentsRequest.
func (m *AlterPartitionReassignmentsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterPartitionReassignmentsRequest.
func (m *AlterPartitionReassignmentsRequest) readTaggedFields(r io.Reader, version int16) error {
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
