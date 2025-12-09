package createpartitions

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	CreatePartitionsRequestApiKey        = 37
	CreatePartitionsRequestHeaderVersion = 1
)

// CreatePartitionsRequest represents a request message.
type CreatePartitionsRequest struct {
	// Each topic that we want to create new partitions inside.
	Topics []CreatePartitionsRequestCreatePartitionsTopic `json:"topics" versions:"0-999"`
	// The time in ms to wait for the partitions to be created.
	TimeoutMs int32 `json:"timeoutms" versions:"0-999"`
	// If true, then validate the request, but don't actually increase the number of partitions.
	ValidateOnly bool `json:"validateonly" versions:"0-999"`
}

// Encode encodes a CreatePartitionsRequest to a byte slice for the given version.
func (m *CreatePartitionsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a CreatePartitionsRequest from a byte slice for the given version.
func (m *CreatePartitionsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a CreatePartitionsRequest to an io.Writer for the given version.
func (m *CreatePartitionsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
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
			// Count
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Topics[i].Count); err != nil {
					return err
				}
			}
			// Assignments
			if version >= 0 && version <= 999 {
				if m.Topics[i].Assignments == nil {
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
						length := uint32(len(m.Topics[i].Assignments) + 1)
						if err := protocol.WriteVaruint32(w, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Assignments))); err != nil {
							return err
						}
					}
					for i := range m.Topics[i].Assignments {
						// BrokerIds
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(m.Topics[i].Assignments[i].BrokerIds) + 1)
								if err := protocol.WriteVaruint32(w, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Assignments[i].BrokerIds))); err != nil {
									return err
								}
							}
							for i := range m.Topics[i].Assignments[i].BrokerIds {
								if err := protocol.WriteInt32(w, m.Topics[i].Assignments[i].BrokerIds[i]); err != nil {
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
	// TimeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TimeoutMs); err != nil {
			return err
		}
	}
	// ValidateOnly
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.ValidateOnly); err != nil {
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

// Read reads a CreatePartitionsRequest from an io.Reader for the given version.
func (m *CreatePartitionsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
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
			m.Topics = make([]CreatePartitionsRequestCreatePartitionsTopic, length)
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
				// Count
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Topics[i].Count = val
				}
				// Assignments
				if version >= 0 && version <= 999 {
					var length int32
					if isFlexible {
						var lengthUint uint32
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint == 0 {
							m.Topics[i].Assignments = nil
						} else {
							if lengthUint < 1 {
								return errors.New("invalid compact array length")
							}
							length = int32(lengthUint - 1)
							m.Topics[i].Assignments = make([]CreatePartitionsRequestCreatePartitionsAssignment, length)
							for i := int32(0); i < length; i++ {
								// BrokerIds
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
										m.Topics[i].Assignments[i].BrokerIds = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Assignments[i].BrokerIds[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Assignments[i].BrokerIds = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Assignments[i].BrokerIds[i] = val
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
						if length == -1 {
							m.Topics[i].Assignments = nil
						} else {
							m.Topics[i].Assignments = make([]CreatePartitionsRequestCreatePartitionsAssignment, length)
							for i := int32(0); i < length; i++ {
								// BrokerIds
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
										m.Topics[i].Assignments[i].BrokerIds = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Assignments[i].BrokerIds[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Assignments[i].BrokerIds = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Assignments[i].BrokerIds[i] = val
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
			m.Topics = make([]CreatePartitionsRequestCreatePartitionsTopic, length)
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
				// Count
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Topics[i].Count = val
				}
				// Assignments
				if version >= 0 && version <= 999 {
					var length int32
					if isFlexible {
						var lengthUint uint32
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint == 0 {
							m.Topics[i].Assignments = nil
						} else {
							if lengthUint < 1 {
								return errors.New("invalid compact array length")
							}
							length = int32(lengthUint - 1)
							m.Topics[i].Assignments = make([]CreatePartitionsRequestCreatePartitionsAssignment, length)
							for i := int32(0); i < length; i++ {
								// BrokerIds
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
										m.Topics[i].Assignments[i].BrokerIds = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Assignments[i].BrokerIds[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Assignments[i].BrokerIds = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Assignments[i].BrokerIds[i] = val
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
						if length == -1 {
							m.Topics[i].Assignments = nil
						} else {
							m.Topics[i].Assignments = make([]CreatePartitionsRequestCreatePartitionsAssignment, length)
							for i := int32(0); i < length; i++ {
								// BrokerIds
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
										m.Topics[i].Assignments[i].BrokerIds = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Assignments[i].BrokerIds[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Assignments[i].BrokerIds = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Assignments[i].BrokerIds[i] = val
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
	// TimeoutMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.TimeoutMs = val
	}
	// ValidateOnly
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.ValidateOnly = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// CreatePartitionsRequestCreatePartitionsTopic represents Each topic that we want to create new partitions inside..
type CreatePartitionsRequestCreatePartitionsTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The new partition count.
	Count int32 `json:"count" versions:"0-999"`
	// The new partition assignments.
	Assignments []CreatePartitionsRequestCreatePartitionsAssignment `json:"assignments" versions:"0-999"`
}

// CreatePartitionsRequestCreatePartitionsAssignment represents The new partition assignments..
type CreatePartitionsRequestCreatePartitionsAssignment struct {
	// The assigned broker IDs.
	BrokerIds []int32 `json:"brokerids" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for CreatePartitionsRequest.
func (m *CreatePartitionsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreatePartitionsRequest.
func (m *CreatePartitionsRequest) readTaggedFields(r io.Reader, version int16) error {
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
