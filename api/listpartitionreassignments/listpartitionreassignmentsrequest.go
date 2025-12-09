package listpartitionreassignments

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ListPartitionReassignmentsRequestApiKey        = 46
	ListPartitionReassignmentsRequestHeaderVersion = 1
)

// ListPartitionReassignmentsRequest represents a request message.
type ListPartitionReassignmentsRequest struct {
	// The time in ms to wait for the request to complete.
	TimeoutMs int32 `json:"timeoutms" versions:"0-999"`
	// The topics to list partition reassignments for, or null to list everything.
	Topics []ListPartitionReassignmentsRequestListPartitionReassignmentsTopics `json:"topics" versions:"0-999"`
}

// Encode encodes a ListPartitionReassignmentsRequest to a byte slice for the given version.
func (m *ListPartitionReassignmentsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ListPartitionReassignmentsRequest from a byte slice for the given version.
func (m *ListPartitionReassignmentsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ListPartitionReassignmentsRequest to an io.Writer for the given version.
func (m *ListPartitionReassignmentsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
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
	// Topics
	if version >= 0 && version <= 999 {
		if m.Topics == nil {
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
				// PartitionIndexes
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(m.Topics[i].PartitionIndexes) + 1)
						if err := protocol.WriteVaruint32(w, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(w, int32(len(m.Topics[i].PartitionIndexes))); err != nil {
							return err
						}
					}
					for i := range m.Topics[i].PartitionIndexes {
						if err := protocol.WriteInt32(w, m.Topics[i].PartitionIndexes[i]); err != nil {
							return err
						}
						_ = i
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

// Read reads a ListPartitionReassignmentsRequest from an io.Reader for the given version.
func (m *ListPartitionReassignmentsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
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
	// Topics
	if version >= 0 && version <= 999 {
		var length int32
		if isFlexible {
			var lengthUint uint32
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint == 0 {
				m.Topics = nil
			} else {
				if lengthUint < 1 {
					return errors.New("invalid compact array length")
				}
				length = int32(lengthUint - 1)
				m.Topics = make([]ListPartitionReassignmentsRequestListPartitionReassignmentsTopics, length)
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
					// PartitionIndexes
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
							m.Topics[i].PartitionIndexes = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].PartitionIndexes[i] = val
							}
						} else {
							var err error
							length, err = protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.Topics[i].PartitionIndexes = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].PartitionIndexes[i] = val
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
				m.Topics = nil
			} else {
				m.Topics = make([]ListPartitionReassignmentsRequestListPartitionReassignmentsTopics, length)
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
					// PartitionIndexes
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
							m.Topics[i].PartitionIndexes = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].PartitionIndexes[i] = val
							}
						} else {
							var err error
							length, err = protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.Topics[i].PartitionIndexes = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].PartitionIndexes[i] = val
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

// ListPartitionReassignmentsRequestListPartitionReassignmentsTopics represents The topics to list partition reassignments for, or null to list everything..
type ListPartitionReassignmentsRequestListPartitionReassignmentsTopics struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The partitions to list partition reassignments for.
	PartitionIndexes []int32 `json:"partitionindexes" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for ListPartitionReassignmentsRequest.
func (m *ListPartitionReassignmentsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ListPartitionReassignmentsRequest.
func (m *ListPartitionReassignmentsRequest) readTaggedFields(r io.Reader, version int16) error {
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
