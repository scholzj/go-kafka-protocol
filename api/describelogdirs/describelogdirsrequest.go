package describelogdirs

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeLogDirsRequestApiKey        = 35
	DescribeLogDirsRequestHeaderVersion = 1
)

// DescribeLogDirsRequest represents a request message.
type DescribeLogDirsRequest struct {
	// Each topic that we want to describe log directories for, or null for all topics.
	Topics []DescribeLogDirsRequestDescribableLogDirTopic `json:"topics" versions:"0-999"`
}

// Encode encodes a DescribeLogDirsRequest to a byte slice for the given version.
func (m *DescribeLogDirsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeLogDirsRequest from a byte slice for the given version.
func (m *DescribeLogDirsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeLogDirsRequest to an io.Writer for the given version.
func (m *DescribeLogDirsRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
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
				// Topic
				if version >= 0 && version <= 999 {
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
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i]); err != nil {
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

// Read reads a DescribeLogDirsRequest from an io.Reader for the given version.
func (m *DescribeLogDirsRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 4 {
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
			if lengthUint == 0 {
				m.Topics = nil
			} else {
				if lengthUint < 1 {
					return errors.New("invalid compact array length")
				}
				length = int32(lengthUint - 1)
				m.Topics = make([]DescribeLogDirsRequestDescribableLogDirTopic, length)
				for i := int32(0); i < length; i++ {
					// Topic
					if version >= 0 && version <= 999 {
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
							m.Topics[i].Partitions = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i] = val
							}
						} else {
							var err error
							length, err = protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.Topics[i].Partitions = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i] = val
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
				m.Topics = make([]DescribeLogDirsRequestDescribableLogDirTopic, length)
				for i := int32(0); i < length; i++ {
					// Topic
					if version >= 0 && version <= 999 {
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
							m.Topics[i].Partitions = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i] = val
							}
						} else {
							var err error
							length, err = protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.Topics[i].Partitions = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i] = val
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

// DescribeLogDirsRequestDescribableLogDirTopic represents Each topic that we want to describe log directories for, or null for all topics..
type DescribeLogDirsRequestDescribableLogDirTopic struct {
	// The topic name.
	Topic string `json:"topic" versions:"0-999"`
	// The partition indexes.
	Partitions []int32 `json:"partitions" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeLogDirsRequest.
func (m *DescribeLogDirsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeLogDirsRequest.
func (m *DescribeLogDirsRequest) readTaggedFields(r io.Reader, version int16) error {
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
