package describetopicpartitions

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeTopicPartitionsRequestApiKey        = 75
	DescribeTopicPartitionsRequestHeaderVersion = 1
)

// DescribeTopicPartitionsRequest represents a request message.
type DescribeTopicPartitionsRequest struct {
	// The topics to fetch details for.
	Topics []DescribeTopicPartitionsRequestTopicRequest `json:"topics" versions:"0-999"`
	// The maximum number of partitions included in the response.
	ResponsePartitionLimit int32 `json:"responsepartitionlimit" versions:"0-999"`
	// The first topic and partition index to fetch details for.
	Cursor DescribeTopicPartitionsRequestCursor `json:"cursor" versions:"0-999"`
}

// Encode encodes a DescribeTopicPartitionsRequest to a byte slice for the given version.
func (m *DescribeTopicPartitionsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeTopicPartitionsRequest from a byte slice for the given version.
func (m *DescribeTopicPartitionsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeTopicPartitionsRequest to an io.Writer for the given version.
func (m *DescribeTopicPartitionsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
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
		}
	}
	// ResponsePartitionLimit
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ResponsePartitionLimit); err != nil {
			return err
		}
	}
	// Cursor
	if version >= 0 && version <= 999 {
		// TopicName
		if version >= 0 && version <= 999 {
			if isFlexible {
				if err := protocol.WriteCompactString(w, m.Cursor.TopicName); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteString(w, m.Cursor.TopicName); err != nil {
					return err
				}
			}
		}
		// PartitionIndex
		if version >= 0 && version <= 999 {
			if err := protocol.WriteInt32(w, m.Cursor.PartitionIndex); err != nil {
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

// Read reads a DescribeTopicPartitionsRequest from an io.Reader for the given version.
func (m *DescribeTopicPartitionsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
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
			m.Topics = make([]DescribeTopicPartitionsRequestTopicRequest, length)
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
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Topics = make([]DescribeTopicPartitionsRequestTopicRequest, length)
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
			}
		}
	}
	// ResponsePartitionLimit
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ResponsePartitionLimit = val
	}
	// Cursor
	if version >= 0 && version <= 999 {
		// TopicName
		if version >= 0 && version <= 999 {
			if isFlexible {
				val, err := protocol.ReadCompactString(r)
				if err != nil {
					return err
				}
				m.Cursor.TopicName = val
			} else {
				val, err := protocol.ReadString(r)
				if err != nil {
					return err
				}
				m.Cursor.TopicName = val
			}
		}
		// PartitionIndex
		if version >= 0 && version <= 999 {
			val, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Cursor.PartitionIndex = val
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

// DescribeTopicPartitionsRequestTopicRequest represents The topics to fetch details for..
type DescribeTopicPartitionsRequestTopicRequest struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
}

// DescribeTopicPartitionsRequestCursor represents The first topic and partition index to fetch details for..
type DescribeTopicPartitionsRequestCursor struct {
	// The name for the first topic to process.
	TopicName string `json:"topicname" versions:"0-999"`
	// The partition index to start with.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeTopicPartitionsRequest.
func (m *DescribeTopicPartitionsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeTopicPartitionsRequest.
func (m *DescribeTopicPartitionsRequest) readTaggedFields(r io.Reader, version int16) error {
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
