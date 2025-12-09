package sharegroupdescribe

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ShareGroupDescribeRequestApiKey        = 77
	ShareGroupDescribeRequestHeaderVersion = 1
)

// ShareGroupDescribeRequest represents a request message.
type ShareGroupDescribeRequest struct {
	// The ids of the groups to describe.
	GroupIds []string `json:"groupids" versions:"0-999"`
	// Whether to include authorized operations.
	IncludeAuthorizedOperations bool `json:"includeauthorizedoperations" versions:"0-999"`
}

// Encode encodes a ShareGroupDescribeRequest to a byte slice for the given version.
func (m *ShareGroupDescribeRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ShareGroupDescribeRequest from a byte slice for the given version.
func (m *ShareGroupDescribeRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ShareGroupDescribeRequest to an io.Writer for the given version.
func (m *ShareGroupDescribeRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// GroupIds
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.GroupIds) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.GroupIds))); err != nil {
				return err
			}
		}
		for i := range m.GroupIds {
			if isFlexible {
				if err := protocol.WriteCompactString(w, m.GroupIds[i]); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteString(w, m.GroupIds[i]); err != nil {
					return err
				}
			}
			_ = i
		}
	}
	// IncludeAuthorizedOperations
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.IncludeAuthorizedOperations); err != nil {
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

// Read reads a ShareGroupDescribeRequest from an io.Reader for the given version.
func (m *ShareGroupDescribeRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// GroupIds
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
			m.GroupIds = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.GroupIds[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.GroupIds[i] = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.GroupIds = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.GroupIds[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.GroupIds[i] = val
				}
			}
		}
	}
	// IncludeAuthorizedOperations
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IncludeAuthorizedOperations = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// TopicPartitions represents .
type TopicPartitions struct {
	// The topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// The topic name.
	TopicName string `json:"topicname" versions:"0-999"`
	// The partitions.
	Partitions []int32 `json:"partitions" versions:"0-999"`
}

// Assignment represents .
type Assignment struct {
	// The assigned topic-partitions to the member.
	TopicPartitions []TopicPartitions `json:"topicpartitions" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for ShareGroupDescribeRequest.
func (m *ShareGroupDescribeRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareGroupDescribeRequest.
func (m *ShareGroupDescribeRequest) readTaggedFields(r io.Reader, version int16) error {
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
