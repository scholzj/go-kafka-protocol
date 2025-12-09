package deletetopics

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DeleteTopicsRequestApiKey        = 20
	DeleteTopicsRequestHeaderVersion = 1
)

// DeleteTopicsRequest represents a request message.
type DeleteTopicsRequest struct {
	// The name or topic ID of the topic.
	Topics []DeleteTopicsRequestDeleteTopicState `json:"topics" versions:"6-999"`
	// The names of the topics to delete.
	TopicNames []string `json:"topicnames" versions:"0-5"`
	// The length of time in milliseconds to wait for the deletions to complete.
	TimeoutMs int32 `json:"timeoutms" versions:"0-999"`
}

// Encode encodes a DeleteTopicsRequest to a byte slice for the given version.
func (m *DeleteTopicsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DeleteTopicsRequest from a byte slice for the given version.
func (m *DeleteTopicsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DeleteTopicsRequest to an io.Writer for the given version.
func (m *DeleteTopicsRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// Topics
	if version >= 6 && version <= 999 {
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
			if version >= 6 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Topics[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Topics[i].Name); err != nil {
						return err
					}
				}
			}
			// TopicId
			if version >= 6 && version <= 999 {
				if err := protocol.WriteUUID(w, m.Topics[i].TopicId); err != nil {
					return err
				}
			}
		}
	}
	// TopicNames
	if version >= 0 && version <= 5 {
		if isFlexible {
			length := uint32(len(m.TopicNames) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.TopicNames))); err != nil {
				return err
			}
		}
		for i := range m.TopicNames {
			if isFlexible {
				if err := protocol.WriteCompactString(w, m.TopicNames[i]); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteString(w, m.TopicNames[i]); err != nil {
					return err
				}
			}
			_ = i
		}
	}
	// TimeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TimeoutMs); err != nil {
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

// Read reads a DeleteTopicsRequest from an io.Reader for the given version.
func (m *DeleteTopicsRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// Topics
	if version >= 6 && version <= 999 {
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
			m.Topics = make([]DeleteTopicsRequestDeleteTopicState, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 6 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Name = val
					}
				}
				// TopicId
				if version >= 6 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Topics[i].TopicId = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Topics = make([]DeleteTopicsRequestDeleteTopicState, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 6 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Topics[i].Name = val
					}
				}
				// TopicId
				if version >= 6 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Topics[i].TopicId = val
				}
			}
		}
	}
	// TopicNames
	if version >= 0 && version <= 5 {
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
			m.TopicNames = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.TopicNames[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.TopicNames[i] = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.TopicNames = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.TopicNames[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.TopicNames[i] = val
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
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTopicsRequestDeleteTopicState represents The name or topic ID of the topic..
type DeleteTopicsRequestDeleteTopicState struct {
	// The topic name.
	Name *string `json:"name" versions:"6-999"`
	// The unique topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"6-999"`
}

// writeTaggedFields writes tagged fields for DeleteTopicsRequest.
func (m *DeleteTopicsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DeleteTopicsRequest.
func (m *DeleteTopicsRequest) readTaggedFields(r io.Reader, version int16) error {
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
