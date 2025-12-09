package deletetopics

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DeleteTopicsResponseApiKey        = 20
	DeleteTopicsResponseHeaderVersion = 1
)

// DeleteTopicsResponse represents a response message.
type DeleteTopicsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"1-999"`
	// The results for each topic we tried to delete.
	Responses []DeleteTopicsResponseDeletableTopicResult `json:"responses" versions:"0-999"`
}

// Encode encodes a DeleteTopicsResponse to a byte slice for the given version.
func (m *DeleteTopicsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DeleteTopicsResponse from a byte slice for the given version.
func (m *DeleteTopicsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DeleteTopicsResponse to an io.Writer for the given version.
func (m *DeleteTopicsResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// Responses
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Responses) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Responses))); err != nil {
				return err
			}
		}
		for i := range m.Responses {
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Responses[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Responses[i].Name); err != nil {
						return err
					}
				}
			}
			// TopicId
			if version >= 6 && version <= 999 {
				if err := protocol.WriteUUID(w, m.Responses[i].TopicId); err != nil {
					return err
				}
			}
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Responses[i].ErrorCode); err != nil {
					return err
				}
			}
			// ErrorMessage
			if version >= 5 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Responses[i].ErrorMessage); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Responses[i].ErrorMessage); err != nil {
						return err
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

// Read reads a DeleteTopicsResponse from an io.Reader for the given version.
func (m *DeleteTopicsResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// Responses
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
			m.Responses = make([]DeleteTopicsResponseDeletableTopicResult, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Name = val
					}
				}
				// TopicId
				if version >= 6 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Responses[i].TopicId = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Responses[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 5 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Responses[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Responses[i].ErrorMessage = val
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Responses = make([]DeleteTopicsResponseDeletableTopicResult, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Name = val
					}
				}
				// TopicId
				if version >= 6 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Responses[i].TopicId = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Responses[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 5 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Responses[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Responses[i].ErrorMessage = val
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

// DeleteTopicsResponseDeletableTopicResult represents The results for each topic we tried to delete..
type DeleteTopicsResponseDeletableTopicResult struct {
	// The topic name.
	Name *string `json:"name" versions:"0-999"`
	// The unique topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"6-999"`
	// The deletion error, or 0 if the deletion succeeded.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"5-999"`
}

// writeTaggedFields writes tagged fields for DeleteTopicsResponse.
func (m *DeleteTopicsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DeleteTopicsResponse.
func (m *DeleteTopicsResponse) readTaggedFields(r io.Reader, version int16) error {
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
