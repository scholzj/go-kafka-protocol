package describeclientquotas

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeClientQuotasRequestApiKey        = 48
	DescribeClientQuotasRequestHeaderVersion = 1
)

// DescribeClientQuotasRequest represents a request message.
type DescribeClientQuotasRequest struct {
	// Filter components to apply to quota entities.
	Components []DescribeClientQuotasRequestComponentData `json:"components" versions:"0-999"`
	// Whether the match is strict, i.e. should exclude entities with unspecified entity types.
	Strict bool `json:"strict" versions:"0-999"`
}

// Encode encodes a DescribeClientQuotasRequest to a byte slice for the given version.
func (m *DescribeClientQuotasRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeClientQuotasRequest from a byte slice for the given version.
func (m *DescribeClientQuotasRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeClientQuotasRequest to an io.Writer for the given version.
func (m *DescribeClientQuotasRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
		isFlexible = true
	}

	// Components
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Components) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Components))); err != nil {
				return err
			}
		}
		for i := range m.Components {
			// EntityType
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Components[i].EntityType); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Components[i].EntityType); err != nil {
						return err
					}
				}
			}
			// MatchType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Components[i].MatchType); err != nil {
					return err
				}
			}
			// Match
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Components[i].Match); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Components[i].Match); err != nil {
						return err
					}
				}
			}
		}
	}
	// Strict
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.Strict); err != nil {
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

// Read reads a DescribeClientQuotasRequest from an io.Reader for the given version.
func (m *DescribeClientQuotasRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
		isFlexible = true
	}

	// Components
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
			m.Components = make([]DescribeClientQuotasRequestComponentData, length)
			for i := int32(0); i < length; i++ {
				// EntityType
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Components[i].EntityType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Components[i].EntityType = val
					}
				}
				// MatchType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Components[i].MatchType = val
				}
				// Match
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Components[i].Match = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Components[i].Match = val
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Components = make([]DescribeClientQuotasRequestComponentData, length)
			for i := int32(0); i < length; i++ {
				// EntityType
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Components[i].EntityType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Components[i].EntityType = val
					}
				}
				// MatchType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Components[i].MatchType = val
				}
				// Match
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Components[i].Match = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Components[i].Match = val
					}
				}
			}
		}
	}
	// Strict
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.Strict = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// DescribeClientQuotasRequestComponentData represents Filter components to apply to quota entities..
type DescribeClientQuotasRequestComponentData struct {
	// The entity type that the filter component applies to.
	EntityType string `json:"entitytype" versions:"0-999"`
	// How to match the entity {0 = exact name, 1 = default name, 2 = any specified name}.
	MatchType int8 `json:"matchtype" versions:"0-999"`
	// The string to match against, or null if unused for the match type.
	Match *string `json:"match" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeClientQuotasRequest.
func (m *DescribeClientQuotasRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeClientQuotasRequest.
func (m *DescribeClientQuotasRequest) readTaggedFields(r io.Reader, version int16) error {
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
