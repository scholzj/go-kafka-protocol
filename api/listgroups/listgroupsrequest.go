package listgroups

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ListGroupsRequestApiKey        = 16
	ListGroupsRequestHeaderVersion = 1
)

// ListGroupsRequest represents a request message.
type ListGroupsRequest struct {
	// The states of the groups we want to list. If empty, all groups are returned with their state.
	StatesFilter []string `json:"statesfilter" versions:"4-999"`
	// The types of the groups we want to list. If empty, all groups are returned with their type.
	TypesFilter []string `json:"typesfilter" versions:"5-999"`
}

// Encode encodes a ListGroupsRequest to a byte slice for the given version.
func (m *ListGroupsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ListGroupsRequest from a byte slice for the given version.
func (m *ListGroupsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ListGroupsRequest to an io.Writer for the given version.
func (m *ListGroupsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// StatesFilter
	if version >= 4 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.StatesFilter) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.StatesFilter))); err != nil {
				return err
			}
		}
		for i := range m.StatesFilter {
			if isFlexible {
				if err := protocol.WriteCompactString(w, m.StatesFilter[i]); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteString(w, m.StatesFilter[i]); err != nil {
					return err
				}
			}
			_ = i
		}
	}
	// TypesFilter
	if version >= 5 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.TypesFilter) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.TypesFilter))); err != nil {
				return err
			}
		}
		for i := range m.TypesFilter {
			if isFlexible {
				if err := protocol.WriteCompactString(w, m.TypesFilter[i]); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteString(w, m.TypesFilter[i]); err != nil {
					return err
				}
			}
			_ = i
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

// Read reads a ListGroupsRequest from an io.Reader for the given version.
func (m *ListGroupsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// StatesFilter
	if version >= 4 && version <= 999 {
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
			m.StatesFilter = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.StatesFilter[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.StatesFilter[i] = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.StatesFilter = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.StatesFilter[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.StatesFilter[i] = val
				}
			}
		}
	}
	// TypesFilter
	if version >= 5 && version <= 999 {
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
			m.TypesFilter = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.TypesFilter[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.TypesFilter[i] = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.TypesFilter = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.TypesFilter[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.TypesFilter[i] = val
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

// writeTaggedFields writes tagged fields for ListGroupsRequest.
func (m *ListGroupsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ListGroupsRequest.
func (m *ListGroupsRequest) readTaggedFields(r io.Reader, version int16) error {
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
