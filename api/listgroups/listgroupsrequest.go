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
			if err := protocol.WriteCompactStringArray(w, m.StatesFilter); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteStringArray(w, m.StatesFilter); err != nil {
				return err
			}
		}
	}
	// TypesFilter
	if version >= 5 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactStringArray(w, m.TypesFilter); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteStringArray(w, m.TypesFilter); err != nil {
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
		if isFlexible {
			val, err := protocol.ReadCompactStringArray(r)
			if err != nil {
				return err
			}
			m.StatesFilter = val
		} else {
			val, err := protocol.ReadStringArray(r)
			if err != nil {
				return err
			}
			m.StatesFilter = val
		}
	}
	// TypesFilter
	if version >= 5 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactStringArray(r)
			if err != nil {
				return err
			}
			m.TypesFilter = val
		} else {
			val, err := protocol.ReadStringArray(r)
			if err != nil {
				return err
			}
			m.TypesFilter = val
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
