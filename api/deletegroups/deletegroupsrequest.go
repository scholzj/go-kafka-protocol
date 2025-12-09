package deletegroups

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DeleteGroupsRequestApiKey        = 42
	DeleteGroupsRequestHeaderVersion = 1
)

// DeleteGroupsRequest represents a request message.
type DeleteGroupsRequest struct {
	// The group names to delete.
	GroupsNames []string `json:"groupsnames" versions:"0-999"`
}

// Encode encodes a DeleteGroupsRequest to a byte slice for the given version.
func (m *DeleteGroupsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DeleteGroupsRequest from a byte slice for the given version.
func (m *DeleteGroupsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DeleteGroupsRequest to an io.Writer for the given version.
func (m *DeleteGroupsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// GroupsNames
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactStringArray(w, m.GroupsNames); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteStringArray(w, m.GroupsNames); err != nil {
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

// Read reads a DeleteGroupsRequest from an io.Reader for the given version.
func (m *DeleteGroupsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// GroupsNames
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactStringArray(r)
			if err != nil {
				return err
			}
			m.GroupsNames = val
		} else {
			val, err := protocol.ReadStringArray(r)
			if err != nil {
				return err
			}
			m.GroupsNames = val
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

// writeTaggedFields writes tagged fields for DeleteGroupsRequest.
func (m *DeleteGroupsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DeleteGroupsRequest.
func (m *DeleteGroupsRequest) readTaggedFields(r io.Reader, version int16) error {
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
