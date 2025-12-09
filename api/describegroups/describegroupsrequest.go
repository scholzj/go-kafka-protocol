package describegroups

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeGroupsRequestApiKey        = 15
	DescribeGroupsRequestHeaderVersion = 1
)

// DescribeGroupsRequest represents a request message.
type DescribeGroupsRequest struct {
	// The names of the groups to describe.
	Groups []string `json:"groups" versions:"0-999"`
	// Whether to include authorized operations.
	IncludeAuthorizedOperations bool `json:"includeauthorizedoperations" versions:"3-999"`
}

// Encode encodes a DescribeGroupsRequest to a byte slice for the given version.
func (m *DescribeGroupsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeGroupsRequest from a byte slice for the given version.
func (m *DescribeGroupsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeGroupsRequest to an io.Writer for the given version.
func (m *DescribeGroupsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 5 {
		isFlexible = true
	}

	// Groups
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactStringArray(w, m.Groups); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteStringArray(w, m.Groups); err != nil {
				return err
			}
		}
	}
	// IncludeAuthorizedOperations
	if version >= 3 && version <= 999 {
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

// Read reads a DescribeGroupsRequest from an io.Reader for the given version.
func (m *DescribeGroupsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 5 {
		isFlexible = true
	}

	// Groups
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactStringArray(r)
			if err != nil {
				return err
			}
			m.Groups = val
		} else {
			val, err := protocol.ReadStringArray(r)
			if err != nil {
				return err
			}
			m.Groups = val
		}
	}
	// IncludeAuthorizedOperations
	if version >= 3 && version <= 999 {
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

// writeTaggedFields writes tagged fields for DescribeGroupsRequest.
func (m *DescribeGroupsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeGroupsRequest.
func (m *DescribeGroupsRequest) readTaggedFields(r io.Reader, version int16) error {
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
