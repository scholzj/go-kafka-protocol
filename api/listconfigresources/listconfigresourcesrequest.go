package listconfigresources

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ListConfigResourcesRequestApiKey        = 74
	ListConfigResourcesRequestHeaderVersion = 1
)

// ListConfigResourcesRequest represents a request message.
type ListConfigResourcesRequest struct {
	// The list of resource type. If the list is empty, it uses default supported config resource types.
	ResourceTypes []int8 `json:"resourcetypes" versions:"1-999"`
}

// Encode encodes a ListConfigResourcesRequest to a byte slice for the given version.
func (m *ListConfigResourcesRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ListConfigResourcesRequest from a byte slice for the given version.
func (m *ListConfigResourcesRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ListConfigResourcesRequest to an io.Writer for the given version.
func (m *ListConfigResourcesRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ResourceTypes
	if version >= 1 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.ResourceTypes) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.ResourceTypes))); err != nil {
				return err
			}
		}
		for i := range m.ResourceTypes {
			if err := protocol.WriteInt8(w, m.ResourceTypes[i]); err != nil {
				return err
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

// Read reads a ListConfigResourcesRequest from an io.Reader for the given version.
func (m *ListConfigResourcesRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ResourceTypes
	if version >= 1 && version <= 999 {
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
			m.ResourceTypes = make([]int8, length)
			for i := int32(0); i < length; i++ {
				val, err := protocol.ReadInt8(r)
				if err != nil {
					return err
				}
				m.ResourceTypes[i] = val
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.ResourceTypes = make([]int8, length)
			for i := int32(0); i < length; i++ {
				val, err := protocol.ReadInt8(r)
				if err != nil {
					return err
				}
				m.ResourceTypes[i] = val
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

// writeTaggedFields writes tagged fields for ListConfigResourcesRequest.
func (m *ListConfigResourcesRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ListConfigResourcesRequest.
func (m *ListConfigResourcesRequest) readTaggedFields(r io.Reader, version int16) error {
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
