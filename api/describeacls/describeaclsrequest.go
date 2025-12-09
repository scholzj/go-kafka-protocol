package describeacls

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeAclsRequestApiKey        = 29
	DescribeAclsRequestHeaderVersion = 1
)

// DescribeAclsRequest represents a request message.
type DescribeAclsRequest struct {
	// The resource type.
	ResourceTypeFilter int8 `json:"resourcetypefilter" versions:"0-999"`
	// The resource name, or null to match any resource name.
	ResourceNameFilter *string `json:"resourcenamefilter" versions:"0-999"`
	// The resource pattern to match.
	PatternTypeFilter int8 `json:"patterntypefilter" versions:"1-999"`
	// The principal to match, or null to match any principal.
	PrincipalFilter *string `json:"principalfilter" versions:"0-999"`
	// The host to match, or null to match any host.
	HostFilter *string `json:"hostfilter" versions:"0-999"`
	// The operation to match.
	Operation int8 `json:"operation" versions:"0-999"`
	// The permission type to match.
	PermissionType int8 `json:"permissiontype" versions:"0-999"`
}

// Encode encodes a DescribeAclsRequest to a byte slice for the given version.
func (m *DescribeAclsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeAclsRequest from a byte slice for the given version.
func (m *DescribeAclsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeAclsRequest to an io.Writer for the given version.
func (m *DescribeAclsRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ResourceTypeFilter
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt8(w, m.ResourceTypeFilter); err != nil {
			return err
		}
	}
	// ResourceNameFilter
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ResourceNameFilter); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ResourceNameFilter); err != nil {
				return err
			}
		}
	}
	// PatternTypeFilter
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt8(w, m.PatternTypeFilter); err != nil {
			return err
		}
	}
	// PrincipalFilter
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.PrincipalFilter); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.PrincipalFilter); err != nil {
				return err
			}
		}
	}
	// HostFilter
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.HostFilter); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.HostFilter); err != nil {
				return err
			}
		}
	}
	// Operation
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt8(w, m.Operation); err != nil {
			return err
		}
	}
	// PermissionType
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt8(w, m.PermissionType); err != nil {
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

// Read reads a DescribeAclsRequest from an io.Reader for the given version.
func (m *DescribeAclsRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ResourceTypeFilter
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		m.ResourceTypeFilter = val
	}
	// ResourceNameFilter
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ResourceNameFilter = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ResourceNameFilter = val
		}
	}
	// PatternTypeFilter
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		m.PatternTypeFilter = val
	}
	// PrincipalFilter
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.PrincipalFilter = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.PrincipalFilter = val
		}
	}
	// HostFilter
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.HostFilter = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.HostFilter = val
		}
	}
	// Operation
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		m.Operation = val
	}
	// PermissionType
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		m.PermissionType = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for DescribeAclsRequest.
func (m *DescribeAclsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeAclsRequest.
func (m *DescribeAclsRequest) readTaggedFields(r io.Reader, version int16) error {
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
