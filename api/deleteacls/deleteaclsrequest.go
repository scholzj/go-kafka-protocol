package deleteacls

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DeleteAclsRequestApiKey        = 31
	DeleteAclsRequestHeaderVersion = 1
)

// DeleteAclsRequest represents a request message.
type DeleteAclsRequest struct {
	// The filters to use when deleting ACLs.
	Filters []DeleteAclsRequestDeleteAclsFilter `json:"filters" versions:"0-999"`
}

// Encode encodes a DeleteAclsRequest to a byte slice for the given version.
func (m *DeleteAclsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DeleteAclsRequest from a byte slice for the given version.
func (m *DeleteAclsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DeleteAclsRequest to an io.Writer for the given version.
func (m *DeleteAclsRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Filters
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Filters) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Filters))); err != nil {
				return err
			}
		}
		for i := range m.Filters {
			// ResourceTypeFilter
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Filters[i].ResourceTypeFilter); err != nil {
					return err
				}
			}
			// ResourceNameFilter
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Filters[i].ResourceNameFilter); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Filters[i].ResourceNameFilter); err != nil {
						return err
					}
				}
			}
			// PatternTypeFilter
			if version >= 1 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Filters[i].PatternTypeFilter); err != nil {
					return err
				}
			}
			// PrincipalFilter
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Filters[i].PrincipalFilter); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Filters[i].PrincipalFilter); err != nil {
						return err
					}
				}
			}
			// HostFilter
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Filters[i].HostFilter); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Filters[i].HostFilter); err != nil {
						return err
					}
				}
			}
			// Operation
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Filters[i].Operation); err != nil {
					return err
				}
			}
			// PermissionType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Filters[i].PermissionType); err != nil {
					return err
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

// Read reads a DeleteAclsRequest from an io.Reader for the given version.
func (m *DeleteAclsRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Filters
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
			m.Filters = make([]DeleteAclsRequestDeleteAclsFilter, length)
			for i := int32(0); i < length; i++ {
				// ResourceTypeFilter
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Filters[i].ResourceTypeFilter = val
				}
				// ResourceNameFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].ResourceNameFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].ResourceNameFilter = val
					}
				}
				// PatternTypeFilter
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Filters[i].PatternTypeFilter = val
				}
				// PrincipalFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].PrincipalFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].PrincipalFilter = val
					}
				}
				// HostFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].HostFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].HostFilter = val
					}
				}
				// Operation
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Filters[i].Operation = val
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Filters[i].PermissionType = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Filters = make([]DeleteAclsRequestDeleteAclsFilter, length)
			for i := int32(0); i < length; i++ {
				// ResourceTypeFilter
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Filters[i].ResourceTypeFilter = val
				}
				// ResourceNameFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].ResourceNameFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].ResourceNameFilter = val
					}
				}
				// PatternTypeFilter
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Filters[i].PatternTypeFilter = val
				}
				// PrincipalFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].PrincipalFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].PrincipalFilter = val
					}
				}
				// HostFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].HostFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Filters[i].HostFilter = val
					}
				}
				// Operation
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Filters[i].Operation = val
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Filters[i].PermissionType = val
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

// DeleteAclsRequestDeleteAclsFilter represents The filters to use when deleting ACLs..
type DeleteAclsRequestDeleteAclsFilter struct {
	// The resource type.
	ResourceTypeFilter int8 `json:"resourcetypefilter" versions:"0-999"`
	// The resource name, or null to match any resource name.
	ResourceNameFilter *string `json:"resourcenamefilter" versions:"0-999"`
	// The pattern type.
	PatternTypeFilter int8 `json:"patterntypefilter" versions:"1-999"`
	// The principal filter, or null to accept all principals.
	PrincipalFilter *string `json:"principalfilter" versions:"0-999"`
	// The host filter, or null to accept all hosts.
	HostFilter *string `json:"hostfilter" versions:"0-999"`
	// The ACL operation.
	Operation int8 `json:"operation" versions:"0-999"`
	// The permission type.
	PermissionType int8 `json:"permissiontype" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DeleteAclsRequest.
func (m *DeleteAclsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DeleteAclsRequest.
func (m *DeleteAclsRequest) readTaggedFields(r io.Reader, version int16) error {
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
