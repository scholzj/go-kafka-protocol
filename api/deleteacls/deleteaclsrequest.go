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
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DeleteAclsRequestDeleteAclsFilter)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// ResourceTypeFilter
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.ResourceTypeFilter); err != nil {
					return nil, err
				}
			}
			// ResourceNameFilter
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.ResourceNameFilter); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.ResourceNameFilter); err != nil {
						return nil, err
					}
				}
			}
			// PatternTypeFilter
			if version >= 1 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.PatternTypeFilter); err != nil {
					return nil, err
				}
			}
			// PrincipalFilter
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.PrincipalFilter); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.PrincipalFilter); err != nil {
						return nil, err
					}
				}
			}
			// HostFilter
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.HostFilter); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.HostFilter); err != nil {
						return nil, err
					}
				}
			}
			// Operation
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.Operation); err != nil {
					return nil, err
				}
			}
			// PermissionType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.PermissionType); err != nil {
					return nil, err
				}
			}
			// Write tagged fields if flexible
			if isFlexible {
				if err := structItem.writeTaggedFields(elemW, version); err != nil {
					return nil, err
				}
			}
			return elemBuf.Bytes(), nil
		}
		items := make([]interface{}, len(m.Filters))
		for i := range m.Filters {
			items[i] = m.Filters[i]
		}
		if isFlexible {
			if err := protocol.WriteCompactArray(w, items, encoder); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, items, encoder); err != nil {
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
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DeleteAclsRequestDeleteAclsFilter
			elemR := bytes.NewReader(data)
			// ResourceTypeFilter
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt8(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ResourceTypeFilter = val
			}
			// ResourceNameFilter
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ResourceNameFilter = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ResourceNameFilter = val
				}
			}
			// PatternTypeFilter
			if version >= 1 && version <= 999 {
				val, err := protocol.ReadInt8(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.PatternTypeFilter = val
			}
			// PrincipalFilter
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.PrincipalFilter = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.PrincipalFilter = val
				}
			}
			// HostFilter
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.HostFilter = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.HostFilter = val
				}
			}
			// Operation
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt8(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.Operation = val
			}
			// PermissionType
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt8(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.PermissionType = val
			}
			// Read tagged fields if flexible
			if isFlexible {
				if err := elem.readTaggedFields(elemR, version); err != nil {
					return nil, 0, err
				}
			}
			consumed := len(data) - elemR.Len()
			return elem, consumed, nil
		}
		if isFlexible {
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint < 1 {
				return errors.New("invalid compact array length")
			}
			length := int32(lengthUint - 1)
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem DeleteAclsRequestDeleteAclsFilter
				// ResourceTypeFilter
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.ResourceTypeFilter = val
				}
				// ResourceNameFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceNameFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceNameFilter = val
					}
				}
				// PatternTypeFilter
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.PatternTypeFilter = val
				}
				// PrincipalFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalFilter = val
					}
				}
				// HostFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.HostFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.HostFilter = val
					}
				}
				// Operation
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.Operation = val
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.PermissionType = val
				}
				// ResourceTypeFilter
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.ResourceTypeFilter); err != nil {
						return err
					}
				}
				// ResourceNameFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.ResourceNameFilter); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.ResourceNameFilter); err != nil {
							return err
						}
					}
				}
				// PatternTypeFilter
				if version >= 1 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.PatternTypeFilter); err != nil {
						return err
					}
				}
				// PrincipalFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.PrincipalFilter); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.PrincipalFilter); err != nil {
							return err
						}
					}
				}
				// HostFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.HostFilter); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.HostFilter); err != nil {
							return err
						}
					}
				}
				// Operation
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.Operation); err != nil {
						return err
					}
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.PermissionType); err != nil {
						return err
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeCompactArray
			lengthBytes := protocol.EncodeVaruint32(lengthUint)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.Filters = make([]DeleteAclsRequestDeleteAclsFilter, len(decoded))
			for i, item := range decoded {
				m.Filters[i] = item.(DeleteAclsRequestDeleteAclsFilter)
			}
		} else {
			length, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem DeleteAclsRequestDeleteAclsFilter
				// ResourceTypeFilter
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.ResourceTypeFilter = val
				}
				// ResourceNameFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceNameFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceNameFilter = val
					}
				}
				// PatternTypeFilter
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.PatternTypeFilter = val
				}
				// PrincipalFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalFilter = val
					}
				}
				// HostFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.HostFilter = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.HostFilter = val
					}
				}
				// Operation
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.Operation = val
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.PermissionType = val
				}
				// ResourceTypeFilter
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.ResourceTypeFilter); err != nil {
						return err
					}
				}
				// ResourceNameFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.ResourceNameFilter); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.ResourceNameFilter); err != nil {
							return err
						}
					}
				}
				// PatternTypeFilter
				if version >= 1 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.PatternTypeFilter); err != nil {
						return err
					}
				}
				// PrincipalFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.PrincipalFilter); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.PrincipalFilter); err != nil {
							return err
						}
					}
				}
				// HostFilter
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.HostFilter); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.HostFilter); err != nil {
							return err
						}
					}
				}
				// Operation
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.Operation); err != nil {
						return err
					}
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.PermissionType); err != nil {
						return err
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeArray
			lengthBytes := protocol.EncodeInt32(length)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.Filters = make([]DeleteAclsRequestDeleteAclsFilter, len(decoded))
			for i, item := range decoded {
				m.Filters[i] = item.(DeleteAclsRequestDeleteAclsFilter)
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
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DeleteAclsRequestDeleteAclsFilter.
func (m *DeleteAclsRequestDeleteAclsFilter) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DeleteAclsRequestDeleteAclsFilter.
func (m *DeleteAclsRequestDeleteAclsFilter) readTaggedFields(r io.Reader, version int16) error {
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
		}
	}

	return nil
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
