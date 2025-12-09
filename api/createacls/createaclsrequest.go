package createacls

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	CreateAclsRequestApiKey        = 30
	CreateAclsRequestHeaderVersion = 1
)

// CreateAclsRequest represents a request message.
type CreateAclsRequest struct {
	// The ACLs that we want to create.
	Creations []CreateAclsRequestAclCreation `json:"creations" versions:"0-999"`
}

// Encode encodes a CreateAclsRequest to a byte slice for the given version.
func (m *CreateAclsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a CreateAclsRequest from a byte slice for the given version.
func (m *CreateAclsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a CreateAclsRequest to an io.Writer for the given version.
func (m *CreateAclsRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Creations
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Creations) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Creations))); err != nil {
				return err
			}
		}
		for i := range m.Creations {
			// ResourceType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Creations[i].ResourceType); err != nil {
					return err
				}
			}
			// ResourceName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Creations[i].ResourceName); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Creations[i].ResourceName); err != nil {
						return err
					}
				}
			}
			// ResourcePatternType
			if version >= 1 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Creations[i].ResourcePatternType); err != nil {
					return err
				}
			}
			// Principal
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Creations[i].Principal); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Creations[i].Principal); err != nil {
						return err
					}
				}
			}
			// Host
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Creations[i].Host); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Creations[i].Host); err != nil {
						return err
					}
				}
			}
			// Operation
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Creations[i].Operation); err != nil {
					return err
				}
			}
			// PermissionType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Creations[i].PermissionType); err != nil {
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

// Read reads a CreateAclsRequest from an io.Reader for the given version.
func (m *CreateAclsRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Creations
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
			m.Creations = make([]CreateAclsRequestAclCreation, length)
			for i := int32(0); i < length; i++ {
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Creations[i].ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Creations[i].ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Creations[i].ResourceName = val
					}
				}
				// ResourcePatternType
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Creations[i].ResourcePatternType = val
				}
				// Principal
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Creations[i].Principal = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Creations[i].Principal = val
					}
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Creations[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Creations[i].Host = val
					}
				}
				// Operation
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Creations[i].Operation = val
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Creations[i].PermissionType = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Creations = make([]CreateAclsRequestAclCreation, length)
			for i := int32(0); i < length; i++ {
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Creations[i].ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Creations[i].ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Creations[i].ResourceName = val
					}
				}
				// ResourcePatternType
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Creations[i].ResourcePatternType = val
				}
				// Principal
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Creations[i].Principal = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Creations[i].Principal = val
					}
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Creations[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Creations[i].Host = val
					}
				}
				// Operation
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Creations[i].Operation = val
				}
				// PermissionType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Creations[i].PermissionType = val
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

// CreateAclsRequestAclCreation represents The ACLs that we want to create..
type CreateAclsRequestAclCreation struct {
	// The type of the resource.
	ResourceType int8 `json:"resourcetype" versions:"0-999"`
	// The resource name for the ACL.
	ResourceName string `json:"resourcename" versions:"0-999"`
	// The pattern type for the ACL.
	ResourcePatternType int8 `json:"resourcepatterntype" versions:"1-999"`
	// The principal for the ACL.
	Principal string `json:"principal" versions:"0-999"`
	// The host for the ACL.
	Host string `json:"host" versions:"0-999"`
	// The operation type for the ACL (read, write, etc.).
	Operation int8 `json:"operation" versions:"0-999"`
	// The permission type for the ACL (allow, deny, etc.).
	PermissionType int8 `json:"permissiontype" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for CreateAclsRequest.
func (m *CreateAclsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreateAclsRequest.
func (m *CreateAclsRequest) readTaggedFields(r io.Reader, version int16) error {
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
