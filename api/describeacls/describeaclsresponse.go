package describeacls

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeAclsResponseApiKey        = 29
	DescribeAclsResponseHeaderVersion = 1
)

// DescribeAclsResponse represents a response message.
type DescribeAclsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// Each Resource that is referenced in an ACL.
	Resources []DescribeAclsResponseDescribeAclsResource `json:"resources" versions:"0-999"`
}

// Encode encodes a DescribeAclsResponse to a byte slice for the given version.
func (m *DescribeAclsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeAclsResponse from a byte slice for the given version.
func (m *DescribeAclsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeAclsResponse to an io.Writer for the given version.
func (m *DescribeAclsResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// ErrorCode
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// ErrorMessage
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ErrorMessage); err != nil {
				return err
			}
		}
	}
	// Resources
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Resources) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Resources))); err != nil {
				return err
			}
		}
		for i := range m.Resources {
			// ResourceType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Resources[i].ResourceType); err != nil {
					return err
				}
			}
			// ResourceName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Resources[i].ResourceName); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Resources[i].ResourceName); err != nil {
						return err
					}
				}
			}
			// PatternType
			if version >= 1 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Resources[i].PatternType); err != nil {
					return err
				}
			}
			// Acls
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Resources[i].Acls) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Resources[i].Acls))); err != nil {
						return err
					}
				}
				for i := range m.Resources[i].Acls {
					// Principal
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Resources[i].Acls[i].Principal); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Resources[i].Acls[i].Principal); err != nil {
								return err
							}
						}
					}
					// Host
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Resources[i].Acls[i].Host); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Resources[i].Acls[i].Host); err != nil {
								return err
							}
						}
					}
					// Operation
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(w, m.Resources[i].Acls[i].Operation); err != nil {
							return err
						}
					}
					// PermissionType
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(w, m.Resources[i].Acls[i].PermissionType); err != nil {
							return err
						}
					}
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

// Read reads a DescribeAclsResponse from an io.Reader for the given version.
func (m *DescribeAclsResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// ErrorCode
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// ErrorMessage
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ErrorMessage = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ErrorMessage = val
		}
	}
	// Resources
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
			m.Resources = make([]DescribeAclsResponseDescribeAclsResource, length)
			for i := int32(0); i < length; i++ {
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Resources[i].ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Resources[i].ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Resources[i].ResourceName = val
					}
				}
				// PatternType
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Resources[i].PatternType = val
				}
				// Acls
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
						m.Resources[i].Acls = make([]DescribeAclsResponseAclDescription, length)
						for i := int32(0); i < length; i++ {
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Host = val
								}
							}
							// Operation
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Resources[i].Acls[i].Operation = val
							}
							// PermissionType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Resources[i].Acls[i].PermissionType = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Resources[i].Acls = make([]DescribeAclsResponseAclDescription, length)
						for i := int32(0); i < length; i++ {
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Host = val
								}
							}
							// Operation
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Resources[i].Acls[i].Operation = val
							}
							// PermissionType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Resources[i].Acls[i].PermissionType = val
							}
						}
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Resources = make([]DescribeAclsResponseDescribeAclsResource, length)
			for i := int32(0); i < length; i++ {
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Resources[i].ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Resources[i].ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Resources[i].ResourceName = val
					}
				}
				// PatternType
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Resources[i].PatternType = val
				}
				// Acls
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
						m.Resources[i].Acls = make([]DescribeAclsResponseAclDescription, length)
						for i := int32(0); i < length; i++ {
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Host = val
								}
							}
							// Operation
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Resources[i].Acls[i].Operation = val
							}
							// PermissionType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Resources[i].Acls[i].PermissionType = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Resources[i].Acls = make([]DescribeAclsResponseAclDescription, length)
						for i := int32(0); i < length; i++ {
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Acls[i].Host = val
								}
							}
							// Operation
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Resources[i].Acls[i].Operation = val
							}
							// PermissionType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Resources[i].Acls[i].PermissionType = val
							}
						}
					}
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

// DescribeAclsResponseDescribeAclsResource represents Each Resource that is referenced in an ACL..
type DescribeAclsResponseDescribeAclsResource struct {
	// The resource type.
	ResourceType int8 `json:"resourcetype" versions:"0-999"`
	// The resource name.
	ResourceName string `json:"resourcename" versions:"0-999"`
	// The resource pattern type.
	PatternType int8 `json:"patterntype" versions:"1-999"`
	// The ACLs.
	Acls []DescribeAclsResponseAclDescription `json:"acls" versions:"0-999"`
}

// DescribeAclsResponseAclDescription represents The ACLs..
type DescribeAclsResponseAclDescription struct {
	// The ACL principal.
	Principal string `json:"principal" versions:"0-999"`
	// The ACL host.
	Host string `json:"host" versions:"0-999"`
	// The ACL operation.
	Operation int8 `json:"operation" versions:"0-999"`
	// The ACL permission type.
	PermissionType int8 `json:"permissiontype" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeAclsResponse.
func (m *DescribeAclsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeAclsResponse.
func (m *DescribeAclsResponse) readTaggedFields(r io.Reader, version int16) error {
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
