package deleteacls

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DeleteAclsResponseApiKey        = 31
	DeleteAclsResponseHeaderVersion = 1
)

// DeleteAclsResponse represents a response message.
type DeleteAclsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The results for each filter.
	FilterResults []DeleteAclsResponseDeleteAclsFilterResult `json:"filterresults" versions:"0-999"`
}

// Encode encodes a DeleteAclsResponse to a byte slice for the given version.
func (m *DeleteAclsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DeleteAclsResponse from a byte slice for the given version.
func (m *DeleteAclsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DeleteAclsResponse to an io.Writer for the given version.
func (m *DeleteAclsResponse) Write(w io.Writer, version int16) error {
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
	// FilterResults
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.FilterResults) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.FilterResults))); err != nil {
				return err
			}
		}
		for i := range m.FilterResults {
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.FilterResults[i].ErrorCode); err != nil {
					return err
				}
			}
			// ErrorMessage
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.FilterResults[i].ErrorMessage); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.FilterResults[i].ErrorMessage); err != nil {
						return err
					}
				}
			}
			// MatchingAcls
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.FilterResults[i].MatchingAcls) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.FilterResults[i].MatchingAcls))); err != nil {
						return err
					}
				}
				for i := range m.FilterResults[i].MatchingAcls {
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(w, m.FilterResults[i].MatchingAcls[i].ErrorCode); err != nil {
							return err
						}
					}
					// ErrorMessage
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.FilterResults[i].MatchingAcls[i].ErrorMessage); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.FilterResults[i].MatchingAcls[i].ErrorMessage); err != nil {
								return err
							}
						}
					}
					// ResourceType
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(w, m.FilterResults[i].MatchingAcls[i].ResourceType); err != nil {
							return err
						}
					}
					// ResourceName
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.FilterResults[i].MatchingAcls[i].ResourceName); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.FilterResults[i].MatchingAcls[i].ResourceName); err != nil {
								return err
							}
						}
					}
					// PatternType
					if version >= 1 && version <= 999 {
						if err := protocol.WriteInt8(w, m.FilterResults[i].MatchingAcls[i].PatternType); err != nil {
							return err
						}
					}
					// Principal
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.FilterResults[i].MatchingAcls[i].Principal); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.FilterResults[i].MatchingAcls[i].Principal); err != nil {
								return err
							}
						}
					}
					// Host
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.FilterResults[i].MatchingAcls[i].Host); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.FilterResults[i].MatchingAcls[i].Host); err != nil {
								return err
							}
						}
					}
					// Operation
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(w, m.FilterResults[i].MatchingAcls[i].Operation); err != nil {
							return err
						}
					}
					// PermissionType
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(w, m.FilterResults[i].MatchingAcls[i].PermissionType); err != nil {
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

// Read reads a DeleteAclsResponse from an io.Reader for the given version.
func (m *DeleteAclsResponse) Read(r io.Reader, version int16) error {
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
	// FilterResults
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
			m.FilterResults = make([]DeleteAclsResponseDeleteAclsFilterResult, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.FilterResults[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.FilterResults[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.FilterResults[i].ErrorMessage = val
					}
				}
				// MatchingAcls
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
						m.FilterResults[i].MatchingAcls = make([]DeleteAclsResponseDeleteAclsMatchingAcl, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ErrorMessage = val
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].ResourceType = val
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ResourceName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ResourceName = val
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].PatternType = val
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Host = val
								}
							}
							// Operation
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].Operation = val
							}
							// PermissionType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].PermissionType = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.FilterResults[i].MatchingAcls = make([]DeleteAclsResponseDeleteAclsMatchingAcl, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ErrorMessage = val
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].ResourceType = val
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ResourceName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ResourceName = val
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].PatternType = val
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Host = val
								}
							}
							// Operation
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].Operation = val
							}
							// PermissionType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].PermissionType = val
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
			m.FilterResults = make([]DeleteAclsResponseDeleteAclsFilterResult, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.FilterResults[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.FilterResults[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.FilterResults[i].ErrorMessage = val
					}
				}
				// MatchingAcls
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
						m.FilterResults[i].MatchingAcls = make([]DeleteAclsResponseDeleteAclsMatchingAcl, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ErrorMessage = val
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].ResourceType = val
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ResourceName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ResourceName = val
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].PatternType = val
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Host = val
								}
							}
							// Operation
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].Operation = val
							}
							// PermissionType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].PermissionType = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.FilterResults[i].MatchingAcls = make([]DeleteAclsResponseDeleteAclsMatchingAcl, length)
						for i := int32(0); i < length; i++ {
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ErrorMessage = val
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].ResourceType = val
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ResourceName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].ResourceName = val
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].PatternType = val
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.FilterResults[i].MatchingAcls[i].Host = val
								}
							}
							// Operation
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].Operation = val
							}
							// PermissionType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.FilterResults[i].MatchingAcls[i].PermissionType = val
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

// DeleteAclsResponseDeleteAclsFilterResult represents The results for each filter..
type DeleteAclsResponseDeleteAclsFilterResult struct {
	// The error code, or 0 if the filter succeeded.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if the filter succeeded.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The ACLs which matched this filter.
	MatchingAcls []DeleteAclsResponseDeleteAclsMatchingAcl `json:"matchingacls" versions:"0-999"`
}

// DeleteAclsResponseDeleteAclsMatchingAcl represents The ACLs which matched this filter..
type DeleteAclsResponseDeleteAclsMatchingAcl struct {
	// The deletion error code, or 0 if the deletion succeeded.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The deletion error message, or null if the deletion succeeded.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The ACL resource type.
	ResourceType int8 `json:"resourcetype" versions:"0-999"`
	// The ACL resource name.
	ResourceName string `json:"resourcename" versions:"0-999"`
	// The ACL resource pattern type.
	PatternType int8 `json:"patterntype" versions:"1-999"`
	// The ACL principal.
	Principal string `json:"principal" versions:"0-999"`
	// The ACL host.
	Host string `json:"host" versions:"0-999"`
	// The ACL operation.
	Operation int8 `json:"operation" versions:"0-999"`
	// The ACL permission type.
	PermissionType int8 `json:"permissiontype" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DeleteAclsResponse.
func (m *DeleteAclsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DeleteAclsResponse.
func (m *DeleteAclsResponse) readTaggedFields(r io.Reader, version int16) error {
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
