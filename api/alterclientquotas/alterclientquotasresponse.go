package alterclientquotas

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AlterClientQuotasResponseApiKey        = 49
	AlterClientQuotasResponseHeaderVersion = 1
)

// AlterClientQuotasResponse represents a response message.
type AlterClientQuotasResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The quota configuration entries to alter.
	Entries []AlterClientQuotasResponseEntryData `json:"entries" versions:"0-999"`
}

// Encode encodes a AlterClientQuotasResponse to a byte slice for the given version.
func (m *AlterClientQuotasResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AlterClientQuotasResponse from a byte slice for the given version.
func (m *AlterClientQuotasResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AlterClientQuotasResponse to an io.Writer for the given version.
func (m *AlterClientQuotasResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// Entries
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Entries) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Entries))); err != nil {
				return err
			}
		}
		for i := range m.Entries {
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Entries[i].ErrorCode); err != nil {
					return err
				}
			}
			// ErrorMessage
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Entries[i].ErrorMessage); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Entries[i].ErrorMessage); err != nil {
						return err
					}
				}
			}
			// Entity
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Entries[i].Entity) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Entries[i].Entity))); err != nil {
						return err
					}
				}
				for i := range m.Entries[i].Entity {
					// EntityType
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Entries[i].Entity[i].EntityType); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Entries[i].Entity[i].EntityType); err != nil {
								return err
							}
						}
					}
					// EntityName
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Entries[i].Entity[i].EntityName); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Entries[i].Entity[i].EntityName); err != nil {
								return err
							}
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

// Read reads a AlterClientQuotasResponse from an io.Reader for the given version.
func (m *AlterClientQuotasResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
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
	// Entries
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
			m.Entries = make([]AlterClientQuotasResponseEntryData, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Entries[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Entries[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Entries[i].ErrorMessage = val
					}
				}
				// Entity
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
						m.Entries[i].Entity = make([]AlterClientQuotasResponseEntityData, length)
						for i := int32(0); i < length; i++ {
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityType = val
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityName = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityName = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Entries[i].Entity = make([]AlterClientQuotasResponseEntityData, length)
						for i := int32(0); i < length; i++ {
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityType = val
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityName = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityName = val
								}
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
			m.Entries = make([]AlterClientQuotasResponseEntryData, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Entries[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Entries[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Entries[i].ErrorMessage = val
					}
				}
				// Entity
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
						m.Entries[i].Entity = make([]AlterClientQuotasResponseEntityData, length)
						for i := int32(0); i < length; i++ {
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityType = val
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityName = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityName = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Entries[i].Entity = make([]AlterClientQuotasResponseEntityData, length)
						for i := int32(0); i < length; i++ {
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityType = val
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityName = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Entity[i].EntityName = val
								}
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

// AlterClientQuotasResponseEntryData represents The quota configuration entries to alter..
type AlterClientQuotasResponseEntryData struct {
	// The error code, or `0` if the quota alteration succeeded.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or `null` if the quota alteration succeeded.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The quota entity to alter.
	Entity []AlterClientQuotasResponseEntityData `json:"entity" versions:"0-999"`
}

// AlterClientQuotasResponseEntityData represents The quota entity to alter..
type AlterClientQuotasResponseEntityData struct {
	// The entity type.
	EntityType string `json:"entitytype" versions:"0-999"`
	// The name of the entity, or null if the default.
	EntityName *string `json:"entityname" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for AlterClientQuotasResponse.
func (m *AlterClientQuotasResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterClientQuotasResponse.
func (m *AlterClientQuotasResponse) readTaggedFields(r io.Reader, version int16) error {
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
