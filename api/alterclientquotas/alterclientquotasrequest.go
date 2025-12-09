package alterclientquotas

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AlterClientQuotasRequestApiKey        = 49
	AlterClientQuotasRequestHeaderVersion = 1
)

// AlterClientQuotasRequest represents a request message.
type AlterClientQuotasRequest struct {
	// The quota configuration entries to alter.
	Entries []AlterClientQuotasRequestEntryData `json:"entries" versions:"0-999"`
	// Whether the alteration should be validated, but not performed.
	ValidateOnly bool `json:"validateonly" versions:"0-999"`
}

// Encode encodes a AlterClientQuotasRequest to a byte slice for the given version.
func (m *AlterClientQuotasRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AlterClientQuotasRequest from a byte slice for the given version.
func (m *AlterClientQuotasRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AlterClientQuotasRequest to an io.Writer for the given version.
func (m *AlterClientQuotasRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
		isFlexible = true
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
			// Ops
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Entries[i].Ops) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Entries[i].Ops))); err != nil {
						return err
					}
				}
				for i := range m.Entries[i].Ops {
					// Key
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Entries[i].Ops[i].Key); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Entries[i].Ops[i].Key); err != nil {
								return err
							}
						}
					}
					// Value
					if version >= 0 && version <= 999 {
					}
					// Remove
					if version >= 0 && version <= 999 {
						if err := protocol.WriteBool(w, m.Entries[i].Ops[i].Remove); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	// ValidateOnly
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.ValidateOnly); err != nil {
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

// Read reads a AlterClientQuotasRequest from an io.Reader for the given version.
func (m *AlterClientQuotasRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
		isFlexible = true
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
			m.Entries = make([]AlterClientQuotasRequestEntryData, length)
			for i := int32(0); i < length; i++ {
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
						m.Entries[i].Entity = make([]AlterClientQuotasRequestEntityData, length)
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
						m.Entries[i].Entity = make([]AlterClientQuotasRequestEntityData, length)
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
				// Ops
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
						m.Entries[i].Ops = make([]AlterClientQuotasRequestOpData, length)
						for i := int32(0); i < length; i++ {
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Ops[i].Key = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Ops[i].Key = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
							}
							// Remove
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Entries[i].Ops[i].Remove = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Entries[i].Ops = make([]AlterClientQuotasRequestOpData, length)
						for i := int32(0); i < length; i++ {
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Ops[i].Key = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Ops[i].Key = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
							}
							// Remove
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Entries[i].Ops[i].Remove = val
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
			m.Entries = make([]AlterClientQuotasRequestEntryData, length)
			for i := int32(0); i < length; i++ {
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
						m.Entries[i].Entity = make([]AlterClientQuotasRequestEntityData, length)
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
						m.Entries[i].Entity = make([]AlterClientQuotasRequestEntityData, length)
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
				// Ops
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
						m.Entries[i].Ops = make([]AlterClientQuotasRequestOpData, length)
						for i := int32(0); i < length; i++ {
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Ops[i].Key = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Ops[i].Key = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
							}
							// Remove
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Entries[i].Ops[i].Remove = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Entries[i].Ops = make([]AlterClientQuotasRequestOpData, length)
						for i := int32(0); i < length; i++ {
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Ops[i].Key = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Entries[i].Ops[i].Key = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
							}
							// Remove
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Entries[i].Ops[i].Remove = val
							}
						}
					}
				}
			}
		}
	}
	// ValidateOnly
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.ValidateOnly = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// AlterClientQuotasRequestEntryData represents The quota configuration entries to alter..
type AlterClientQuotasRequestEntryData struct {
	// The quota entity to alter.
	Entity []AlterClientQuotasRequestEntityData `json:"entity" versions:"0-999"`
	// An individual quota configuration entry to alter.
	Ops []AlterClientQuotasRequestOpData `json:"ops" versions:"0-999"`
}

// AlterClientQuotasRequestEntityData represents The quota entity to alter..
type AlterClientQuotasRequestEntityData struct {
	// The entity type.
	EntityType string `json:"entitytype" versions:"0-999"`
	// The name of the entity, or null if the default.
	EntityName *string `json:"entityname" versions:"0-999"`
}

// AlterClientQuotasRequestOpData represents An individual quota configuration entry to alter..
type AlterClientQuotasRequestOpData struct {
	// The quota configuration key.
	Key string `json:"key" versions:"0-999"`
	// The value to set, otherwise ignored if the value is to be removed.
	Value float64 `json:"value" versions:"0-999"`
	// Whether the quota configuration value should be removed, otherwise set.
	Remove bool `json:"remove" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for AlterClientQuotasRequest.
func (m *AlterClientQuotasRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterClientQuotasRequest.
func (m *AlterClientQuotasRequest) readTaggedFields(r io.Reader, version int16) error {
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
