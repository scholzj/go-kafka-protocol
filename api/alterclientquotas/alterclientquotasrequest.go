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
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(AlterClientQuotasRequestEntryData)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Entity
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Entity) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Entity))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Entity {
					// EntityType
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Entity[i].EntityType); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Entity[i].EntityType); err != nil {
								return nil, err
							}
						}
					}
					// EntityName
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.Entity[i].EntityName); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.Entity[i].EntityName); err != nil {
								return nil, err
							}
						}
					}
				}
			}
			// Ops
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Ops) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Ops))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Ops {
					// Key
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Ops[i].Key); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Ops[i].Key); err != nil {
								return nil, err
							}
						}
					}
					// Value
					if version >= 0 && version <= 999 {
					}
					// Remove
					if version >= 0 && version <= 999 {
						if err := protocol.WriteBool(elemW, structItem.Ops[i].Remove); err != nil {
							return nil, err
						}
					}
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
		items := make([]interface{}, len(m.Entries))
		for i := range m.Entries {
			items[i] = m.Entries[i]
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
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem AlterClientQuotasRequestEntryData
			elemR := bytes.NewReader(data)
			// Entity
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// Ops
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
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
				var tempElem AlterClientQuotasRequestEntryData
				// Entity
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AlterClientQuotasRequestEntityData
						elemR := bytes.NewReader(data)
						// EntityType
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EntityType = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EntityType = val
							}
						}
						// EntityName
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EntityName = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EntityName = val
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
							var tempElem AlterClientQuotasRequestEntityData
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.EntityType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.EntityType = val
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.EntityName = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.EntityName = val
								}
							}
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.EntityType); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.EntityType); err != nil {
										return err
									}
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.EntityName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.EntityName); err != nil {
										return err
									}
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
						tempElem.Entity = make([]AlterClientQuotasRequestEntityData, len(decoded))
						for i, item := range decoded {
							tempElem.Entity[i] = item.(AlterClientQuotasRequestEntityData)
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
							var tempElem AlterClientQuotasRequestEntityData
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.EntityType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.EntityType = val
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.EntityName = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.EntityName = val
								}
							}
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.EntityType); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.EntityType); err != nil {
										return err
									}
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.EntityName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.EntityName); err != nil {
										return err
									}
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
						tempElem.Entity = make([]AlterClientQuotasRequestEntityData, len(decoded))
						for i, item := range decoded {
							tempElem.Entity[i] = item.(AlterClientQuotasRequestEntityData)
						}
					}
				}
				// Ops
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AlterClientQuotasRequestOpData
						elemR := bytes.NewReader(data)
						// Key
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Key = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Key = val
							}
						}
						// Value
						if version >= 0 && version <= 999 {
						}
						// Remove
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Remove = val
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
							var tempElem AlterClientQuotasRequestOpData
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Key = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Key = val
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
								tempElem.Remove = val
							}
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
										return err
									}
								}
							}
							// Value
							if version >= 0 && version <= 999 {
							}
							// Remove
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.Remove); err != nil {
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
						tempElem.Ops = make([]AlterClientQuotasRequestOpData, len(decoded))
						for i, item := range decoded {
							tempElem.Ops[i] = item.(AlterClientQuotasRequestOpData)
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
							var tempElem AlterClientQuotasRequestOpData
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Key = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Key = val
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
								tempElem.Remove = val
							}
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
										return err
									}
								}
							}
							// Value
							if version >= 0 && version <= 999 {
							}
							// Remove
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.Remove); err != nil {
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
						tempElem.Ops = make([]AlterClientQuotasRequestOpData, len(decoded))
						for i, item := range decoded {
							tempElem.Ops[i] = item.(AlterClientQuotasRequestOpData)
						}
					}
				}
				// Entity
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Entity) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Entity))); err != nil {
							return err
						}
					}
					for i := range tempElem.Entity {
						// EntityType
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Entity[i].EntityType); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Entity[i].EntityType); err != nil {
									return err
								}
							}
						}
						// EntityName
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Entity[i].EntityName); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Entity[i].EntityName); err != nil {
									return err
								}
							}
						}
					}
				}
				// Ops
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Ops) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Ops))); err != nil {
							return err
						}
					}
					for i := range tempElem.Ops {
						// Key
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Ops[i].Key); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Ops[i].Key); err != nil {
									return err
								}
							}
						}
						// Value
						if version >= 0 && version <= 999 {
						}
						// Remove
						if version >= 0 && version <= 999 {
							if err := protocol.WriteBool(elemW, tempElem.Ops[i].Remove); err != nil {
								return err
							}
						}
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
			m.Entries = make([]AlterClientQuotasRequestEntryData, len(decoded))
			for i, item := range decoded {
				m.Entries[i] = item.(AlterClientQuotasRequestEntryData)
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
				var tempElem AlterClientQuotasRequestEntryData
				// Entity
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AlterClientQuotasRequestEntityData
						elemR := bytes.NewReader(data)
						// EntityType
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EntityType = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EntityType = val
							}
						}
						// EntityName
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EntityName = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.EntityName = val
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
							var tempElem AlterClientQuotasRequestEntityData
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.EntityType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.EntityType = val
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.EntityName = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.EntityName = val
								}
							}
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.EntityType); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.EntityType); err != nil {
										return err
									}
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.EntityName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.EntityName); err != nil {
										return err
									}
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
						tempElem.Entity = make([]AlterClientQuotasRequestEntityData, len(decoded))
						for i, item := range decoded {
							tempElem.Entity[i] = item.(AlterClientQuotasRequestEntityData)
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
							var tempElem AlterClientQuotasRequestEntityData
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.EntityType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.EntityType = val
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.EntityName = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.EntityName = val
								}
							}
							// EntityType
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.EntityType); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.EntityType); err != nil {
										return err
									}
								}
							}
							// EntityName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.EntityName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.EntityName); err != nil {
										return err
									}
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
						tempElem.Entity = make([]AlterClientQuotasRequestEntityData, len(decoded))
						for i, item := range decoded {
							tempElem.Entity[i] = item.(AlterClientQuotasRequestEntityData)
						}
					}
				}
				// Ops
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AlterClientQuotasRequestOpData
						elemR := bytes.NewReader(data)
						// Key
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Key = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Key = val
							}
						}
						// Value
						if version >= 0 && version <= 999 {
						}
						// Remove
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Remove = val
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
							var tempElem AlterClientQuotasRequestOpData
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Key = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Key = val
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
								tempElem.Remove = val
							}
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
										return err
									}
								}
							}
							// Value
							if version >= 0 && version <= 999 {
							}
							// Remove
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.Remove); err != nil {
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
						tempElem.Ops = make([]AlterClientQuotasRequestOpData, len(decoded))
						for i, item := range decoded {
							tempElem.Ops[i] = item.(AlterClientQuotasRequestOpData)
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
							var tempElem AlterClientQuotasRequestOpData
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Key = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Key = val
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
								tempElem.Remove = val
							}
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
										return err
									}
								}
							}
							// Value
							if version >= 0 && version <= 999 {
							}
							// Remove
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.Remove); err != nil {
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
						tempElem.Ops = make([]AlterClientQuotasRequestOpData, len(decoded))
						for i, item := range decoded {
							tempElem.Ops[i] = item.(AlterClientQuotasRequestOpData)
						}
					}
				}
				// Entity
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Entity) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Entity))); err != nil {
							return err
						}
					}
					for i := range tempElem.Entity {
						// EntityType
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Entity[i].EntityType); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Entity[i].EntityType); err != nil {
									return err
								}
							}
						}
						// EntityName
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Entity[i].EntityName); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Entity[i].EntityName); err != nil {
									return err
								}
							}
						}
					}
				}
				// Ops
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Ops) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Ops))); err != nil {
							return err
						}
					}
					for i := range tempElem.Ops {
						// Key
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Ops[i].Key); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Ops[i].Key); err != nil {
									return err
								}
							}
						}
						// Value
						if version >= 0 && version <= 999 {
						}
						// Remove
						if version >= 0 && version <= 999 {
							if err := protocol.WriteBool(elemW, tempElem.Ops[i].Remove); err != nil {
								return err
							}
						}
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
			m.Entries = make([]AlterClientQuotasRequestEntryData, len(decoded))
			for i, item := range decoded {
				m.Entries[i] = item.(AlterClientQuotasRequestEntryData)
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
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AlterClientQuotasRequestEntryData.
func (m *AlterClientQuotasRequestEntryData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterClientQuotasRequestEntryData.
func (m *AlterClientQuotasRequestEntryData) readTaggedFields(r io.Reader, version int16) error {
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

// AlterClientQuotasRequestEntityData represents The quota entity to alter..
type AlterClientQuotasRequestEntityData struct {
	// The entity type.
	EntityType string `json:"entitytype" versions:"0-999"`
	// The name of the entity, or null if the default.
	EntityName *string `json:"entityname" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AlterClientQuotasRequestEntityData.
func (m *AlterClientQuotasRequestEntityData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterClientQuotasRequestEntityData.
func (m *AlterClientQuotasRequestEntityData) readTaggedFields(r io.Reader, version int16) error {
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

// AlterClientQuotasRequestOpData represents An individual quota configuration entry to alter..
type AlterClientQuotasRequestOpData struct {
	// The quota configuration key.
	Key string `json:"key" versions:"0-999"`
	// The value to set, otherwise ignored if the value is to be removed.
	Value float64 `json:"value" versions:"0-999"`
	// Whether the quota configuration value should be removed, otherwise set.
	Remove bool `json:"remove" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AlterClientQuotasRequestOpData.
func (m *AlterClientQuotasRequestOpData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterClientQuotasRequestOpData.
func (m *AlterClientQuotasRequestOpData) readTaggedFields(r io.Reader, version int16) error {
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
