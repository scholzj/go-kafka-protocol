package describeclientquotas

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeClientQuotasResponseApiKey        = 48
	DescribeClientQuotasResponseHeaderVersion = 1
)

// DescribeClientQuotasResponse represents a response message.
type DescribeClientQuotasResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The error code, or `0` if the quota description succeeded.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or `null` if the quota description succeeded.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// A result entry.
	Entries []DescribeClientQuotasResponseEntryData `json:"entries" versions:"0-999"`
}

// Encode encodes a DescribeClientQuotasResponse to a byte slice for the given version.
func (m *DescribeClientQuotasResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeClientQuotasResponse from a byte slice for the given version.
func (m *DescribeClientQuotasResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeClientQuotasResponse to an io.Writer for the given version.
func (m *DescribeClientQuotasResponse) Write(w io.Writer, version int16) error {
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
	// Entries
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DescribeClientQuotasResponseEntryData)
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
			// Values
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Values) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Values))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Values {
					// Key
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Values[i].Key); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Values[i].Key); err != nil {
								return nil, err
							}
						}
					}
					// Value
					if version >= 0 && version <= 999 {
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
		if m.Entries == nil {
			if isFlexible {
				if err := protocol.WriteVaruint32(w, 0); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteInt32(w, -1); err != nil {
					return err
				}
			}
		} else {
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
	}
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a DescribeClientQuotasResponse from an io.Reader for the given version.
func (m *DescribeClientQuotasResponse) Read(r io.Reader, version int16) error {
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
	// Entries
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DescribeClientQuotasResponseEntryData
			elemR := bytes.NewReader(data)
			// Entity
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// Values
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
			if lengthUint == 0 {
				m.Entries = nil
				return nil
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
				var tempElem DescribeClientQuotasResponseEntryData
				// Entity
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeClientQuotasResponseEntityData
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
							var tempElem DescribeClientQuotasResponseEntityData
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
						tempElem.Entity = make([]DescribeClientQuotasResponseEntityData, len(decoded))
						for i, item := range decoded {
							tempElem.Entity[i] = item.(DescribeClientQuotasResponseEntityData)
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
							var tempElem DescribeClientQuotasResponseEntityData
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
						tempElem.Entity = make([]DescribeClientQuotasResponseEntityData, len(decoded))
						for i, item := range decoded {
							tempElem.Entity[i] = item.(DescribeClientQuotasResponseEntityData)
						}
					}
				}
				// Values
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeClientQuotasResponseValueData
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
							var tempElem DescribeClientQuotasResponseValueData
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
						tempElem.Values = make([]DescribeClientQuotasResponseValueData, len(decoded))
						for i, item := range decoded {
							tempElem.Values[i] = item.(DescribeClientQuotasResponseValueData)
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
							var tempElem DescribeClientQuotasResponseValueData
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
						tempElem.Values = make([]DescribeClientQuotasResponseValueData, len(decoded))
						for i, item := range decoded {
							tempElem.Values[i] = item.(DescribeClientQuotasResponseValueData)
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
				// Values
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Values) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Values))); err != nil {
							return err
						}
					}
					for i := range tempElem.Values {
						// Key
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Values[i].Key); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Values[i].Key); err != nil {
									return err
								}
							}
						}
						// Value
						if version >= 0 && version <= 999 {
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
			m.Entries = make([]DescribeClientQuotasResponseEntryData, len(decoded))
			for i, item := range decoded {
				m.Entries[i] = item.(DescribeClientQuotasResponseEntryData)
			}
		} else {
			length, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			if length == -1 {
				m.Entries = nil
				return nil
			}
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem DescribeClientQuotasResponseEntryData
				// Entity
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeClientQuotasResponseEntityData
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
							var tempElem DescribeClientQuotasResponseEntityData
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
						tempElem.Entity = make([]DescribeClientQuotasResponseEntityData, len(decoded))
						for i, item := range decoded {
							tempElem.Entity[i] = item.(DescribeClientQuotasResponseEntityData)
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
							var tempElem DescribeClientQuotasResponseEntityData
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
						tempElem.Entity = make([]DescribeClientQuotasResponseEntityData, len(decoded))
						for i, item := range decoded {
							tempElem.Entity[i] = item.(DescribeClientQuotasResponseEntityData)
						}
					}
				}
				// Values
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeClientQuotasResponseValueData
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
							var tempElem DescribeClientQuotasResponseValueData
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
						tempElem.Values = make([]DescribeClientQuotasResponseValueData, len(decoded))
						for i, item := range decoded {
							tempElem.Values[i] = item.(DescribeClientQuotasResponseValueData)
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
							var tempElem DescribeClientQuotasResponseValueData
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
						tempElem.Values = make([]DescribeClientQuotasResponseValueData, len(decoded))
						for i, item := range decoded {
							tempElem.Values[i] = item.(DescribeClientQuotasResponseValueData)
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
				// Values
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Values) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Values))); err != nil {
							return err
						}
					}
					for i := range tempElem.Values {
						// Key
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Values[i].Key); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Values[i].Key); err != nil {
									return err
								}
							}
						}
						// Value
						if version >= 0 && version <= 999 {
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
			m.Entries = make([]DescribeClientQuotasResponseEntryData, len(decoded))
			for i, item := range decoded {
				m.Entries[i] = item.(DescribeClientQuotasResponseEntryData)
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

// DescribeClientQuotasResponseEntryData represents A result entry..
type DescribeClientQuotasResponseEntryData struct {
	// The quota entity description.
	Entity []DescribeClientQuotasResponseEntityData `json:"entity" versions:"0-999"`
	// The quota values for the entity.
	Values []DescribeClientQuotasResponseValueData `json:"values" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeClientQuotasResponseEntryData.
func (m *DescribeClientQuotasResponseEntryData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeClientQuotasResponseEntryData.
func (m *DescribeClientQuotasResponseEntryData) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeClientQuotasResponseEntityData represents The quota entity description..
type DescribeClientQuotasResponseEntityData struct {
	// The entity type.
	EntityType string `json:"entitytype" versions:"0-999"`
	// The entity name, or null if the default.
	EntityName *string `json:"entityname" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeClientQuotasResponseEntityData.
func (m *DescribeClientQuotasResponseEntityData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeClientQuotasResponseEntityData.
func (m *DescribeClientQuotasResponseEntityData) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeClientQuotasResponseValueData represents The quota values for the entity..
type DescribeClientQuotasResponseValueData struct {
	// The quota configuration key.
	Key string `json:"key" versions:"0-999"`
	// The quota configuration value.
	Value float64 `json:"value" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeClientQuotasResponseValueData.
func (m *DescribeClientQuotasResponseValueData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeClientQuotasResponseValueData.
func (m *DescribeClientQuotasResponseValueData) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for DescribeClientQuotasResponse.
func (m *DescribeClientQuotasResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeClientQuotasResponse.
func (m *DescribeClientQuotasResponse) readTaggedFields(r io.Reader, version int16) error {
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
