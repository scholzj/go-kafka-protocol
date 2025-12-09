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
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DeleteAclsResponseDeleteAclsFilterResult)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ErrorCode); err != nil {
					return nil, err
				}
			}
			// ErrorMessage
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.ErrorMessage); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.ErrorMessage); err != nil {
						return nil, err
					}
				}
			}
			// MatchingAcls
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.MatchingAcls) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.MatchingAcls))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.MatchingAcls {
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(elemW, structItem.MatchingAcls[i].ErrorCode); err != nil {
							return nil, err
						}
					}
					// ErrorMessage
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.MatchingAcls[i].ErrorMessage); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.MatchingAcls[i].ErrorMessage); err != nil {
								return nil, err
							}
						}
					}
					// ResourceType
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(elemW, structItem.MatchingAcls[i].ResourceType); err != nil {
							return nil, err
						}
					}
					// ResourceName
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.MatchingAcls[i].ResourceName); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.MatchingAcls[i].ResourceName); err != nil {
								return nil, err
							}
						}
					}
					// PatternType
					if version >= 1 && version <= 999 {
						if err := protocol.WriteInt8(elemW, structItem.MatchingAcls[i].PatternType); err != nil {
							return nil, err
						}
					}
					// Principal
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.MatchingAcls[i].Principal); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.MatchingAcls[i].Principal); err != nil {
								return nil, err
							}
						}
					}
					// Host
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.MatchingAcls[i].Host); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.MatchingAcls[i].Host); err != nil {
								return nil, err
							}
						}
					}
					// Operation
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(elemW, structItem.MatchingAcls[i].Operation); err != nil {
							return nil, err
						}
					}
					// PermissionType
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(elemW, structItem.MatchingAcls[i].PermissionType); err != nil {
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
		items := make([]interface{}, len(m.FilterResults))
		for i := range m.FilterResults {
			items[i] = m.FilterResults[i]
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
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DeleteAclsResponseDeleteAclsFilterResult
			elemR := bytes.NewReader(data)
			// ErrorCode
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ErrorCode = val
			}
			// ErrorMessage
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ErrorMessage = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ErrorMessage = val
				}
			}
			// MatchingAcls
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
				var tempElem DeleteAclsResponseDeleteAclsFilterResult
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					}
				}
				// MatchingAcls
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DeleteAclsResponseDeleteAclsMatchingAcl
						elemR := bytes.NewReader(data)
						// ErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ErrorCode = val
						}
						// ErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ErrorMessage = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ErrorMessage = val
							}
						}
						// ResourceType
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ResourceType = val
						}
						// ResourceName
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ResourceName = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ResourceName = val
							}
						}
						// PatternType
						if version >= 1 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PatternType = val
						}
						// Principal
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Principal = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Principal = val
							}
						}
						// Host
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Host = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Host = val
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
							var tempElem DeleteAclsResponseDeleteAclsMatchingAcl
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ResourceType = val
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ResourceName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ResourceName = val
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.PatternType = val
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
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
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ResourceType); err != nil {
									return err
								}
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ResourceName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ResourceName); err != nil {
										return err
									}
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.PatternType); err != nil {
									return err
								}
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Principal); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Principal); err != nil {
										return err
									}
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
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
						tempElem.MatchingAcls = make([]DeleteAclsResponseDeleteAclsMatchingAcl, len(decoded))
						for i, item := range decoded {
							tempElem.MatchingAcls[i] = item.(DeleteAclsResponseDeleteAclsMatchingAcl)
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
							var tempElem DeleteAclsResponseDeleteAclsMatchingAcl
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ResourceType = val
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ResourceName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ResourceName = val
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.PatternType = val
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
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
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ResourceType); err != nil {
									return err
								}
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ResourceName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ResourceName); err != nil {
										return err
									}
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.PatternType); err != nil {
									return err
								}
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Principal); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Principal); err != nil {
										return err
									}
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
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
						tempElem.MatchingAcls = make([]DeleteAclsResponseDeleteAclsMatchingAcl, len(decoded))
						for i, item := range decoded {
							tempElem.MatchingAcls[i] = item.(DeleteAclsResponseDeleteAclsMatchingAcl)
						}
					}
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					}
				}
				// MatchingAcls
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.MatchingAcls) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.MatchingAcls))); err != nil {
							return err
						}
					}
					for i := range tempElem.MatchingAcls {
						// ErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.MatchingAcls[i].ErrorCode); err != nil {
								return err
							}
						}
						// ErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.MatchingAcls[i].ErrorMessage); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.MatchingAcls[i].ErrorMessage); err != nil {
									return err
								}
							}
						}
						// ResourceType
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.MatchingAcls[i].ResourceType); err != nil {
								return err
							}
						}
						// ResourceName
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.MatchingAcls[i].ResourceName); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.MatchingAcls[i].ResourceName); err != nil {
									return err
								}
							}
						}
						// PatternType
						if version >= 1 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.MatchingAcls[i].PatternType); err != nil {
								return err
							}
						}
						// Principal
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.MatchingAcls[i].Principal); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.MatchingAcls[i].Principal); err != nil {
									return err
								}
							}
						}
						// Host
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.MatchingAcls[i].Host); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.MatchingAcls[i].Host); err != nil {
									return err
								}
							}
						}
						// Operation
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.MatchingAcls[i].Operation); err != nil {
								return err
							}
						}
						// PermissionType
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.MatchingAcls[i].PermissionType); err != nil {
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
			m.FilterResults = make([]DeleteAclsResponseDeleteAclsFilterResult, len(decoded))
			for i, item := range decoded {
				m.FilterResults[i] = item.(DeleteAclsResponseDeleteAclsFilterResult)
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
				var tempElem DeleteAclsResponseDeleteAclsFilterResult
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					}
				}
				// MatchingAcls
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DeleteAclsResponseDeleteAclsMatchingAcl
						elemR := bytes.NewReader(data)
						// ErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ErrorCode = val
						}
						// ErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ErrorMessage = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ErrorMessage = val
							}
						}
						// ResourceType
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ResourceType = val
						}
						// ResourceName
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ResourceName = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ResourceName = val
							}
						}
						// PatternType
						if version >= 1 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PatternType = val
						}
						// Principal
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Principal = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Principal = val
							}
						}
						// Host
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Host = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Host = val
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
							var tempElem DeleteAclsResponseDeleteAclsMatchingAcl
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ResourceType = val
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ResourceName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ResourceName = val
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.PatternType = val
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
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
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ResourceType); err != nil {
									return err
								}
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ResourceName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ResourceName); err != nil {
										return err
									}
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.PatternType); err != nil {
									return err
								}
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Principal); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Principal); err != nil {
										return err
									}
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
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
						tempElem.MatchingAcls = make([]DeleteAclsResponseDeleteAclsMatchingAcl, len(decoded))
						for i, item := range decoded {
							tempElem.MatchingAcls[i] = item.(DeleteAclsResponseDeleteAclsMatchingAcl)
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
							var tempElem DeleteAclsResponseDeleteAclsMatchingAcl
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ResourceType = val
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ResourceName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ResourceName = val
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.PatternType = val
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Principal = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Principal = val
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Host = val
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
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								}
							}
							// ResourceType
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ResourceType); err != nil {
									return err
								}
							}
							// ResourceName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ResourceName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ResourceName); err != nil {
										return err
									}
								}
							}
							// PatternType
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.PatternType); err != nil {
									return err
								}
							}
							// Principal
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Principal); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Principal); err != nil {
										return err
									}
								}
							}
							// Host
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
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
						tempElem.MatchingAcls = make([]DeleteAclsResponseDeleteAclsMatchingAcl, len(decoded))
						for i, item := range decoded {
							tempElem.MatchingAcls[i] = item.(DeleteAclsResponseDeleteAclsMatchingAcl)
						}
					}
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					}
				}
				// MatchingAcls
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.MatchingAcls) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.MatchingAcls))); err != nil {
							return err
						}
					}
					for i := range tempElem.MatchingAcls {
						// ErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.MatchingAcls[i].ErrorCode); err != nil {
								return err
							}
						}
						// ErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.MatchingAcls[i].ErrorMessage); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.MatchingAcls[i].ErrorMessage); err != nil {
									return err
								}
							}
						}
						// ResourceType
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.MatchingAcls[i].ResourceType); err != nil {
								return err
							}
						}
						// ResourceName
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.MatchingAcls[i].ResourceName); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.MatchingAcls[i].ResourceName); err != nil {
									return err
								}
							}
						}
						// PatternType
						if version >= 1 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.MatchingAcls[i].PatternType); err != nil {
								return err
							}
						}
						// Principal
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.MatchingAcls[i].Principal); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.MatchingAcls[i].Principal); err != nil {
									return err
								}
							}
						}
						// Host
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.MatchingAcls[i].Host); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.MatchingAcls[i].Host); err != nil {
									return err
								}
							}
						}
						// Operation
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.MatchingAcls[i].Operation); err != nil {
								return err
							}
						}
						// PermissionType
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.MatchingAcls[i].PermissionType); err != nil {
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
			m.FilterResults = make([]DeleteAclsResponseDeleteAclsFilterResult, len(decoded))
			for i, item := range decoded {
				m.FilterResults[i] = item.(DeleteAclsResponseDeleteAclsFilterResult)
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
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DeleteAclsResponseDeleteAclsFilterResult.
func (m *DeleteAclsResponseDeleteAclsFilterResult) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DeleteAclsResponseDeleteAclsFilterResult.
func (m *DeleteAclsResponseDeleteAclsFilterResult) readTaggedFields(r io.Reader, version int16) error {
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
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DeleteAclsResponseDeleteAclsMatchingAcl.
func (m *DeleteAclsResponseDeleteAclsMatchingAcl) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DeleteAclsResponseDeleteAclsMatchingAcl.
func (m *DeleteAclsResponseDeleteAclsMatchingAcl) readTaggedFields(r io.Reader, version int16) error {
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
