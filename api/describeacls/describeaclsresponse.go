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
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DescribeAclsResponseDescribeAclsResource)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// ResourceType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.ResourceType); err != nil {
					return nil, err
				}
			}
			// ResourceName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.ResourceName); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.ResourceName); err != nil {
						return nil, err
					}
				}
			}
			// PatternType
			if version >= 1 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.PatternType); err != nil {
					return nil, err
				}
			}
			// Acls
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Acls) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Acls))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Acls {
					// Principal
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Acls[i].Principal); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Acls[i].Principal); err != nil {
								return nil, err
							}
						}
					}
					// Host
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Acls[i].Host); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Acls[i].Host); err != nil {
								return nil, err
							}
						}
					}
					// Operation
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(elemW, structItem.Acls[i].Operation); err != nil {
							return nil, err
						}
					}
					// PermissionType
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(elemW, structItem.Acls[i].PermissionType); err != nil {
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
		items := make([]interface{}, len(m.Resources))
		for i := range m.Resources {
			items[i] = m.Resources[i]
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
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DescribeAclsResponseDescribeAclsResource
			elemR := bytes.NewReader(data)
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
			// Acls
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
				var tempElem DescribeAclsResponseDescribeAclsResource
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
				// Acls
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeAclsResponseAclDescription
						elemR := bytes.NewReader(data)
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
							var tempElem DescribeAclsResponseAclDescription
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
						tempElem.Acls = make([]DescribeAclsResponseAclDescription, len(decoded))
						for i, item := range decoded {
							tempElem.Acls[i] = item.(DescribeAclsResponseAclDescription)
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
							var tempElem DescribeAclsResponseAclDescription
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
						tempElem.Acls = make([]DescribeAclsResponseAclDescription, len(decoded))
						for i, item := range decoded {
							tempElem.Acls[i] = item.(DescribeAclsResponseAclDescription)
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
				// Acls
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Acls) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Acls))); err != nil {
							return err
						}
					}
					for i := range tempElem.Acls {
						// Principal
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Acls[i].Principal); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Acls[i].Principal); err != nil {
									return err
								}
							}
						}
						// Host
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Acls[i].Host); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Acls[i].Host); err != nil {
									return err
								}
							}
						}
						// Operation
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.Acls[i].Operation); err != nil {
								return err
							}
						}
						// PermissionType
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.Acls[i].PermissionType); err != nil {
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
			m.Resources = make([]DescribeAclsResponseDescribeAclsResource, len(decoded))
			for i, item := range decoded {
				m.Resources[i] = item.(DescribeAclsResponseDescribeAclsResource)
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
				var tempElem DescribeAclsResponseDescribeAclsResource
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
				// Acls
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeAclsResponseAclDescription
						elemR := bytes.NewReader(data)
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
							var tempElem DescribeAclsResponseAclDescription
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
						tempElem.Acls = make([]DescribeAclsResponseAclDescription, len(decoded))
						for i, item := range decoded {
							tempElem.Acls[i] = item.(DescribeAclsResponseAclDescription)
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
							var tempElem DescribeAclsResponseAclDescription
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
						tempElem.Acls = make([]DescribeAclsResponseAclDescription, len(decoded))
						for i, item := range decoded {
							tempElem.Acls[i] = item.(DescribeAclsResponseAclDescription)
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
				// Acls
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Acls) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Acls))); err != nil {
							return err
						}
					}
					for i := range tempElem.Acls {
						// Principal
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Acls[i].Principal); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Acls[i].Principal); err != nil {
									return err
								}
							}
						}
						// Host
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Acls[i].Host); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Acls[i].Host); err != nil {
									return err
								}
							}
						}
						// Operation
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.Acls[i].Operation); err != nil {
								return err
							}
						}
						// PermissionType
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.Acls[i].PermissionType); err != nil {
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
			m.Resources = make([]DescribeAclsResponseDescribeAclsResource, len(decoded))
			for i, item := range decoded {
				m.Resources[i] = item.(DescribeAclsResponseDescribeAclsResource)
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
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeAclsResponseDescribeAclsResource.
func (m *DescribeAclsResponseDescribeAclsResource) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeAclsResponseDescribeAclsResource.
func (m *DescribeAclsResponseDescribeAclsResource) readTaggedFields(r io.Reader, version int16) error {
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
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeAclsResponseAclDescription.
func (m *DescribeAclsResponseAclDescription) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeAclsResponseAclDescription.
func (m *DescribeAclsResponseAclDescription) readTaggedFields(r io.Reader, version int16) error {
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
