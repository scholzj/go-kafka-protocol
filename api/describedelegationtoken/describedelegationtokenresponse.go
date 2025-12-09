package describedelegationtoken

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeDelegationTokenResponseApiKey        = 41
	DescribeDelegationTokenResponseHeaderVersion = 1
)

// DescribeDelegationTokenResponse represents a response message.
type DescribeDelegationTokenResponse struct {
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The tokens.
	Tokens []DescribeDelegationTokenResponseDescribedDelegationToken `json:"tokens" versions:"0-999"`
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
}

// Encode encodes a DescribeDelegationTokenResponse to a byte slice for the given version.
func (m *DescribeDelegationTokenResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeDelegationTokenResponse from a byte slice for the given version.
func (m *DescribeDelegationTokenResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeDelegationTokenResponse to an io.Writer for the given version.
func (m *DescribeDelegationTokenResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ErrorCode
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// Tokens
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DescribeDelegationTokenResponseDescribedDelegationToken)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// PrincipalType
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.PrincipalType); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.PrincipalType); err != nil {
						return nil, err
					}
				}
			}
			// PrincipalName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.PrincipalName); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.PrincipalName); err != nil {
						return nil, err
					}
				}
			}
			// TokenRequesterPrincipalType
			if version >= 3 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.TokenRequesterPrincipalType); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.TokenRequesterPrincipalType); err != nil {
						return nil, err
					}
				}
			}
			// TokenRequesterPrincipalName
			if version >= 3 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.TokenRequesterPrincipalName); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.TokenRequesterPrincipalName); err != nil {
						return nil, err
					}
				}
			}
			// IssueTimestamp
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(elemW, structItem.IssueTimestamp); err != nil {
					return nil, err
				}
			}
			// ExpiryTimestamp
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(elemW, structItem.ExpiryTimestamp); err != nil {
					return nil, err
				}
			}
			// MaxTimestamp
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(elemW, structItem.MaxTimestamp); err != nil {
					return nil, err
				}
			}
			// TokenId
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.TokenId); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.TokenId); err != nil {
						return nil, err
					}
				}
			}
			// Hmac
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactBytes(elemW, structItem.Hmac); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteBytes(elemW, structItem.Hmac); err != nil {
						return nil, err
					}
				}
			}
			// Renewers
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Renewers) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Renewers))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Renewers {
					// PrincipalType
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Renewers[i].PrincipalType); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Renewers[i].PrincipalType); err != nil {
								return nil, err
							}
						}
					}
					// PrincipalName
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Renewers[i].PrincipalName); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Renewers[i].PrincipalName); err != nil {
								return nil, err
							}
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
		items := make([]interface{}, len(m.Tokens))
		for i := range m.Tokens {
			items[i] = m.Tokens[i]
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
	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
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

// Read reads a DescribeDelegationTokenResponse from an io.Reader for the given version.
func (m *DescribeDelegationTokenResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ErrorCode
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// Tokens
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DescribeDelegationTokenResponseDescribedDelegationToken
			elemR := bytes.NewReader(data)
			// PrincipalType
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.PrincipalType = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.PrincipalType = val
				}
			}
			// PrincipalName
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.PrincipalName = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.PrincipalName = val
				}
			}
			// TokenRequesterPrincipalType
			if version >= 3 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TokenRequesterPrincipalType = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TokenRequesterPrincipalType = val
				}
			}
			// TokenRequesterPrincipalName
			if version >= 3 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TokenRequesterPrincipalName = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TokenRequesterPrincipalName = val
				}
			}
			// IssueTimestamp
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt64(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.IssueTimestamp = val
			}
			// ExpiryTimestamp
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt64(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ExpiryTimestamp = val
			}
			// MaxTimestamp
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt64(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.MaxTimestamp = val
			}
			// TokenId
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TokenId = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TokenId = val
				}
			}
			// Hmac
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactBytes(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Hmac = val
				} else {
					val, err := protocol.ReadBytes(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Hmac = val
				}
			}
			// Renewers
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
				var tempElem DescribeDelegationTokenResponseDescribedDelegationToken
				// PrincipalType
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalType = val
					}
				}
				// PrincipalName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalName = val
					}
				}
				// TokenRequesterPrincipalType
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TokenRequesterPrincipalType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TokenRequesterPrincipalType = val
					}
				}
				// TokenRequesterPrincipalName
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TokenRequesterPrincipalName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TokenRequesterPrincipalName = val
					}
				}
				// IssueTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.IssueTimestamp = val
				}
				// ExpiryTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.ExpiryTimestamp = val
				}
				// MaxTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.MaxTimestamp = val
				}
				// TokenId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TokenId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TokenId = val
					}
				}
				// Hmac
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						tempElem.Hmac = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						tempElem.Hmac = val
					}
				}
				// Renewers
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeDelegationTokenResponseDescribedDelegationTokenRenewer
						elemR := bytes.NewReader(data)
						// PrincipalType
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PrincipalType = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PrincipalType = val
							}
						}
						// PrincipalName
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PrincipalName = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PrincipalName = val
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
							var tempElem DescribeDelegationTokenResponseDescribedDelegationTokenRenewer
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalType = val
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalName = val
								}
							}
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.PrincipalType); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.PrincipalType); err != nil {
										return err
									}
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.PrincipalName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.PrincipalName); err != nil {
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
						tempElem.Renewers = make([]DescribeDelegationTokenResponseDescribedDelegationTokenRenewer, len(decoded))
						for i, item := range decoded {
							tempElem.Renewers[i] = item.(DescribeDelegationTokenResponseDescribedDelegationTokenRenewer)
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
							var tempElem DescribeDelegationTokenResponseDescribedDelegationTokenRenewer
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalType = val
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalName = val
								}
							}
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.PrincipalType); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.PrincipalType); err != nil {
										return err
									}
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.PrincipalName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.PrincipalName); err != nil {
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
						tempElem.Renewers = make([]DescribeDelegationTokenResponseDescribedDelegationTokenRenewer, len(decoded))
						for i, item := range decoded {
							tempElem.Renewers[i] = item.(DescribeDelegationTokenResponseDescribedDelegationTokenRenewer)
						}
					}
				}
				// PrincipalType
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.PrincipalType); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.PrincipalType); err != nil {
							return err
						}
					}
				}
				// PrincipalName
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.PrincipalName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.PrincipalName); err != nil {
							return err
						}
					}
				}
				// TokenRequesterPrincipalType
				if version >= 3 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TokenRequesterPrincipalType); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TokenRequesterPrincipalType); err != nil {
							return err
						}
					}
				}
				// TokenRequesterPrincipalName
				if version >= 3 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TokenRequesterPrincipalName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TokenRequesterPrincipalName); err != nil {
							return err
						}
					}
				}
				// IssueTimestamp
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.IssueTimestamp); err != nil {
						return err
					}
				}
				// ExpiryTimestamp
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.ExpiryTimestamp); err != nil {
						return err
					}
				}
				// MaxTimestamp
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.MaxTimestamp); err != nil {
						return err
					}
				}
				// TokenId
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TokenId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TokenId); err != nil {
							return err
						}
					}
				}
				// Hmac
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactBytes(elemW, tempElem.Hmac); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteBytes(elemW, tempElem.Hmac); err != nil {
							return err
						}
					}
				}
				// Renewers
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Renewers) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Renewers))); err != nil {
							return err
						}
					}
					for i := range tempElem.Renewers {
						// PrincipalType
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Renewers[i].PrincipalType); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Renewers[i].PrincipalType); err != nil {
									return err
								}
							}
						}
						// PrincipalName
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Renewers[i].PrincipalName); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Renewers[i].PrincipalName); err != nil {
									return err
								}
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
			m.Tokens = make([]DescribeDelegationTokenResponseDescribedDelegationToken, len(decoded))
			for i, item := range decoded {
				m.Tokens[i] = item.(DescribeDelegationTokenResponseDescribedDelegationToken)
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
				var tempElem DescribeDelegationTokenResponseDescribedDelegationToken
				// PrincipalType
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalType = val
					}
				}
				// PrincipalName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.PrincipalName = val
					}
				}
				// TokenRequesterPrincipalType
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TokenRequesterPrincipalType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TokenRequesterPrincipalType = val
					}
				}
				// TokenRequesterPrincipalName
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TokenRequesterPrincipalName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TokenRequesterPrincipalName = val
					}
				}
				// IssueTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.IssueTimestamp = val
				}
				// ExpiryTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.ExpiryTimestamp = val
				}
				// MaxTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.MaxTimestamp = val
				}
				// TokenId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TokenId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TokenId = val
					}
				}
				// Hmac
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						tempElem.Hmac = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						tempElem.Hmac = val
					}
				}
				// Renewers
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeDelegationTokenResponseDescribedDelegationTokenRenewer
						elemR := bytes.NewReader(data)
						// PrincipalType
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PrincipalType = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PrincipalType = val
							}
						}
						// PrincipalName
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PrincipalName = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PrincipalName = val
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
							var tempElem DescribeDelegationTokenResponseDescribedDelegationTokenRenewer
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalType = val
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalName = val
								}
							}
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.PrincipalType); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.PrincipalType); err != nil {
										return err
									}
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.PrincipalName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.PrincipalName); err != nil {
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
						tempElem.Renewers = make([]DescribeDelegationTokenResponseDescribedDelegationTokenRenewer, len(decoded))
						for i, item := range decoded {
							tempElem.Renewers[i] = item.(DescribeDelegationTokenResponseDescribedDelegationTokenRenewer)
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
							var tempElem DescribeDelegationTokenResponseDescribedDelegationTokenRenewer
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalType = val
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.PrincipalName = val
								}
							}
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.PrincipalType); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.PrincipalType); err != nil {
										return err
									}
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.PrincipalName); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.PrincipalName); err != nil {
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
						tempElem.Renewers = make([]DescribeDelegationTokenResponseDescribedDelegationTokenRenewer, len(decoded))
						for i, item := range decoded {
							tempElem.Renewers[i] = item.(DescribeDelegationTokenResponseDescribedDelegationTokenRenewer)
						}
					}
				}
				// PrincipalType
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.PrincipalType); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.PrincipalType); err != nil {
							return err
						}
					}
				}
				// PrincipalName
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.PrincipalName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.PrincipalName); err != nil {
							return err
						}
					}
				}
				// TokenRequesterPrincipalType
				if version >= 3 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TokenRequesterPrincipalType); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TokenRequesterPrincipalType); err != nil {
							return err
						}
					}
				}
				// TokenRequesterPrincipalName
				if version >= 3 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TokenRequesterPrincipalName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TokenRequesterPrincipalName); err != nil {
							return err
						}
					}
				}
				// IssueTimestamp
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.IssueTimestamp); err != nil {
						return err
					}
				}
				// ExpiryTimestamp
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.ExpiryTimestamp); err != nil {
						return err
					}
				}
				// MaxTimestamp
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.MaxTimestamp); err != nil {
						return err
					}
				}
				// TokenId
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TokenId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TokenId); err != nil {
							return err
						}
					}
				}
				// Hmac
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactBytes(elemW, tempElem.Hmac); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteBytes(elemW, tempElem.Hmac); err != nil {
							return err
						}
					}
				}
				// Renewers
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Renewers) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Renewers))); err != nil {
							return err
						}
					}
					for i := range tempElem.Renewers {
						// PrincipalType
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Renewers[i].PrincipalType); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Renewers[i].PrincipalType); err != nil {
									return err
								}
							}
						}
						// PrincipalName
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Renewers[i].PrincipalName); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Renewers[i].PrincipalName); err != nil {
									return err
								}
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
			m.Tokens = make([]DescribeDelegationTokenResponseDescribedDelegationToken, len(decoded))
			for i, item := range decoded {
				m.Tokens[i] = item.(DescribeDelegationTokenResponseDescribedDelegationToken)
			}
		}
	}
	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// DescribeDelegationTokenResponseDescribedDelegationToken represents The tokens..
type DescribeDelegationTokenResponseDescribedDelegationToken struct {
	// The token principal type.
	PrincipalType string `json:"principaltype" versions:"0-999"`
	// The token principal name.
	PrincipalName string `json:"principalname" versions:"0-999"`
	// The principal type of the requester of the token.
	TokenRequesterPrincipalType string `json:"tokenrequesterprincipaltype" versions:"3-999"`
	// The principal type of the requester of the token.
	TokenRequesterPrincipalName string `json:"tokenrequesterprincipalname" versions:"3-999"`
	// The token issue timestamp in milliseconds.
	IssueTimestamp int64 `json:"issuetimestamp" versions:"0-999"`
	// The token expiry timestamp in milliseconds.
	ExpiryTimestamp int64 `json:"expirytimestamp" versions:"0-999"`
	// The token maximum timestamp length in milliseconds.
	MaxTimestamp int64 `json:"maxtimestamp" versions:"0-999"`
	// The token ID.
	TokenId string `json:"tokenid" versions:"0-999"`
	// The token HMAC.
	Hmac []byte `json:"hmac" versions:"0-999"`
	// Those who are able to renew this token before it expires.
	Renewers []DescribeDelegationTokenResponseDescribedDelegationTokenRenewer `json:"renewers" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeDelegationTokenResponseDescribedDelegationToken.
func (m *DescribeDelegationTokenResponseDescribedDelegationToken) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeDelegationTokenResponseDescribedDelegationToken.
func (m *DescribeDelegationTokenResponseDescribedDelegationToken) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeDelegationTokenResponseDescribedDelegationTokenRenewer represents Those who are able to renew this token before it expires..
type DescribeDelegationTokenResponseDescribedDelegationTokenRenewer struct {
	// The renewer principal type.
	PrincipalType string `json:"principaltype" versions:"0-999"`
	// The renewer principal name.
	PrincipalName string `json:"principalname" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeDelegationTokenResponseDescribedDelegationTokenRenewer.
func (m *DescribeDelegationTokenResponseDescribedDelegationTokenRenewer) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeDelegationTokenResponseDescribedDelegationTokenRenewer.
func (m *DescribeDelegationTokenResponseDescribedDelegationTokenRenewer) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for DescribeDelegationTokenResponse.
func (m *DescribeDelegationTokenResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeDelegationTokenResponse.
func (m *DescribeDelegationTokenResponse) readTaggedFields(r io.Reader, version int16) error {
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
