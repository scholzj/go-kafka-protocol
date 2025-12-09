package describeuserscramcredentials

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeUserScramCredentialsResponseApiKey        = 50
	DescribeUserScramCredentialsResponseHeaderVersion = 1
)

// DescribeUserScramCredentialsResponse represents a response message.
type DescribeUserScramCredentialsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The message-level error code, 0 except for user authorization or infrastructure issues.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The message-level error message, if any.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The results for descriptions, one per user.
	Results []DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult `json:"results" versions:"0-999"`
}

// Encode encodes a DescribeUserScramCredentialsResponse to a byte slice for the given version.
func (m *DescribeUserScramCredentialsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeUserScramCredentialsResponse from a byte slice for the given version.
func (m *DescribeUserScramCredentialsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeUserScramCredentialsResponse to an io.Writer for the given version.
func (m *DescribeUserScramCredentialsResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
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
	// Results
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// User
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.User); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.User); err != nil {
						return nil, err
					}
				}
			}
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
			// CredentialInfos
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.CredentialInfos) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.CredentialInfos))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.CredentialInfos {
					// Mechanism
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(elemW, structItem.CredentialInfos[i].Mechanism); err != nil {
							return nil, err
						}
					}
					// Iterations
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.CredentialInfos[i].Iterations); err != nil {
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
		items := make([]interface{}, len(m.Results))
		for i := range m.Results {
			items[i] = m.Results[i]
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

// Read reads a DescribeUserScramCredentialsResponse from an io.Reader for the given version.
func (m *DescribeUserScramCredentialsResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
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
	// Results
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult
			elemR := bytes.NewReader(data)
			// User
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.User = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.User = val
				}
			}
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
			// CredentialInfos
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
				var tempElem DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult
				// User
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.User = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.User = val
					}
				}
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
				// CredentialInfos
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeUserScramCredentialsResponseCredentialInfo
						elemR := bytes.NewReader(data)
						// Mechanism
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Mechanism = val
						}
						// Iterations
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Iterations = val
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
							var tempElem DescribeUserScramCredentialsResponseCredentialInfo
							// Mechanism
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.Mechanism = val
							}
							// Iterations
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Iterations = val
							}
							// Mechanism
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.Mechanism); err != nil {
									return err
								}
							}
							// Iterations
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Iterations); err != nil {
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
						tempElem.CredentialInfos = make([]DescribeUserScramCredentialsResponseCredentialInfo, len(decoded))
						for i, item := range decoded {
							tempElem.CredentialInfos[i] = item.(DescribeUserScramCredentialsResponseCredentialInfo)
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
							var tempElem DescribeUserScramCredentialsResponseCredentialInfo
							// Mechanism
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.Mechanism = val
							}
							// Iterations
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Iterations = val
							}
							// Mechanism
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.Mechanism); err != nil {
									return err
								}
							}
							// Iterations
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Iterations); err != nil {
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
						tempElem.CredentialInfos = make([]DescribeUserScramCredentialsResponseCredentialInfo, len(decoded))
						for i, item := range decoded {
							tempElem.CredentialInfos[i] = item.(DescribeUserScramCredentialsResponseCredentialInfo)
						}
					}
				}
				// User
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.User); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.User); err != nil {
							return err
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
				// CredentialInfos
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.CredentialInfos) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.CredentialInfos))); err != nil {
							return err
						}
					}
					for i := range tempElem.CredentialInfos {
						// Mechanism
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.CredentialInfos[i].Mechanism); err != nil {
								return err
							}
						}
						// Iterations
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.CredentialInfos[i].Iterations); err != nil {
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
			m.Results = make([]DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult, len(decoded))
			for i, item := range decoded {
				m.Results[i] = item.(DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult)
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
				var tempElem DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult
				// User
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.User = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.User = val
					}
				}
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
				// CredentialInfos
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeUserScramCredentialsResponseCredentialInfo
						elemR := bytes.NewReader(data)
						// Mechanism
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Mechanism = val
						}
						// Iterations
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.Iterations = val
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
							var tempElem DescribeUserScramCredentialsResponseCredentialInfo
							// Mechanism
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.Mechanism = val
							}
							// Iterations
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Iterations = val
							}
							// Mechanism
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.Mechanism); err != nil {
									return err
								}
							}
							// Iterations
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Iterations); err != nil {
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
						tempElem.CredentialInfos = make([]DescribeUserScramCredentialsResponseCredentialInfo, len(decoded))
						for i, item := range decoded {
							tempElem.CredentialInfos[i] = item.(DescribeUserScramCredentialsResponseCredentialInfo)
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
							var tempElem DescribeUserScramCredentialsResponseCredentialInfo
							// Mechanism
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.Mechanism = val
							}
							// Iterations
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.Iterations = val
							}
							// Mechanism
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.Mechanism); err != nil {
									return err
								}
							}
							// Iterations
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Iterations); err != nil {
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
						tempElem.CredentialInfos = make([]DescribeUserScramCredentialsResponseCredentialInfo, len(decoded))
						for i, item := range decoded {
							tempElem.CredentialInfos[i] = item.(DescribeUserScramCredentialsResponseCredentialInfo)
						}
					}
				}
				// User
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.User); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.User); err != nil {
							return err
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
				// CredentialInfos
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.CredentialInfos) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.CredentialInfos))); err != nil {
							return err
						}
					}
					for i := range tempElem.CredentialInfos {
						// Mechanism
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.CredentialInfos[i].Mechanism); err != nil {
								return err
							}
						}
						// Iterations
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.CredentialInfos[i].Iterations); err != nil {
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
			m.Results = make([]DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult, len(decoded))
			for i, item := range decoded {
				m.Results[i] = item.(DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult)
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

// DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult represents The results for descriptions, one per user..
type DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult struct {
	// The user name.
	User string `json:"user" versions:"0-999"`
	// The user-level error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The user-level error message, if any.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The mechanism and related information associated with the user's SCRAM credentials.
	CredentialInfos []DescribeUserScramCredentialsResponseCredentialInfo `json:"credentialinfos" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult.
func (m *DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult.
func (m *DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeUserScramCredentialsResponseCredentialInfo represents The mechanism and related information associated with the user's SCRAM credentials..
type DescribeUserScramCredentialsResponseCredentialInfo struct {
	// The SCRAM mechanism.
	Mechanism int8 `json:"mechanism" versions:"0-999"`
	// The number of iterations used in the SCRAM credential.
	Iterations int32 `json:"iterations" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeUserScramCredentialsResponseCredentialInfo.
func (m *DescribeUserScramCredentialsResponseCredentialInfo) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeUserScramCredentialsResponseCredentialInfo.
func (m *DescribeUserScramCredentialsResponseCredentialInfo) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for DescribeUserScramCredentialsResponse.
func (m *DescribeUserScramCredentialsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeUserScramCredentialsResponse.
func (m *DescribeUserScramCredentialsResponse) readTaggedFields(r io.Reader, version int16) error {
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
