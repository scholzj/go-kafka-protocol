package alteruserscramcredentials

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AlterUserScramCredentialsRequestApiKey        = 51
	AlterUserScramCredentialsRequestHeaderVersion = 1
)

// AlterUserScramCredentialsRequest represents a request message.
type AlterUserScramCredentialsRequest struct {
	// The SCRAM credentials to remove.
	Deletions []AlterUserScramCredentialsRequestScramCredentialDeletion `json:"deletions" versions:"0-999"`
	// The SCRAM credentials to update/insert.
	Upsertions []AlterUserScramCredentialsRequestScramCredentialUpsertion `json:"upsertions" versions:"0-999"`
}

// Encode encodes a AlterUserScramCredentialsRequest to a byte slice for the given version.
func (m *AlterUserScramCredentialsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AlterUserScramCredentialsRequest from a byte slice for the given version.
func (m *AlterUserScramCredentialsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AlterUserScramCredentialsRequest to an io.Writer for the given version.
func (m *AlterUserScramCredentialsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// Deletions
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(AlterUserScramCredentialsRequestScramCredentialDeletion)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				}
			}
			// Mechanism
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.Mechanism); err != nil {
					return nil, err
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
		items := make([]interface{}, len(m.Deletions))
		for i := range m.Deletions {
			items[i] = m.Deletions[i]
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
	// Upsertions
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(AlterUserScramCredentialsRequestScramCredentialUpsertion)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				}
			}
			// Mechanism
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.Mechanism); err != nil {
					return nil, err
				}
			}
			// Iterations
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.Iterations); err != nil {
					return nil, err
				}
			}
			// Salt
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactBytes(elemW, structItem.Salt); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteBytes(elemW, structItem.Salt); err != nil {
						return nil, err
					}
				}
			}
			// SaltedPassword
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactBytes(elemW, structItem.SaltedPassword); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteBytes(elemW, structItem.SaltedPassword); err != nil {
						return nil, err
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
		items := make([]interface{}, len(m.Upsertions))
		for i := range m.Upsertions {
			items[i] = m.Upsertions[i]
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

// Read reads a AlterUserScramCredentialsRequest from an io.Reader for the given version.
func (m *AlterUserScramCredentialsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// Deletions
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem AlterUserScramCredentialsRequestScramCredentialDeletion
			elemR := bytes.NewReader(data)
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				}
			}
			// Mechanism
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt8(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.Mechanism = val
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
				var tempElem AlterUserScramCredentialsRequestScramCredentialDeletion
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// Mechanism
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.Mechanism = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// Mechanism
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.Mechanism); err != nil {
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
			m.Deletions = make([]AlterUserScramCredentialsRequestScramCredentialDeletion, len(decoded))
			for i, item := range decoded {
				m.Deletions[i] = item.(AlterUserScramCredentialsRequestScramCredentialDeletion)
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
				var tempElem AlterUserScramCredentialsRequestScramCredentialDeletion
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// Mechanism
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.Mechanism = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// Mechanism
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.Mechanism); err != nil {
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
			m.Deletions = make([]AlterUserScramCredentialsRequestScramCredentialDeletion, len(decoded))
			for i, item := range decoded {
				m.Deletions[i] = item.(AlterUserScramCredentialsRequestScramCredentialDeletion)
			}
		}
	}
	// Upsertions
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem AlterUserScramCredentialsRequestScramCredentialUpsertion
			elemR := bytes.NewReader(data)
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				}
			}
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
			// Salt
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactBytes(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Salt = val
				} else {
					val, err := protocol.ReadBytes(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Salt = val
				}
			}
			// SaltedPassword
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactBytes(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.SaltedPassword = val
				} else {
					val, err := protocol.ReadBytes(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.SaltedPassword = val
				}
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
				var tempElem AlterUserScramCredentialsRequestScramCredentialUpsertion
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
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
				// Salt
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						tempElem.Salt = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						tempElem.Salt = val
					}
				}
				// SaltedPassword
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						tempElem.SaltedPassword = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						tempElem.SaltedPassword = val
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
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
				// Salt
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactBytes(elemW, tempElem.Salt); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteBytes(elemW, tempElem.Salt); err != nil {
							return err
						}
					}
				}
				// SaltedPassword
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactBytes(elemW, tempElem.SaltedPassword); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteBytes(elemW, tempElem.SaltedPassword); err != nil {
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
			m.Upsertions = make([]AlterUserScramCredentialsRequestScramCredentialUpsertion, len(decoded))
			for i, item := range decoded {
				m.Upsertions[i] = item.(AlterUserScramCredentialsRequestScramCredentialUpsertion)
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
				var tempElem AlterUserScramCredentialsRequestScramCredentialUpsertion
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
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
				// Salt
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						tempElem.Salt = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						tempElem.Salt = val
					}
				}
				// SaltedPassword
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						tempElem.SaltedPassword = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						tempElem.SaltedPassword = val
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
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
				// Salt
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactBytes(elemW, tempElem.Salt); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteBytes(elemW, tempElem.Salt); err != nil {
							return err
						}
					}
				}
				// SaltedPassword
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactBytes(elemW, tempElem.SaltedPassword); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteBytes(elemW, tempElem.SaltedPassword); err != nil {
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
			m.Upsertions = make([]AlterUserScramCredentialsRequestScramCredentialUpsertion, len(decoded))
			for i, item := range decoded {
				m.Upsertions[i] = item.(AlterUserScramCredentialsRequestScramCredentialUpsertion)
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

// AlterUserScramCredentialsRequestScramCredentialDeletion represents The SCRAM credentials to remove..
type AlterUserScramCredentialsRequestScramCredentialDeletion struct {
	// The user name.
	Name string `json:"name" versions:"0-999"`
	// The SCRAM mechanism.
	Mechanism int8 `json:"mechanism" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AlterUserScramCredentialsRequestScramCredentialDeletion.
func (m *AlterUserScramCredentialsRequestScramCredentialDeletion) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterUserScramCredentialsRequestScramCredentialDeletion.
func (m *AlterUserScramCredentialsRequestScramCredentialDeletion) readTaggedFields(r io.Reader, version int16) error {
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

// AlterUserScramCredentialsRequestScramCredentialUpsertion represents The SCRAM credentials to update/insert..
type AlterUserScramCredentialsRequestScramCredentialUpsertion struct {
	// The user name.
	Name string `json:"name" versions:"0-999"`
	// The SCRAM mechanism.
	Mechanism int8 `json:"mechanism" versions:"0-999"`
	// The number of iterations.
	Iterations int32 `json:"iterations" versions:"0-999"`
	// A random salt generated by the client.
	Salt []byte `json:"salt" versions:"0-999"`
	// The salted password.
	SaltedPassword []byte `json:"saltedpassword" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AlterUserScramCredentialsRequestScramCredentialUpsertion.
func (m *AlterUserScramCredentialsRequestScramCredentialUpsertion) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterUserScramCredentialsRequestScramCredentialUpsertion.
func (m *AlterUserScramCredentialsRequestScramCredentialUpsertion) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for AlterUserScramCredentialsRequest.
func (m *AlterUserScramCredentialsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterUserScramCredentialsRequest.
func (m *AlterUserScramCredentialsRequest) readTaggedFields(r io.Reader, version int16) error {
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
