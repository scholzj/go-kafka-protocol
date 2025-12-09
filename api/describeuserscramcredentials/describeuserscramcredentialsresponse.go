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
		if isFlexible {
			length := uint32(len(m.Results) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Results))); err != nil {
				return err
			}
		}
		for i := range m.Results {
			// User
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Results[i].User); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Results[i].User); err != nil {
						return err
					}
				}
			}
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Results[i].ErrorCode); err != nil {
					return err
				}
			}
			// ErrorMessage
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Results[i].ErrorMessage); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Results[i].ErrorMessage); err != nil {
						return err
					}
				}
			}
			// CredentialInfos
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Results[i].CredentialInfos) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Results[i].CredentialInfos))); err != nil {
						return err
					}
				}
				for i := range m.Results[i].CredentialInfos {
					// Mechanism
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt8(w, m.Results[i].CredentialInfos[i].Mechanism); err != nil {
							return err
						}
					}
					// Iterations
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Results[i].CredentialInfos[i].Iterations); err != nil {
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
			m.Results = make([]DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult, length)
			for i := int32(0); i < length; i++ {
				// User
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Results[i].User = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Results[i].User = val
					}
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Results[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Results[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Results[i].ErrorMessage = val
					}
				}
				// CredentialInfos
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
						m.Results[i].CredentialInfos = make([]DescribeUserScramCredentialsResponseCredentialInfo, length)
						for i := int32(0); i < length; i++ {
							// Mechanism
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].CredentialInfos[i].Mechanism = val
							}
							// Iterations
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].CredentialInfos[i].Iterations = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Results[i].CredentialInfos = make([]DescribeUserScramCredentialsResponseCredentialInfo, length)
						for i := int32(0); i < length; i++ {
							// Mechanism
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].CredentialInfos[i].Mechanism = val
							}
							// Iterations
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].CredentialInfos[i].Iterations = val
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
			m.Results = make([]DescribeUserScramCredentialsResponseDescribeUserScramCredentialsResult, length)
			for i := int32(0); i < length; i++ {
				// User
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Results[i].User = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Results[i].User = val
					}
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Results[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Results[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Results[i].ErrorMessage = val
					}
				}
				// CredentialInfos
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
						m.Results[i].CredentialInfos = make([]DescribeUserScramCredentialsResponseCredentialInfo, length)
						for i := int32(0); i < length; i++ {
							// Mechanism
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].CredentialInfos[i].Mechanism = val
							}
							// Iterations
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].CredentialInfos[i].Iterations = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Results[i].CredentialInfos = make([]DescribeUserScramCredentialsResponseCredentialInfo, length)
						for i := int32(0); i < length; i++ {
							// Mechanism
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].CredentialInfos[i].Mechanism = val
							}
							// Iterations
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].CredentialInfos[i].Iterations = val
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
}

// DescribeUserScramCredentialsResponseCredentialInfo represents The mechanism and related information associated with the user's SCRAM credentials..
type DescribeUserScramCredentialsResponseCredentialInfo struct {
	// The SCRAM mechanism.
	Mechanism int8 `json:"mechanism" versions:"0-999"`
	// The number of iterations used in the SCRAM credential.
	Iterations int32 `json:"iterations" versions:"0-999"`
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
