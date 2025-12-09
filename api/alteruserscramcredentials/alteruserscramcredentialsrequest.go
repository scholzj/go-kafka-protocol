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
		if isFlexible {
			length := uint32(len(m.Deletions) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Deletions))); err != nil {
				return err
			}
		}
		for i := range m.Deletions {
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Deletions[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Deletions[i].Name); err != nil {
						return err
					}
				}
			}
			// Mechanism
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Deletions[i].Mechanism); err != nil {
					return err
				}
			}
		}
	}
	// Upsertions
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Upsertions) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Upsertions))); err != nil {
				return err
			}
		}
		for i := range m.Upsertions {
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Upsertions[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Upsertions[i].Name); err != nil {
						return err
					}
				}
			}
			// Mechanism
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Upsertions[i].Mechanism); err != nil {
					return err
				}
			}
			// Iterations
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Upsertions[i].Iterations); err != nil {
					return err
				}
			}
			// Salt
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactBytes(w, m.Upsertions[i].Salt); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteBytes(w, m.Upsertions[i].Salt); err != nil {
						return err
					}
				}
			}
			// SaltedPassword
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactBytes(w, m.Upsertions[i].SaltedPassword); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteBytes(w, m.Upsertions[i].SaltedPassword); err != nil {
						return err
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
			m.Deletions = make([]AlterUserScramCredentialsRequestScramCredentialDeletion, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Deletions[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Deletions[i].Name = val
					}
				}
				// Mechanism
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Deletions[i].Mechanism = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Deletions = make([]AlterUserScramCredentialsRequestScramCredentialDeletion, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Deletions[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Deletions[i].Name = val
					}
				}
				// Mechanism
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Deletions[i].Mechanism = val
				}
			}
		}
	}
	// Upsertions
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
			m.Upsertions = make([]AlterUserScramCredentialsRequestScramCredentialUpsertion, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].Name = val
					}
				}
				// Mechanism
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Upsertions[i].Mechanism = val
				}
				// Iterations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Upsertions[i].Iterations = val
				}
				// Salt
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].Salt = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].Salt = val
					}
				}
				// SaltedPassword
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].SaltedPassword = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].SaltedPassword = val
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Upsertions = make([]AlterUserScramCredentialsRequestScramCredentialUpsertion, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].Name = val
					}
				}
				// Mechanism
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Upsertions[i].Mechanism = val
				}
				// Iterations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Upsertions[i].Iterations = val
				}
				// Salt
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].Salt = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].Salt = val
					}
				}
				// SaltedPassword
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].SaltedPassword = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						m.Upsertions[i].SaltedPassword = val
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

// AlterUserScramCredentialsRequestScramCredentialDeletion represents The SCRAM credentials to remove..
type AlterUserScramCredentialsRequestScramCredentialDeletion struct {
	// The user name.
	Name string `json:"name" versions:"0-999"`
	// The SCRAM mechanism.
	Mechanism int8 `json:"mechanism" versions:"0-999"`
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
