package createdelegationtoken

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	CreateDelegationTokenRequestApiKey        = 38
	CreateDelegationTokenRequestHeaderVersion = 1
)

// CreateDelegationTokenRequest represents a request message.
type CreateDelegationTokenRequest struct {
	// The principal type of the owner of the token. If it's null it defaults to the token request principal.
	OwnerPrincipalType *string `json:"ownerprincipaltype" versions:"3-999"`
	// The principal name of the owner of the token. If it's null it defaults to the token request principal.
	OwnerPrincipalName *string `json:"ownerprincipalname" versions:"3-999"`
	// A list of those who are allowed to renew this token before it expires.
	Renewers []CreateDelegationTokenRequestCreatableRenewers `json:"renewers" versions:"0-999"`
	// The maximum lifetime of the token in milliseconds, or -1 to use the server side default.
	MaxLifetimeMs int64 `json:"maxlifetimems" versions:"0-999"`
}

// Encode encodes a CreateDelegationTokenRequest to a byte slice for the given version.
func (m *CreateDelegationTokenRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a CreateDelegationTokenRequest from a byte slice for the given version.
func (m *CreateDelegationTokenRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a CreateDelegationTokenRequest to an io.Writer for the given version.
func (m *CreateDelegationTokenRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// OwnerPrincipalType
	if version >= 3 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.OwnerPrincipalType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.OwnerPrincipalType); err != nil {
				return err
			}
		}
	}
	// OwnerPrincipalName
	if version >= 3 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.OwnerPrincipalName); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.OwnerPrincipalName); err != nil {
				return err
			}
		}
	}
	// Renewers
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Renewers) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Renewers))); err != nil {
				return err
			}
		}
		for i := range m.Renewers {
			// PrincipalType
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Renewers[i].PrincipalType); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Renewers[i].PrincipalType); err != nil {
						return err
					}
				}
			}
			// PrincipalName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Renewers[i].PrincipalName); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Renewers[i].PrincipalName); err != nil {
						return err
					}
				}
			}
		}
	}
	// MaxLifetimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.MaxLifetimeMs); err != nil {
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

// Read reads a CreateDelegationTokenRequest from an io.Reader for the given version.
func (m *CreateDelegationTokenRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// OwnerPrincipalType
	if version >= 3 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.OwnerPrincipalType = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.OwnerPrincipalType = val
		}
	}
	// OwnerPrincipalName
	if version >= 3 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.OwnerPrincipalName = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.OwnerPrincipalName = val
		}
	}
	// Renewers
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
			m.Renewers = make([]CreateDelegationTokenRequestCreatableRenewers, length)
			for i := int32(0); i < length; i++ {
				// PrincipalType
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Renewers[i].PrincipalType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Renewers[i].PrincipalType = val
					}
				}
				// PrincipalName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Renewers[i].PrincipalName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Renewers[i].PrincipalName = val
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Renewers = make([]CreateDelegationTokenRequestCreatableRenewers, length)
			for i := int32(0); i < length; i++ {
				// PrincipalType
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Renewers[i].PrincipalType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Renewers[i].PrincipalType = val
					}
				}
				// PrincipalName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Renewers[i].PrincipalName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Renewers[i].PrincipalName = val
					}
				}
			}
		}
	}
	// MaxLifetimeMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.MaxLifetimeMs = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// CreateDelegationTokenRequestCreatableRenewers represents A list of those who are allowed to renew this token before it expires..
type CreateDelegationTokenRequestCreatableRenewers struct {
	// The type of the Kafka principal.
	PrincipalType string `json:"principaltype" versions:"0-999"`
	// The name of the Kafka principal.
	PrincipalName string `json:"principalname" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for CreateDelegationTokenRequest.
func (m *CreateDelegationTokenRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreateDelegationTokenRequest.
func (m *CreateDelegationTokenRequest) readTaggedFields(r io.Reader, version int16) error {
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
