package createdelegationtoken

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	CreateDelegationTokenResponseApiKey        = 38
	CreateDelegationTokenResponseHeaderVersion = 1
)

// CreateDelegationTokenResponse represents a response message.
type CreateDelegationTokenResponse struct {
	// The top-level error, or zero if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The principal type of the token owner.
	PrincipalType string `json:"principaltype" versions:"0-999"`
	// The name of the token owner.
	PrincipalName string `json:"principalname" versions:"0-999"`
	// The principal type of the requester of the token.
	TokenRequesterPrincipalType string `json:"tokenrequesterprincipaltype" versions:"3-999"`
	// The principal type of the requester of the token.
	TokenRequesterPrincipalName string `json:"tokenrequesterprincipalname" versions:"3-999"`
	// When this token was generated.
	IssueTimestampMs int64 `json:"issuetimestampms" versions:"0-999"`
	// When this token expires.
	ExpiryTimestampMs int64 `json:"expirytimestampms" versions:"0-999"`
	// The maximum lifetime of this token.
	MaxTimestampMs int64 `json:"maxtimestampms" versions:"0-999"`
	// The token UUID.
	TokenId string `json:"tokenid" versions:"0-999"`
	// HMAC of the delegation token.
	Hmac []byte `json:"hmac" versions:"0-999"`
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
}

// Encode encodes a CreateDelegationTokenResponse to a byte slice for the given version.
func (m *CreateDelegationTokenResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a CreateDelegationTokenResponse from a byte slice for the given version.
func (m *CreateDelegationTokenResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a CreateDelegationTokenResponse to an io.Writer for the given version.
func (m *CreateDelegationTokenResponse) Write(w io.Writer, version int16) error {
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
	// PrincipalType
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.PrincipalType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.PrincipalType); err != nil {
				return err
			}
		}
	}
	// PrincipalName
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.PrincipalName); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.PrincipalName); err != nil {
				return err
			}
		}
	}
	// TokenRequesterPrincipalType
	if version >= 3 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.TokenRequesterPrincipalType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.TokenRequesterPrincipalType); err != nil {
				return err
			}
		}
	}
	// TokenRequesterPrincipalName
	if version >= 3 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.TokenRequesterPrincipalName); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.TokenRequesterPrincipalName); err != nil {
				return err
			}
		}
	}
	// IssueTimestampMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.IssueTimestampMs); err != nil {
			return err
		}
	}
	// ExpiryTimestampMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.ExpiryTimestampMs); err != nil {
			return err
		}
	}
	// MaxTimestampMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.MaxTimestampMs); err != nil {
			return err
		}
	}
	// TokenId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.TokenId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.TokenId); err != nil {
				return err
			}
		}
	}
	// Hmac
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactBytes(w, m.Hmac); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteBytes(w, m.Hmac); err != nil {
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

// Read reads a CreateDelegationTokenResponse from an io.Reader for the given version.
func (m *CreateDelegationTokenResponse) Read(r io.Reader, version int16) error {
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
	// PrincipalType
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.PrincipalType = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.PrincipalType = val
		}
	}
	// PrincipalName
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.PrincipalName = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.PrincipalName = val
		}
	}
	// TokenRequesterPrincipalType
	if version >= 3 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.TokenRequesterPrincipalType = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.TokenRequesterPrincipalType = val
		}
	}
	// TokenRequesterPrincipalName
	if version >= 3 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.TokenRequesterPrincipalName = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.TokenRequesterPrincipalName = val
		}
	}
	// IssueTimestampMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.IssueTimestampMs = val
	}
	// ExpiryTimestampMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.ExpiryTimestampMs = val
	}
	// MaxTimestampMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.MaxTimestampMs = val
	}
	// TokenId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.TokenId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.TokenId = val
		}
	}
	// Hmac
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactBytes(r)
			if err != nil {
				return err
			}
			m.Hmac = val
		} else {
			val, err := protocol.ReadBytes(r)
			if err != nil {
				return err
			}
			m.Hmac = val
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

// writeTaggedFields writes tagged fields for CreateDelegationTokenResponse.
func (m *CreateDelegationTokenResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreateDelegationTokenResponse.
func (m *CreateDelegationTokenResponse) readTaggedFields(r io.Reader, version int16) error {
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
