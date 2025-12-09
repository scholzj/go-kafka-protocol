package expiredelegationtoken

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ExpireDelegationTokenRequestApiKey        = 40
	ExpireDelegationTokenRequestHeaderVersion = 1
)

// ExpireDelegationTokenRequest represents a request message.
type ExpireDelegationTokenRequest struct {
	// The HMAC of the delegation token to be expired.
	Hmac []byte `json:"hmac" versions:"0-999"`
	// The expiry time period in milliseconds.
	ExpiryTimePeriodMs int64 `json:"expirytimeperiodms" versions:"0-999"`
}

// Encode encodes a ExpireDelegationTokenRequest to a byte slice for the given version.
func (m *ExpireDelegationTokenRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ExpireDelegationTokenRequest from a byte slice for the given version.
func (m *ExpireDelegationTokenRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ExpireDelegationTokenRequest to an io.Writer for the given version.
func (m *ExpireDelegationTokenRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
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
	// ExpiryTimePeriodMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.ExpiryTimePeriodMs); err != nil {
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

// Read reads a ExpireDelegationTokenRequest from an io.Reader for the given version.
func (m *ExpireDelegationTokenRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
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
	// ExpiryTimePeriodMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.ExpiryTimePeriodMs = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for ExpireDelegationTokenRequest.
func (m *ExpireDelegationTokenRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ExpireDelegationTokenRequest.
func (m *ExpireDelegationTokenRequest) readTaggedFields(r io.Reader, version int16) error {
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
