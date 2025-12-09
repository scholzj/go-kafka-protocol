package renewdelegationtoken

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	RenewDelegationTokenRequestApiKey        = 39
	RenewDelegationTokenRequestHeaderVersion = 1
)

// RenewDelegationTokenRequest represents a request message.
type RenewDelegationTokenRequest struct {
	// The HMAC of the delegation token to be renewed.
	Hmac []byte `json:"hmac" versions:"0-999"`
	// The renewal time period in milliseconds.
	RenewPeriodMs int64 `json:"renewperiodms" versions:"0-999"`
}

// Encode encodes a RenewDelegationTokenRequest to a byte slice for the given version.
func (m *RenewDelegationTokenRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a RenewDelegationTokenRequest from a byte slice for the given version.
func (m *RenewDelegationTokenRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a RenewDelegationTokenRequest to an io.Writer for the given version.
func (m *RenewDelegationTokenRequest) Write(w io.Writer, version int16) error {
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
	// RenewPeriodMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.RenewPeriodMs); err != nil {
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

// Read reads a RenewDelegationTokenRequest from an io.Reader for the given version.
func (m *RenewDelegationTokenRequest) Read(r io.Reader, version int16) error {
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
	// RenewPeriodMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.RenewPeriodMs = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for RenewDelegationTokenRequest.
func (m *RenewDelegationTokenRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for RenewDelegationTokenRequest.
func (m *RenewDelegationTokenRequest) readTaggedFields(r io.Reader, version int16) error {
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
