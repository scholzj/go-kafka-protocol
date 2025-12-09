package saslauthenticate

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	SaslAuthenticateResponseApiKey        = 36
	SaslAuthenticateResponseHeaderVersion = 1
)

// SaslAuthenticateResponse represents a response message.
type SaslAuthenticateResponse struct {
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The SASL authentication bytes from the server, as defined by the SASL mechanism.
	AuthBytes []byte `json:"authbytes" versions:"0-999"`
	// Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
	SessionLifetimeMs int64 `json:"sessionlifetimems" versions:"1-999"`
}

// Encode encodes a SaslAuthenticateResponse to a byte slice for the given version.
func (m *SaslAuthenticateResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a SaslAuthenticateResponse from a byte slice for the given version.
func (m *SaslAuthenticateResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a SaslAuthenticateResponse to an io.Writer for the given version.
func (m *SaslAuthenticateResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
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
	// AuthBytes
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactBytes(w, m.AuthBytes); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteBytes(w, m.AuthBytes); err != nil {
				return err
			}
		}
	}
	// SessionLifetimeMs
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt64(w, m.SessionLifetimeMs); err != nil {
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

// Read reads a SaslAuthenticateResponse from an io.Reader for the given version.
func (m *SaslAuthenticateResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
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
	// AuthBytes
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactBytes(r)
			if err != nil {
				return err
			}
			m.AuthBytes = val
		} else {
			val, err := protocol.ReadBytes(r)
			if err != nil {
				return err
			}
			m.AuthBytes = val
		}
	}
	// SessionLifetimeMs
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.SessionLifetimeMs = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for SaslAuthenticateResponse.
func (m *SaslAuthenticateResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for SaslAuthenticateResponse.
func (m *SaslAuthenticateResponse) readTaggedFields(r io.Reader, version int16) error {
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
