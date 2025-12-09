package saslhandshake

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	SaslHandshakeResponseApiKey        = 17
	SaslHandshakeResponseHeaderVersion = 1
)

// SaslHandshakeResponse represents a response message.
type SaslHandshakeResponse struct {
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The mechanisms enabled in the server.
	Mechanisms []string `json:"mechanisms" versions:"0-999"`
}

// Encode encodes a SaslHandshakeResponse to a byte slice for the given version.
func (m *SaslHandshakeResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a SaslHandshakeResponse from a byte slice for the given version.
func (m *SaslHandshakeResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a SaslHandshakeResponse to an io.Writer for the given version.
func (m *SaslHandshakeResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ErrorCode
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// Mechanisms
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactStringArray(w, m.Mechanisms); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteStringArray(w, m.Mechanisms); err != nil {
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

// Read reads a SaslHandshakeResponse from an io.Reader for the given version.
func (m *SaslHandshakeResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
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
	// Mechanisms
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactStringArray(r)
			if err != nil {
				return err
			}
			m.Mechanisms = val
		} else {
			val, err := protocol.ReadStringArray(r)
			if err != nil {
				return err
			}
			m.Mechanisms = val
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

// writeTaggedFields writes tagged fields for SaslHandshakeResponse.
func (m *SaslHandshakeResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for SaslHandshakeResponse.
func (m *SaslHandshakeResponse) readTaggedFields(r io.Reader, version int16) error {
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
