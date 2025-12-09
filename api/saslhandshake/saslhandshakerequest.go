package saslhandshake

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	SaslHandshakeRequestApiKey        = 17
	SaslHandshakeRequestHeaderVersion = 1
)

// SaslHandshakeRequest represents a request message.
type SaslHandshakeRequest struct {
	// The SASL mechanism chosen by the client.
	Mechanism string `json:"mechanism" versions:"0-999"`
}

// Encode encodes a SaslHandshakeRequest to a byte slice for the given version.
func (m *SaslHandshakeRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a SaslHandshakeRequest from a byte slice for the given version.
func (m *SaslHandshakeRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a SaslHandshakeRequest to an io.Writer for the given version.
func (m *SaslHandshakeRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// Mechanism
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.Mechanism); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.Mechanism); err != nil {
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

// Read reads a SaslHandshakeRequest from an io.Reader for the given version.
func (m *SaslHandshakeRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// Mechanism
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.Mechanism = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.Mechanism = val
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

// writeTaggedFields writes tagged fields for SaslHandshakeRequest.
func (m *SaslHandshakeRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for SaslHandshakeRequest.
func (m *SaslHandshakeRequest) readTaggedFields(r io.Reader, version int16) error {
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
