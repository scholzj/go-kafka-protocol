package envelope

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	EnvelopeRequestApiKey        = 58
	EnvelopeRequestHeaderVersion = 1
)

// EnvelopeRequest represents a request message.
type EnvelopeRequest struct {
	// The embedded request header and data.
	RequestData []byte `json:"requestdata" versions:"0-999"`
	// Value of the initial client principal when the request is redirected by a broker.
	RequestPrincipal *[]byte `json:"requestprincipal" versions:"0-999"`
	// The original client's address in bytes.
	ClientHostAddress []byte `json:"clienthostaddress" versions:"0-999"`
}

// Encode encodes a EnvelopeRequest to a byte slice for the given version.
func (m *EnvelopeRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a EnvelopeRequest from a byte slice for the given version.
func (m *EnvelopeRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a EnvelopeRequest to an io.Writer for the given version.
func (m *EnvelopeRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// RequestData
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactBytes(w, m.RequestData); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteBytes(w, m.RequestData); err != nil {
				return err
			}
		}
	}
	// RequestPrincipal
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableBytes(w, m.RequestPrincipal); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableBytes(w, m.RequestPrincipal); err != nil {
				return err
			}
		}
	}
	// ClientHostAddress
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactBytes(w, m.ClientHostAddress); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteBytes(w, m.ClientHostAddress); err != nil {
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

// Read reads a EnvelopeRequest from an io.Reader for the given version.
func (m *EnvelopeRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// RequestData
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactBytes(r)
			if err != nil {
				return err
			}
			m.RequestData = val
		} else {
			val, err := protocol.ReadBytes(r)
			if err != nil {
				return err
			}
			m.RequestData = val
		}
	}
	// RequestPrincipal
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableBytes(r)
			if err != nil {
				return err
			}
			m.RequestPrincipal = val
		} else {
			val, err := protocol.ReadNullableBytes(r)
			if err != nil {
				return err
			}
			m.RequestPrincipal = val
		}
	}
	// ClientHostAddress
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactBytes(r)
			if err != nil {
				return err
			}
			m.ClientHostAddress = val
		} else {
			val, err := protocol.ReadBytes(r)
			if err != nil {
				return err
			}
			m.ClientHostAddress = val
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

// writeTaggedFields writes tagged fields for EnvelopeRequest.
func (m *EnvelopeRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for EnvelopeRequest.
func (m *EnvelopeRequest) readTaggedFields(r io.Reader, version int16) error {
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
