package findcoordinator

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	FindCoordinatorRequestApiKey        = 10
	FindCoordinatorRequestHeaderVersion = 1
)

// FindCoordinatorRequest represents a request message.
type FindCoordinatorRequest struct {
	// The coordinator key.
	Key string `json:"key" versions:"0-3"`
	// The coordinator key type. (group, transaction, share).
	KeyType int8 `json:"keytype" versions:"1-999"`
	// The coordinator keys.
	CoordinatorKeys []string `json:"coordinatorkeys" versions:"4-999"`
}

// Encode encodes a FindCoordinatorRequest to a byte slice for the given version.
func (m *FindCoordinatorRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a FindCoordinatorRequest from a byte slice for the given version.
func (m *FindCoordinatorRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a FindCoordinatorRequest to an io.Writer for the given version.
func (m *FindCoordinatorRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// Key
	if version >= 0 && version <= 3 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.Key); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.Key); err != nil {
				return err
			}
		}
	}
	// KeyType
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt8(w, m.KeyType); err != nil {
			return err
		}
	}
	// CoordinatorKeys
	if version >= 4 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.CoordinatorKeys) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.CoordinatorKeys))); err != nil {
				return err
			}
		}
		for i := range m.CoordinatorKeys {
			if isFlexible {
				if err := protocol.WriteCompactString(w, m.CoordinatorKeys[i]); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteString(w, m.CoordinatorKeys[i]); err != nil {
					return err
				}
			}
			_ = i
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

// Read reads a FindCoordinatorRequest from an io.Reader for the given version.
func (m *FindCoordinatorRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// Key
	if version >= 0 && version <= 3 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.Key = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.Key = val
		}
	}
	// KeyType
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		m.KeyType = val
	}
	// CoordinatorKeys
	if version >= 4 && version <= 999 {
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
			m.CoordinatorKeys = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.CoordinatorKeys[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.CoordinatorKeys[i] = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.CoordinatorKeys = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.CoordinatorKeys[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.CoordinatorKeys[i] = val
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

// writeTaggedFields writes tagged fields for FindCoordinatorRequest.
func (m *FindCoordinatorRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for FindCoordinatorRequest.
func (m *FindCoordinatorRequest) readTaggedFields(r io.Reader, version int16) error {
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
