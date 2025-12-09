package describeuserscramcredentials

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeUserScramCredentialsRequestApiKey        = 50
	DescribeUserScramCredentialsRequestHeaderVersion = 1
)

// DescribeUserScramCredentialsRequest represents a request message.
type DescribeUserScramCredentialsRequest struct {
	// The users to describe, or null/empty to describe all users.
	Users []DescribeUserScramCredentialsRequestUserName `json:"users" versions:"0-999"`
}

// Encode encodes a DescribeUserScramCredentialsRequest to a byte slice for the given version.
func (m *DescribeUserScramCredentialsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeUserScramCredentialsRequest from a byte slice for the given version.
func (m *DescribeUserScramCredentialsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeUserScramCredentialsRequest to an io.Writer for the given version.
func (m *DescribeUserScramCredentialsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// Users
	if version >= 0 && version <= 999 {
		if m.Users == nil {
			if isFlexible {
				if err := protocol.WriteVaruint32(w, 0); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteInt32(w, -1); err != nil {
					return err
				}
			}
		} else {
			if isFlexible {
				length := uint32(len(m.Users) + 1)
				if err := protocol.WriteVaruint32(w, length); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteInt32(w, int32(len(m.Users))); err != nil {
					return err
				}
			}
			for i := range m.Users {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(w, m.Users[i].Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(w, m.Users[i].Name); err != nil {
							return err
						}
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

// Read reads a DescribeUserScramCredentialsRequest from an io.Reader for the given version.
func (m *DescribeUserScramCredentialsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// Users
	if version >= 0 && version <= 999 {
		var length int32
		if isFlexible {
			var lengthUint uint32
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint == 0 {
				m.Users = nil
			} else {
				if lengthUint < 1 {
					return errors.New("invalid compact array length")
				}
				length = int32(lengthUint - 1)
				m.Users = make([]DescribeUserScramCredentialsRequestUserName, length)
				for i := int32(0); i < length; i++ {
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							val, err := protocol.ReadCompactString(r)
							if err != nil {
								return err
							}
							m.Users[i].Name = val
						} else {
							val, err := protocol.ReadString(r)
							if err != nil {
								return err
							}
							m.Users[i].Name = val
						}
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			if length == -1 {
				m.Users = nil
			} else {
				m.Users = make([]DescribeUserScramCredentialsRequestUserName, length)
				for i := int32(0); i < length; i++ {
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							val, err := protocol.ReadCompactString(r)
							if err != nil {
								return err
							}
							m.Users[i].Name = val
						} else {
							val, err := protocol.ReadString(r)
							if err != nil {
								return err
							}
							m.Users[i].Name = val
						}
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

// DescribeUserScramCredentialsRequestUserName represents The users to describe, or null/empty to describe all users..
type DescribeUserScramCredentialsRequestUserName struct {
	// The user name.
	Name string `json:"name" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeUserScramCredentialsRequest.
func (m *DescribeUserScramCredentialsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeUserScramCredentialsRequest.
func (m *DescribeUserScramCredentialsRequest) readTaggedFields(r io.Reader, version int16) error {
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
