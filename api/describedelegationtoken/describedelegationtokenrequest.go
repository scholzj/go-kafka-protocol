package describedelegationtoken

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeDelegationTokenRequestApiKey        = 41
	DescribeDelegationTokenRequestHeaderVersion = 1
)

// DescribeDelegationTokenRequest represents a request message.
type DescribeDelegationTokenRequest struct {
	// Each owner that we want to describe delegation tokens for, or null to describe all tokens.
	Owners []DescribeDelegationTokenRequestDescribeDelegationTokenOwner `json:"owners" versions:"0-999"`
}

// Encode encodes a DescribeDelegationTokenRequest to a byte slice for the given version.
func (m *DescribeDelegationTokenRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeDelegationTokenRequest from a byte slice for the given version.
func (m *DescribeDelegationTokenRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeDelegationTokenRequest to an io.Writer for the given version.
func (m *DescribeDelegationTokenRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Owners
	if version >= 0 && version <= 999 {
		if m.Owners == nil {
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
				length := uint32(len(m.Owners) + 1)
				if err := protocol.WriteVaruint32(w, length); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteInt32(w, int32(len(m.Owners))); err != nil {
					return err
				}
			}
			for i := range m.Owners {
				// PrincipalType
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(w, m.Owners[i].PrincipalType); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(w, m.Owners[i].PrincipalType); err != nil {
							return err
						}
					}
				}
				// PrincipalName
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(w, m.Owners[i].PrincipalName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(w, m.Owners[i].PrincipalName); err != nil {
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

// Read reads a DescribeDelegationTokenRequest from an io.Reader for the given version.
func (m *DescribeDelegationTokenRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Owners
	if version >= 0 && version <= 999 {
		var length int32
		if isFlexible {
			var lengthUint uint32
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint == 0 {
				m.Owners = nil
			} else {
				if lengthUint < 1 {
					return errors.New("invalid compact array length")
				}
				length = int32(lengthUint - 1)
				m.Owners = make([]DescribeDelegationTokenRequestDescribeDelegationTokenOwner, length)
				for i := int32(0); i < length; i++ {
					// PrincipalType
					if version >= 0 && version <= 999 {
						if isFlexible {
							val, err := protocol.ReadCompactString(r)
							if err != nil {
								return err
							}
							m.Owners[i].PrincipalType = val
						} else {
							val, err := protocol.ReadString(r)
							if err != nil {
								return err
							}
							m.Owners[i].PrincipalType = val
						}
					}
					// PrincipalName
					if version >= 0 && version <= 999 {
						if isFlexible {
							val, err := protocol.ReadCompactString(r)
							if err != nil {
								return err
							}
							m.Owners[i].PrincipalName = val
						} else {
							val, err := protocol.ReadString(r)
							if err != nil {
								return err
							}
							m.Owners[i].PrincipalName = val
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
				m.Owners = nil
			} else {
				m.Owners = make([]DescribeDelegationTokenRequestDescribeDelegationTokenOwner, length)
				for i := int32(0); i < length; i++ {
					// PrincipalType
					if version >= 0 && version <= 999 {
						if isFlexible {
							val, err := protocol.ReadCompactString(r)
							if err != nil {
								return err
							}
							m.Owners[i].PrincipalType = val
						} else {
							val, err := protocol.ReadString(r)
							if err != nil {
								return err
							}
							m.Owners[i].PrincipalType = val
						}
					}
					// PrincipalName
					if version >= 0 && version <= 999 {
						if isFlexible {
							val, err := protocol.ReadCompactString(r)
							if err != nil {
								return err
							}
							m.Owners[i].PrincipalName = val
						} else {
							val, err := protocol.ReadString(r)
							if err != nil {
								return err
							}
							m.Owners[i].PrincipalName = val
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

// DescribeDelegationTokenRequestDescribeDelegationTokenOwner represents Each owner that we want to describe delegation tokens for, or null to describe all tokens..
type DescribeDelegationTokenRequestDescribeDelegationTokenOwner struct {
	// The owner principal type.
	PrincipalType string `json:"principaltype" versions:"0-999"`
	// The owner principal name.
	PrincipalName string `json:"principalname" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeDelegationTokenRequest.
func (m *DescribeDelegationTokenRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeDelegationTokenRequest.
func (m *DescribeDelegationTokenRequest) readTaggedFields(r io.Reader, version int16) error {
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
