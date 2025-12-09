package listconfigresources

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ListConfigResourcesResponseApiKey        = 74
	ListConfigResourcesResponseHeaderVersion = 1
)

// ListConfigResourcesResponse represents a response message.
type ListConfigResourcesResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// Each config resource in the response.
	ConfigResources []ListConfigResourcesResponseConfigResource `json:"configresources" versions:"0-999"`
}

// Encode encodes a ListConfigResourcesResponse to a byte slice for the given version.
func (m *ListConfigResourcesResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ListConfigResourcesResponse from a byte slice for the given version.
func (m *ListConfigResourcesResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ListConfigResourcesResponse to an io.Writer for the given version.
func (m *ListConfigResourcesResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// ErrorCode
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// ConfigResources
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.ConfigResources) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.ConfigResources))); err != nil {
				return err
			}
		}
		for i := range m.ConfigResources {
			// ResourceName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.ConfigResources[i].ResourceName); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.ConfigResources[i].ResourceName); err != nil {
						return err
					}
				}
			}
			// ResourceType
			if version >= 1 && version <= 999 {
				if err := protocol.WriteInt8(w, m.ConfigResources[i].ResourceType); err != nil {
					return err
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

// Read reads a ListConfigResourcesResponse from an io.Reader for the given version.
func (m *ListConfigResourcesResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// ErrorCode
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// ConfigResources
	if version >= 0 && version <= 999 {
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
			m.ConfigResources = make([]ListConfigResourcesResponseConfigResource, length)
			for i := int32(0); i < length; i++ {
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.ConfigResources[i].ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.ConfigResources[i].ResourceName = val
					}
				}
				// ResourceType
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.ConfigResources[i].ResourceType = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.ConfigResources = make([]ListConfigResourcesResponseConfigResource, length)
			for i := int32(0); i < length; i++ {
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.ConfigResources[i].ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.ConfigResources[i].ResourceName = val
					}
				}
				// ResourceType
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.ConfigResources[i].ResourceType = val
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

// ListConfigResourcesResponseConfigResource represents Each config resource in the response..
type ListConfigResourcesResponseConfigResource struct {
	// The resource name.
	ResourceName string `json:"resourcename" versions:"0-999"`
	// The resource type.
	ResourceType int8 `json:"resourcetype" versions:"1-999"`
}

// writeTaggedFields writes tagged fields for ListConfigResourcesResponse.
func (m *ListConfigResourcesResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ListConfigResourcesResponse.
func (m *ListConfigResourcesResponse) readTaggedFields(r io.Reader, version int16) error {
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
