package describeconfigs

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeConfigsRequestApiKey        = 32
	DescribeConfigsRequestHeaderVersion = 1
)

// DescribeConfigsRequest represents a request message.
type DescribeConfigsRequest struct {
	// The resources whose configurations we want to describe.
	Resources []DescribeConfigsRequestDescribeConfigsResource `json:"resources" versions:"0-999"`
	// True if we should include all synonyms.
	IncludeSynonyms bool `json:"includesynonyms" versions:"1-999"`
	// True if we should include configuration documentation.
	IncludeDocumentation bool `json:"includedocumentation" versions:"3-999"`
}

// Encode encodes a DescribeConfigsRequest to a byte slice for the given version.
func (m *DescribeConfigsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeConfigsRequest from a byte slice for the given version.
func (m *DescribeConfigsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeConfigsRequest to an io.Writer for the given version.
func (m *DescribeConfigsRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// Resources
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Resources) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Resources))); err != nil {
				return err
			}
		}
		for i := range m.Resources {
			// ResourceType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Resources[i].ResourceType); err != nil {
					return err
				}
			}
			// ResourceName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Resources[i].ResourceName); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Resources[i].ResourceName); err != nil {
						return err
					}
				}
			}
			// ConfigurationKeys
			if version >= 0 && version <= 999 {
				if m.Resources[i].ConfigurationKeys == nil {
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
						length := uint32(len(m.Resources[i].ConfigurationKeys) + 1)
						if err := protocol.WriteVaruint32(w, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(w, int32(len(m.Resources[i].ConfigurationKeys))); err != nil {
							return err
						}
					}
					for i := range m.Resources[i].ConfigurationKeys {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Resources[i].ConfigurationKeys[i]); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Resources[i].ConfigurationKeys[i]); err != nil {
								return err
							}
						}
						_ = i
					}
				}
			}
		}
	}
	// IncludeSynonyms
	if version >= 1 && version <= 999 {
		if err := protocol.WriteBool(w, m.IncludeSynonyms); err != nil {
			return err
		}
	}
	// IncludeDocumentation
	if version >= 3 && version <= 999 {
		if err := protocol.WriteBool(w, m.IncludeDocumentation); err != nil {
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

// Read reads a DescribeConfigsRequest from an io.Reader for the given version.
func (m *DescribeConfigsRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// Resources
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
			m.Resources = make([]DescribeConfigsRequestDescribeConfigsResource, length)
			for i := int32(0); i < length; i++ {
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Resources[i].ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Resources[i].ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Resources[i].ResourceName = val
					}
				}
				// ConfigurationKeys
				if version >= 0 && version <= 999 {
					var length int32
					if isFlexible {
						var lengthUint uint32
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint == 0 {
							m.Resources[i].ConfigurationKeys = nil
						} else {
							if lengthUint < 1 {
								return errors.New("invalid compact array length")
							}
							length = int32(lengthUint - 1)
							m.Resources[i].ConfigurationKeys = make([]string, length)
							for i := int32(0); i < length; i++ {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].ConfigurationKeys[i] = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].ConfigurationKeys[i] = val
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
							m.Resources[i].ConfigurationKeys = nil
						} else {
							m.Resources[i].ConfigurationKeys = make([]string, length)
							for i := int32(0); i < length; i++ {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].ConfigurationKeys[i] = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].ConfigurationKeys[i] = val
								}
							}
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
			m.Resources = make([]DescribeConfigsRequestDescribeConfigsResource, length)
			for i := int32(0); i < length; i++ {
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Resources[i].ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Resources[i].ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Resources[i].ResourceName = val
					}
				}
				// ConfigurationKeys
				if version >= 0 && version <= 999 {
					var length int32
					if isFlexible {
						var lengthUint uint32
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint == 0 {
							m.Resources[i].ConfigurationKeys = nil
						} else {
							if lengthUint < 1 {
								return errors.New("invalid compact array length")
							}
							length = int32(lengthUint - 1)
							m.Resources[i].ConfigurationKeys = make([]string, length)
							for i := int32(0); i < length; i++ {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].ConfigurationKeys[i] = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].ConfigurationKeys[i] = val
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
							m.Resources[i].ConfigurationKeys = nil
						} else {
							m.Resources[i].ConfigurationKeys = make([]string, length)
							for i := int32(0); i < length; i++ {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].ConfigurationKeys[i] = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].ConfigurationKeys[i] = val
								}
							}
						}
					}
				}
			}
		}
	}
	// IncludeSynonyms
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IncludeSynonyms = val
	}
	// IncludeDocumentation
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IncludeDocumentation = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// DescribeConfigsRequestDescribeConfigsResource represents The resources whose configurations we want to describe..
type DescribeConfigsRequestDescribeConfigsResource struct {
	// The resource type.
	ResourceType int8 `json:"resourcetype" versions:"0-999"`
	// The resource name.
	ResourceName string `json:"resourcename" versions:"0-999"`
	// The configuration keys to list, or null to list all configuration keys.
	ConfigurationKeys []string `json:"configurationkeys" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeConfigsRequest.
func (m *DescribeConfigsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeConfigsRequest.
func (m *DescribeConfigsRequest) readTaggedFields(r io.Reader, version int16) error {
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
