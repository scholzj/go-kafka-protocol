package alterconfigs

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AlterConfigsRequestApiKey        = 33
	AlterConfigsRequestHeaderVersion = 1
)

// AlterConfigsRequest represents a request message.
type AlterConfigsRequest struct {
	// The updates for each resource.
	Resources []AlterConfigsRequestAlterConfigsResource `json:"resources" versions:"0-999"`
	// True if we should validate the request, but not change the configurations.
	ValidateOnly bool `json:"validateonly" versions:"0-999"`
}

// Encode encodes a AlterConfigsRequest to a byte slice for the given version.
func (m *AlterConfigsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AlterConfigsRequest from a byte slice for the given version.
func (m *AlterConfigsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AlterConfigsRequest to an io.Writer for the given version.
func (m *AlterConfigsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
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
			// Configs
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Resources[i].Configs) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Resources[i].Configs))); err != nil {
						return err
					}
				}
				for i := range m.Resources[i].Configs {
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Resources[i].Configs[i].Name); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Resources[i].Configs[i].Name); err != nil {
								return err
							}
						}
					}
					// Value
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Resources[i].Configs[i].Value); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Resources[i].Configs[i].Value); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}
	// ValidateOnly
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.ValidateOnly); err != nil {
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

// Read reads a AlterConfigsRequest from an io.Reader for the given version.
func (m *AlterConfigsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
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
			m.Resources = make([]AlterConfigsRequestAlterConfigsResource, length)
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
				// Configs
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
						m.Resources[i].Configs = make([]AlterConfigsRequestAlterableConfig, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Name = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Value = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Resources[i].Configs = make([]AlterConfigsRequestAlterableConfig, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Name = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Value = val
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
			m.Resources = make([]AlterConfigsRequestAlterConfigsResource, length)
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
				// Configs
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
						m.Resources[i].Configs = make([]AlterConfigsRequestAlterableConfig, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Name = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Value = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Resources[i].Configs = make([]AlterConfigsRequestAlterableConfig, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Name = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Resources[i].Configs[i].Value = val
								}
							}
						}
					}
				}
			}
		}
	}
	// ValidateOnly
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.ValidateOnly = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// AlterConfigsRequestAlterConfigsResource represents The updates for each resource..
type AlterConfigsRequestAlterConfigsResource struct {
	// The resource type.
	ResourceType int8 `json:"resourcetype" versions:"0-999"`
	// The resource name.
	ResourceName string `json:"resourcename" versions:"0-999"`
	// The configurations.
	Configs []AlterConfigsRequestAlterableConfig `json:"configs" versions:"0-999"`
}

// AlterConfigsRequestAlterableConfig represents The configurations..
type AlterConfigsRequestAlterableConfig struct {
	// The configuration key name.
	Name string `json:"name" versions:"0-999"`
	// The value to set for the configuration key.
	Value *string `json:"value" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for AlterConfigsRequest.
func (m *AlterConfigsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterConfigsRequest.
func (m *AlterConfigsRequest) readTaggedFields(r io.Reader, version int16) error {
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
