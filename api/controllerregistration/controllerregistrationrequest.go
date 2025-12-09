package controllerregistration

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ControllerRegistrationRequestApiKey        = 70
	ControllerRegistrationRequestHeaderVersion = 1
)

// ControllerRegistrationRequest represents a request message.
type ControllerRegistrationRequest struct {
	// The ID of the controller to register.
	ControllerId int32 `json:"controllerid" versions:"0-999"`
	// The controller incarnation ID, which is unique to each process run.
	IncarnationId uuid.UUID `json:"incarnationid" versions:"0-999"`
	// Set if the required configurations for ZK migration are present.
	ZkMigrationReady bool `json:"zkmigrationready" versions:"0-999"`
	// The listeners of this controller.
	Listeners []ControllerRegistrationRequestListener `json:"listeners" versions:"0-999"`
	// The features on this controller.
	Features []ControllerRegistrationRequestFeature `json:"features" versions:"0-999"`
}

// Encode encodes a ControllerRegistrationRequest to a byte slice for the given version.
func (m *ControllerRegistrationRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ControllerRegistrationRequest from a byte slice for the given version.
func (m *ControllerRegistrationRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ControllerRegistrationRequest to an io.Writer for the given version.
func (m *ControllerRegistrationRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ControllerId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ControllerId); err != nil {
			return err
		}
	}
	// IncarnationId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteUUID(w, m.IncarnationId); err != nil {
			return err
		}
	}
	// ZkMigrationReady
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.ZkMigrationReady); err != nil {
			return err
		}
	}
	// Listeners
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Listeners) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Listeners))); err != nil {
				return err
			}
		}
		for i := range m.Listeners {
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Listeners[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Listeners[i].Name); err != nil {
						return err
					}
				}
			}
			// Host
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Listeners[i].Host); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Listeners[i].Host); err != nil {
						return err
					}
				}
			}
			// Port
			if version >= 0 && version <= 999 {
			}
			// SecurityProtocol
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Listeners[i].SecurityProtocol); err != nil {
					return err
				}
			}
		}
	}
	// Features
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Features) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Features))); err != nil {
				return err
			}
		}
		for i := range m.Features {
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Features[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Features[i].Name); err != nil {
						return err
					}
				}
			}
			// MinSupportedVersion
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Features[i].MinSupportedVersion); err != nil {
					return err
				}
			}
			// MaxSupportedVersion
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Features[i].MaxSupportedVersion); err != nil {
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

// Read reads a ControllerRegistrationRequest from an io.Reader for the given version.
func (m *ControllerRegistrationRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ControllerId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ControllerId = val
	}
	// IncarnationId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadUUID(r)
		if err != nil {
			return err
		}
		m.IncarnationId = val
	}
	// ZkMigrationReady
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.ZkMigrationReady = val
	}
	// Listeners
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
			m.Listeners = make([]ControllerRegistrationRequestListener, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Name = val
					}
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Host = val
					}
				}
				// Port
				if version >= 0 && version <= 999 {
				}
				// SecurityProtocol
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Listeners[i].SecurityProtocol = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Listeners = make([]ControllerRegistrationRequestListener, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Name = val
					}
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Listeners[i].Host = val
					}
				}
				// Port
				if version >= 0 && version <= 999 {
				}
				// SecurityProtocol
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Listeners[i].SecurityProtocol = val
				}
			}
		}
	}
	// Features
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
			m.Features = make([]ControllerRegistrationRequestFeature, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Features[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Features[i].Name = val
					}
				}
				// MinSupportedVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Features[i].MinSupportedVersion = val
				}
				// MaxSupportedVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Features[i].MaxSupportedVersion = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Features = make([]ControllerRegistrationRequestFeature, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Features[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Features[i].Name = val
					}
				}
				// MinSupportedVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Features[i].MinSupportedVersion = val
				}
				// MaxSupportedVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Features[i].MaxSupportedVersion = val
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

// ControllerRegistrationRequestListener represents The listeners of this controller..
type ControllerRegistrationRequestListener struct {
	// The name of the endpoint.
	Name string `json:"name" versions:"0-999"`
	// The hostname.
	Host string `json:"host" versions:"0-999"`
	// The port.
	Port uint16 `json:"port" versions:"0-999"`
	// The security protocol.
	SecurityProtocol int16 `json:"securityprotocol" versions:"0-999"`
}

// ControllerRegistrationRequestFeature represents The features on this controller..
type ControllerRegistrationRequestFeature struct {
	// The feature name.
	Name string `json:"name" versions:"0-999"`
	// The minimum supported feature level.
	MinSupportedVersion int16 `json:"minsupportedversion" versions:"0-999"`
	// The maximum supported feature level.
	MaxSupportedVersion int16 `json:"maxsupportedversion" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for ControllerRegistrationRequest.
func (m *ControllerRegistrationRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ControllerRegistrationRequest.
func (m *ControllerRegistrationRequest) readTaggedFields(r io.Reader, version int16) error {
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
