package brokerregistration

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	BrokerRegistrationRequestApiKey        = 62
	BrokerRegistrationRequestHeaderVersion = 1
)

// BrokerRegistrationRequest represents a request message.
type BrokerRegistrationRequest struct {
	// The broker ID.
	BrokerId int32 `json:"brokerid" versions:"0-999"`
	// The cluster id of the broker process.
	ClusterId string `json:"clusterid" versions:"0-999"`
	// The incarnation id of the broker process.
	IncarnationId uuid.UUID `json:"incarnationid" versions:"0-999"`
	// The listeners of this broker.
	Listeners []BrokerRegistrationRequestListener `json:"listeners" versions:"0-999"`
	// The features on this broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
	Features []BrokerRegistrationRequestFeature `json:"features" versions:"0-999"`
	// The rack which this broker is in.
	Rack *string `json:"rack" versions:"0-999"`
	// If the required configurations for ZK migration are present, this value is set to true.
	IsMigratingZkBroker bool `json:"ismigratingzkbroker" versions:"1-999"`
	// Log directories configured in this broker which are available.
	LogDirs []uuid.UUID `json:"logdirs" versions:"2-999"`
	// The epoch before a clean shutdown.
	PreviousBrokerEpoch int64 `json:"previousbrokerepoch" versions:"3-999"`
}

// Encode encodes a BrokerRegistrationRequest to a byte slice for the given version.
func (m *BrokerRegistrationRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a BrokerRegistrationRequest from a byte slice for the given version.
func (m *BrokerRegistrationRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a BrokerRegistrationRequest to an io.Writer for the given version.
func (m *BrokerRegistrationRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// BrokerId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.BrokerId); err != nil {
			return err
		}
	}
	// ClusterId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.ClusterId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.ClusterId); err != nil {
				return err
			}
		}
	}
	// IncarnationId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteUUID(w, m.IncarnationId); err != nil {
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
	// Rack
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.Rack); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.Rack); err != nil {
				return err
			}
		}
	}
	// IsMigratingZkBroker
	if version >= 1 && version <= 999 {
		if err := protocol.WriteBool(w, m.IsMigratingZkBroker); err != nil {
			return err
		}
	}
	// LogDirs
	if version >= 2 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.LogDirs) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.LogDirs))); err != nil {
				return err
			}
		}
		for i := range m.LogDirs {
			if err := protocol.WriteUUID(w, m.LogDirs[i]); err != nil {
				return err
			}
			_ = i
		}
	}
	// PreviousBrokerEpoch
	if version >= 3 && version <= 999 {
		if err := protocol.WriteInt64(w, m.PreviousBrokerEpoch); err != nil {
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

// Read reads a BrokerRegistrationRequest from an io.Reader for the given version.
func (m *BrokerRegistrationRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// BrokerId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.BrokerId = val
	}
	// ClusterId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		}
	}
	// IncarnationId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadUUID(r)
		if err != nil {
			return err
		}
		m.IncarnationId = val
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
			m.Listeners = make([]BrokerRegistrationRequestListener, length)
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
			m.Listeners = make([]BrokerRegistrationRequestListener, length)
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
			m.Features = make([]BrokerRegistrationRequestFeature, length)
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
			m.Features = make([]BrokerRegistrationRequestFeature, length)
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
	// Rack
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.Rack = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.Rack = val
		}
	}
	// IsMigratingZkBroker
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IsMigratingZkBroker = val
	}
	// LogDirs
	if version >= 2 && version <= 999 {
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
			m.LogDirs = make([]uuid.UUID, length)
			for i := int32(0); i < length; i++ {
				val, err := protocol.ReadUUID(r)
				if err != nil {
					return err
				}
				m.LogDirs[i] = val
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.LogDirs = make([]uuid.UUID, length)
			for i := int32(0); i < length; i++ {
				val, err := protocol.ReadUUID(r)
				if err != nil {
					return err
				}
				m.LogDirs[i] = val
			}
		}
	}
	// PreviousBrokerEpoch
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.PreviousBrokerEpoch = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// BrokerRegistrationRequestListener represents The listeners of this broker..
type BrokerRegistrationRequestListener struct {
	// The name of the endpoint.
	Name string `json:"name" versions:"0-999"`
	// The hostname.
	Host string `json:"host" versions:"0-999"`
	// The port.
	Port uint16 `json:"port" versions:"0-999"`
	// The security protocol.
	SecurityProtocol int16 `json:"securityprotocol" versions:"0-999"`
}

// BrokerRegistrationRequestFeature represents The features on this broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted..
type BrokerRegistrationRequestFeature struct {
	// The feature name.
	Name string `json:"name" versions:"0-999"`
	// The minimum supported feature level.
	MinSupportedVersion int16 `json:"minsupportedversion" versions:"0-999"`
	// The maximum supported feature level.
	MaxSupportedVersion int16 `json:"maxsupportedversion" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for BrokerRegistrationRequest.
func (m *BrokerRegistrationRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for BrokerRegistrationRequest.
func (m *BrokerRegistrationRequest) readTaggedFields(r io.Reader, version int16) error {
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
