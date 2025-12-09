package describecluster

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeClusterResponseApiKey        = 60
	DescribeClusterResponseHeaderVersion = 1
)

// DescribeClusterResponse represents a response message.
type DescribeClusterResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The top-level error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The top-level error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The endpoint type that was described. 1=brokers, 2=controllers.
	EndpointType int8 `json:"endpointtype" versions:"1-999"`
	// The cluster ID that responding broker belongs to.
	ClusterId string `json:"clusterid" versions:"0-999"`
	// The ID of the controller. When handled by a controller, returns the current voter leader ID. When handled by a broker, returns a random alive broker ID as a fallback.
	ControllerId int32 `json:"controllerid" versions:"0-999"`
	// Each broker in the response.
	Brokers []DescribeClusterResponseDescribeClusterBroker `json:"brokers" versions:"0-999"`
	// 32-bit bitfield to represent authorized operations for this cluster.
	ClusterAuthorizedOperations int32 `json:"clusterauthorizedoperations" versions:"0-999"`
}

// Encode encodes a DescribeClusterResponse to a byte slice for the given version.
func (m *DescribeClusterResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeClusterResponse from a byte slice for the given version.
func (m *DescribeClusterResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeClusterResponse to an io.Writer for the given version.
func (m *DescribeClusterResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
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
	// ErrorMessage
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ErrorMessage); err != nil {
				return err
			}
		}
	}
	// EndpointType
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt8(w, m.EndpointType); err != nil {
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
	// ControllerId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ControllerId); err != nil {
			return err
		}
	}
	// Brokers
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Brokers) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Brokers))); err != nil {
				return err
			}
		}
		for i := range m.Brokers {
			// BrokerId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Brokers[i].BrokerId); err != nil {
					return err
				}
			}
			// Host
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Brokers[i].Host); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Brokers[i].Host); err != nil {
						return err
					}
				}
			}
			// Port
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Brokers[i].Port); err != nil {
					return err
				}
			}
			// Rack
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Brokers[i].Rack); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Brokers[i].Rack); err != nil {
						return err
					}
				}
			}
			// IsFenced
			if version >= 2 && version <= 999 {
				if err := protocol.WriteBool(w, m.Brokers[i].IsFenced); err != nil {
					return err
				}
			}
		}
	}
	// ClusterAuthorizedOperations
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ClusterAuthorizedOperations); err != nil {
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

// Read reads a DescribeClusterResponse from an io.Reader for the given version.
func (m *DescribeClusterResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
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
	// ErrorMessage
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ErrorMessage = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ErrorMessage = val
		}
	}
	// EndpointType
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		m.EndpointType = val
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
	// ControllerId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ControllerId = val
	}
	// Brokers
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
			m.Brokers = make([]DescribeClusterResponseDescribeClusterBroker, length)
			for i := int32(0); i < length; i++ {
				// BrokerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Brokers[i].BrokerId = val
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Host = val
					}
				}
				// Port
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Brokers[i].Port = val
				}
				// Rack
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Rack = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Rack = val
					}
				}
				// IsFenced
				if version >= 2 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					m.Brokers[i].IsFenced = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Brokers = make([]DescribeClusterResponseDescribeClusterBroker, length)
			for i := int32(0); i < length; i++ {
				// BrokerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Brokers[i].BrokerId = val
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Host = val
					}
				}
				// Port
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Brokers[i].Port = val
				}
				// Rack
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Rack = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Brokers[i].Rack = val
					}
				}
				// IsFenced
				if version >= 2 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					m.Brokers[i].IsFenced = val
				}
			}
		}
	}
	// ClusterAuthorizedOperations
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ClusterAuthorizedOperations = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// DescribeClusterResponseDescribeClusterBroker represents Each broker in the response..
type DescribeClusterResponseDescribeClusterBroker struct {
	// The broker ID.
	BrokerId int32 `json:"brokerid" versions:"0-999"`
	// The broker hostname.
	Host string `json:"host" versions:"0-999"`
	// The broker port.
	Port int32 `json:"port" versions:"0-999"`
	// The rack of the broker, or null if it has not been assigned to a rack.
	Rack *string `json:"rack" versions:"0-999"`
	// Whether the broker is fenced
	IsFenced bool `json:"isfenced" versions:"2-999"`
}

// writeTaggedFields writes tagged fields for DescribeClusterResponse.
func (m *DescribeClusterResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeClusterResponse.
func (m *DescribeClusterResponse) readTaggedFields(r io.Reader, version int16) error {
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
