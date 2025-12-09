package joingroup

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	JoinGroupRequestApiKey        = 11
	JoinGroupRequestHeaderVersion = 1
)

// JoinGroupRequest represents a request message.
type JoinGroupRequest struct {
	// The group identifier.
	GroupId string `json:"groupid" versions:"0-999"`
	// The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
	SessionTimeoutMs int32 `json:"sessiontimeoutms" versions:"0-999"`
	// The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
	RebalanceTimeoutMs int32 `json:"rebalancetimeoutms" versions:"1-999"`
	// The member id assigned by the group coordinator.
	MemberId string `json:"memberid" versions:"0-999"`
	// The unique identifier of the consumer instance provided by end user.
	GroupInstanceId *string `json:"groupinstanceid" versions:"5-999"`
	// The unique name the for class of protocols implemented by the group we want to join.
	ProtocolType string `json:"protocoltype" versions:"0-999"`
	// The list of protocols that the member supports.
	Protocols []JoinGroupRequestJoinGroupRequestProtocol `json:"protocols" versions:"0-999"`
	// The reason why the member (re-)joins the group.
	Reason *string `json:"reason" versions:"8-999"`
}

// Encode encodes a JoinGroupRequest to a byte slice for the given version.
func (m *JoinGroupRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a JoinGroupRequest from a byte slice for the given version.
func (m *JoinGroupRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a JoinGroupRequest to an io.Writer for the given version.
func (m *JoinGroupRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 9 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.GroupId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.GroupId); err != nil {
				return err
			}
		}
	}
	// SessionTimeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.SessionTimeoutMs); err != nil {
			return err
		}
	}
	// RebalanceTimeoutMs
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.RebalanceTimeoutMs); err != nil {
			return err
		}
	}
	// MemberId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.MemberId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.MemberId); err != nil {
				return err
			}
		}
	}
	// GroupInstanceId
	if version >= 5 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.GroupInstanceId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.GroupInstanceId); err != nil {
				return err
			}
		}
	}
	// ProtocolType
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.ProtocolType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.ProtocolType); err != nil {
				return err
			}
		}
	}
	// Protocols
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(JoinGroupRequestJoinGroupRequestProtocol)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				}
			}
			// Metadata
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactBytes(elemW, structItem.Metadata); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteBytes(elemW, structItem.Metadata); err != nil {
						return nil, err
					}
				}
			}
			// Write tagged fields if flexible
			if isFlexible {
				if err := structItem.writeTaggedFields(elemW, version); err != nil {
					return nil, err
				}
			}
			return elemBuf.Bytes(), nil
		}
		items := make([]interface{}, len(m.Protocols))
		for i := range m.Protocols {
			items[i] = m.Protocols[i]
		}
		if isFlexible {
			if err := protocol.WriteCompactArray(w, items, encoder); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, items, encoder); err != nil {
				return err
			}
		}
	}
	// Reason
	if version >= 8 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.Reason); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.Reason); err != nil {
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

// Read reads a JoinGroupRequest from an io.Reader for the given version.
func (m *JoinGroupRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 9 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		}
	}
	// SessionTimeoutMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.SessionTimeoutMs = val
	}
	// RebalanceTimeoutMs
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.RebalanceTimeoutMs = val
	}
	// MemberId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.MemberId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.MemberId = val
		}
	}
	// GroupInstanceId
	if version >= 5 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.GroupInstanceId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.GroupInstanceId = val
		}
	}
	// ProtocolType
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.ProtocolType = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.ProtocolType = val
		}
	}
	// Protocols
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem JoinGroupRequestJoinGroupRequestProtocol
			elemR := bytes.NewReader(data)
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				}
			}
			// Metadata
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactBytes(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Metadata = val
				} else {
					val, err := protocol.ReadBytes(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Metadata = val
				}
			}
			// Read tagged fields if flexible
			if isFlexible {
				if err := elem.readTaggedFields(elemR, version); err != nil {
					return nil, 0, err
				}
			}
			consumed := len(data) - elemR.Len()
			return elem, consumed, nil
		}
		if isFlexible {
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint < 1 {
				return errors.New("invalid compact array length")
			}
			length := int32(lengthUint - 1)
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem JoinGroupRequestJoinGroupRequestProtocol
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// Metadata
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						tempElem.Metadata = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						tempElem.Metadata = val
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// Metadata
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactBytes(elemW, tempElem.Metadata); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteBytes(elemW, tempElem.Metadata); err != nil {
							return err
						}
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeCompactArray
			lengthBytes := protocol.EncodeVaruint32(lengthUint)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.Protocols = make([]JoinGroupRequestJoinGroupRequestProtocol, len(decoded))
			for i, item := range decoded {
				m.Protocols[i] = item.(JoinGroupRequestJoinGroupRequestProtocol)
			}
		} else {
			length, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem JoinGroupRequestJoinGroupRequestProtocol
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// Metadata
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						tempElem.Metadata = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						tempElem.Metadata = val
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// Metadata
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactBytes(elemW, tempElem.Metadata); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteBytes(elemW, tempElem.Metadata); err != nil {
							return err
						}
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeArray
			lengthBytes := protocol.EncodeInt32(length)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.Protocols = make([]JoinGroupRequestJoinGroupRequestProtocol, len(decoded))
			for i, item := range decoded {
				m.Protocols[i] = item.(JoinGroupRequestJoinGroupRequestProtocol)
			}
		}
	}
	// Reason
	if version >= 8 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.Reason = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.Reason = val
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

// JoinGroupRequestJoinGroupRequestProtocol represents The list of protocols that the member supports..
type JoinGroupRequestJoinGroupRequestProtocol struct {
	// The protocol name.
	Name string `json:"name" versions:"0-999"`
	// The protocol metadata.
	Metadata []byte `json:"metadata" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for JoinGroupRequestJoinGroupRequestProtocol.
func (m *JoinGroupRequestJoinGroupRequestProtocol) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for JoinGroupRequestJoinGroupRequestProtocol.
func (m *JoinGroupRequestJoinGroupRequestProtocol) readTaggedFields(r io.Reader, version int16) error {
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
		}
	}

	return nil
}

// writeTaggedFields writes tagged fields for JoinGroupRequest.
func (m *JoinGroupRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for JoinGroupRequest.
func (m *JoinGroupRequest) readTaggedFields(r io.Reader, version int16) error {
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
