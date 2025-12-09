package metadata

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	MetadataResponseApiKey        = 3
	MetadataResponseHeaderVersion = 1
)

// MetadataResponse represents a response message.
type MetadataResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"3-999"`
	// A list of brokers present in the cluster.
	Brokers []MetadataResponseMetadataResponseBroker `json:"brokers" versions:"0-999"`
	// The cluster ID that responding broker belongs to.
	ClusterId *string `json:"clusterid" versions:"2-999"`
	// The ID of the controller broker.
	ControllerId int32 `json:"controllerid" versions:"1-999"`
	// Each topic in the response.
	Topics []MetadataResponseMetadataResponseTopic `json:"topics" versions:"0-999"`
	// 32-bit bitfield to represent authorized operations for this cluster.
	ClusterAuthorizedOperations int32 `json:"clusterauthorizedoperations" versions:"8-10"`
	// The top-level error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"13-999"`
}

// Encode encodes a MetadataResponse to a byte slice for the given version.
func (m *MetadataResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a MetadataResponse from a byte slice for the given version.
func (m *MetadataResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a MetadataResponse to an io.Writer for the given version.
func (m *MetadataResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 3 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// Brokers
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(MetadataResponseMetadataResponseBroker)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// NodeId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.NodeId); err != nil {
					return nil, err
				}
			}
			// Host
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Host); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Host); err != nil {
						return nil, err
					}
				}
			}
			// Port
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.Port); err != nil {
					return nil, err
				}
			}
			// Rack
			if version >= 1 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.Rack); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.Rack); err != nil {
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
		items := make([]interface{}, len(m.Brokers))
		for i := range m.Brokers {
			items[i] = m.Brokers[i]
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
	// ClusterId
	if version >= 2 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ClusterId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ClusterId); err != nil {
				return err
			}
		}
	}
	// ControllerId
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ControllerId); err != nil {
			return err
		}
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(MetadataResponseMetadataResponseTopic)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ErrorCode); err != nil {
					return nil, err
				}
			}
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				}
			}
			// TopicId
			if version >= 10 && version <= 999 {
				if err := protocol.WriteUUID(elemW, structItem.TopicId); err != nil {
					return nil, err
				}
			}
			// IsInternal
			if version >= 1 && version <= 999 {
				if err := protocol.WriteBool(elemW, structItem.IsInternal); err != nil {
					return nil, err
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Partitions) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Partitions {
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(elemW, structItem.Partitions[i].ErrorCode); err != nil {
							return nil, err
						}
					}
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].PartitionIndex); err != nil {
							return nil, err
						}
					}
					// LeaderId
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].LeaderId); err != nil {
							return nil, err
						}
					}
					// LeaderEpoch
					if version >= 7 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].LeaderEpoch); err != nil {
							return nil, err
						}
					}
					// ReplicaNodes
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions[i].ReplicaNodes); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Partitions[i].ReplicaNodes); err != nil {
								return nil, err
							}
						}
					}
					// IsrNodes
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions[i].IsrNodes); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Partitions[i].IsrNodes); err != nil {
								return nil, err
							}
						}
					}
					// OfflineReplicas
					if version >= 5 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions[i].OfflineReplicas); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Partitions[i].OfflineReplicas); err != nil {
								return nil, err
							}
						}
					}
				}
			}
			// TopicAuthorizedOperations
			if version >= 8 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.TopicAuthorizedOperations); err != nil {
					return nil, err
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
		items := make([]interface{}, len(m.Topics))
		for i := range m.Topics {
			items[i] = m.Topics[i]
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
	// ClusterAuthorizedOperations
	if version >= 8 && version <= 10 {
		if err := protocol.WriteInt32(w, m.ClusterAuthorizedOperations); err != nil {
			return err
		}
	}
	// ErrorCode
	if version >= 13 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
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

// Read reads a MetadataResponse from an io.Reader for the given version.
func (m *MetadataResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 13 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 9 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// Brokers
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem MetadataResponseMetadataResponseBroker
			elemR := bytes.NewReader(data)
			// NodeId
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.NodeId = val
			}
			// Host
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Host = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Host = val
				}
			}
			// Port
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.Port = val
			}
			// Rack
			if version >= 1 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Rack = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Rack = val
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
				var tempElem MetadataResponseMetadataResponseBroker
				// NodeId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.NodeId = val
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					}
				}
				// Port
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.Port = val
				}
				// Rack
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Rack = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Rack = val
					}
				}
				// NodeId
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.NodeId); err != nil {
						return err
					}
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
							return err
						}
					}
				}
				// Port
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.Port); err != nil {
						return err
					}
				}
				// Rack
				if version >= 1 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.Rack); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.Rack); err != nil {
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
			m.Brokers = make([]MetadataResponseMetadataResponseBroker, len(decoded))
			for i, item := range decoded {
				m.Brokers[i] = item.(MetadataResponseMetadataResponseBroker)
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
				var tempElem MetadataResponseMetadataResponseBroker
				// NodeId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.NodeId = val
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					}
				}
				// Port
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.Port = val
				}
				// Rack
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Rack = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Rack = val
					}
				}
				// NodeId
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.NodeId); err != nil {
						return err
					}
				}
				// Host
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
							return err
						}
					}
				}
				// Port
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.Port); err != nil {
						return err
					}
				}
				// Rack
				if version >= 1 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.Rack); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.Rack); err != nil {
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
			m.Brokers = make([]MetadataResponseMetadataResponseBroker, len(decoded))
			for i, item := range decoded {
				m.Brokers[i] = item.(MetadataResponseMetadataResponseBroker)
			}
		}
	}
	// ClusterId
	if version >= 2 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		}
	}
	// ControllerId
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ControllerId = val
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem MetadataResponseMetadataResponseTopic
			elemR := bytes.NewReader(data)
			// ErrorCode
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ErrorCode = val
			}
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				}
			}
			// TopicId
			if version >= 10 && version <= 999 {
				val, err := protocol.ReadUUID(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.TopicId = val
			}
			// IsInternal
			if version >= 1 && version <= 999 {
				val, err := protocol.ReadBool(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.IsInternal = val
			}
			// Partitions
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// TopicAuthorizedOperations
			if version >= 8 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.TopicAuthorizedOperations = val
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
				var tempElem MetadataResponseMetadataResponseTopic
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// TopicId
				if version >= 10 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.TopicId = val
				}
				// IsInternal
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					tempElem.IsInternal = val
				}
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem MetadataResponseMetadataResponsePartition
						elemR := bytes.NewReader(data)
						// ErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ErrorCode = val
						}
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// LeaderId
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderId = val
						}
						// LeaderEpoch
						if version >= 7 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderEpoch = val
						}
						// ReplicaNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ReplicaNodes = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ReplicaNodes = val
							}
						}
						// IsrNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.IsrNodes = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.IsrNodes = val
							}
						}
						// OfflineReplicas
						if version >= 5 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.OfflineReplicas = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.OfflineReplicas = val
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
							var tempElem MetadataResponseMetadataResponsePartition
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 7 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 7 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
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
						tempElem.Partitions = make([]MetadataResponseMetadataResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(MetadataResponseMetadataResponsePartition)
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
							var tempElem MetadataResponseMetadataResponsePartition
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 7 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 7 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
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
						tempElem.Partitions = make([]MetadataResponseMetadataResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(MetadataResponseMetadataResponsePartition)
						}
					}
				}
				// TopicAuthorizedOperations
				if version >= 8 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.TopicAuthorizedOperations = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// TopicId
				if version >= 10 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
						return err
					}
				}
				// IsInternal
				if version >= 1 && version <= 999 {
					if err := protocol.WriteBool(elemW, tempElem.IsInternal); err != nil {
						return err
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Partitions) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions))); err != nil {
							return err
						}
					}
					for i := range tempElem.Partitions {
						// ErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
								return err
							}
						}
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
							}
						}
						// LeaderId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderId); err != nil {
								return err
							}
						}
						// LeaderEpoch
						if version >= 7 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
								return err
							}
						}
						// ReplicaNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].ReplicaNodes); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].ReplicaNodes); err != nil {
									return err
								}
							}
						}
						// IsrNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].IsrNodes); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].IsrNodes); err != nil {
									return err
								}
							}
						}
						// OfflineReplicas
						if version >= 5 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].OfflineReplicas); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].OfflineReplicas); err != nil {
									return err
								}
							}
						}
					}
				}
				// TopicAuthorizedOperations
				if version >= 8 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.TopicAuthorizedOperations); err != nil {
						return err
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
			m.Topics = make([]MetadataResponseMetadataResponseTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(MetadataResponseMetadataResponseTopic)
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
				var tempElem MetadataResponseMetadataResponseTopic
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// TopicId
				if version >= 10 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.TopicId = val
				}
				// IsInternal
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					tempElem.IsInternal = val
				}
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem MetadataResponseMetadataResponsePartition
						elemR := bytes.NewReader(data)
						// ErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ErrorCode = val
						}
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// LeaderId
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderId = val
						}
						// LeaderEpoch
						if version >= 7 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderEpoch = val
						}
						// ReplicaNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ReplicaNodes = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ReplicaNodes = val
							}
						}
						// IsrNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.IsrNodes = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.IsrNodes = val
							}
						}
						// OfflineReplicas
						if version >= 5 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.OfflineReplicas = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.OfflineReplicas = val
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
							var tempElem MetadataResponseMetadataResponsePartition
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 7 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 7 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
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
						tempElem.Partitions = make([]MetadataResponseMetadataResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(MetadataResponseMetadataResponsePartition)
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
							var tempElem MetadataResponseMetadataResponsePartition
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.ErrorCode = val
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 7 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.ReplicaNodes = val
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.IsrNodes = val
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.OfflineReplicas = val
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
									return err
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 7 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// ReplicaNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.ReplicaNodes); err != nil {
										return err
									}
								}
							}
							// IsrNodes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.IsrNodes); err != nil {
										return err
									}
								}
							}
							// OfflineReplicas
							if version >= 5 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.OfflineReplicas); err != nil {
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
						tempElem.Partitions = make([]MetadataResponseMetadataResponsePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(MetadataResponseMetadataResponsePartition)
						}
					}
				}
				// TopicAuthorizedOperations
				if version >= 8 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.TopicAuthorizedOperations = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// TopicId
				if version >= 10 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
						return err
					}
				}
				// IsInternal
				if version >= 1 && version <= 999 {
					if err := protocol.WriteBool(elemW, tempElem.IsInternal); err != nil {
						return err
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Partitions) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions))); err != nil {
							return err
						}
					}
					for i := range tempElem.Partitions {
						// ErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
								return err
							}
						}
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
							}
						}
						// LeaderId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderId); err != nil {
								return err
							}
						}
						// LeaderEpoch
						if version >= 7 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
								return err
							}
						}
						// ReplicaNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].ReplicaNodes); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].ReplicaNodes); err != nil {
									return err
								}
							}
						}
						// IsrNodes
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].IsrNodes); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].IsrNodes); err != nil {
									return err
								}
							}
						}
						// OfflineReplicas
						if version >= 5 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].OfflineReplicas); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].OfflineReplicas); err != nil {
									return err
								}
							}
						}
					}
				}
				// TopicAuthorizedOperations
				if version >= 8 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.TopicAuthorizedOperations); err != nil {
						return err
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
			m.Topics = make([]MetadataResponseMetadataResponseTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(MetadataResponseMetadataResponseTopic)
			}
		}
	}
	// ClusterAuthorizedOperations
	if version >= 8 && version <= 10 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ClusterAuthorizedOperations = val
	}
	// ErrorCode
	if version >= 13 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// MetadataResponseMetadataResponseBroker represents A list of brokers present in the cluster..
type MetadataResponseMetadataResponseBroker struct {
	// The broker ID.
	NodeId int32 `json:"nodeid" versions:"0-999"`
	// The broker hostname.
	Host string `json:"host" versions:"0-999"`
	// The broker port.
	Port int32 `json:"port" versions:"0-999"`
	// The rack of the broker, or null if it has not been assigned to a rack.
	Rack *string `json:"rack" versions:"1-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for MetadataResponseMetadataResponseBroker.
func (m *MetadataResponseMetadataResponseBroker) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for MetadataResponseMetadataResponseBroker.
func (m *MetadataResponseMetadataResponseBroker) readTaggedFields(r io.Reader, version int16) error {
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

// MetadataResponseMetadataResponseTopic represents Each topic in the response..
type MetadataResponseMetadataResponseTopic struct {
	// The topic error, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
	Name *string `json:"name" versions:"0-999"`
	// The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
	TopicId uuid.UUID `json:"topicid" versions:"10-999"`
	// True if the topic is internal.
	IsInternal bool `json:"isinternal" versions:"1-999"`
	// Each partition in the topic.
	Partitions []MetadataResponseMetadataResponsePartition `json:"partitions" versions:"0-999"`
	// 32-bit bitfield to represent authorized operations for this topic.
	TopicAuthorizedOperations int32 `json:"topicauthorizedoperations" versions:"8-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for MetadataResponseMetadataResponseTopic.
func (m *MetadataResponseMetadataResponseTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for MetadataResponseMetadataResponseTopic.
func (m *MetadataResponseMetadataResponseTopic) readTaggedFields(r io.Reader, version int16) error {
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

// MetadataResponseMetadataResponsePartition represents Each partition in the topic..
type MetadataResponseMetadataResponsePartition struct {
	// The partition error, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The ID of the leader broker.
	LeaderId int32 `json:"leaderid" versions:"0-999"`
	// The leader epoch of this partition.
	LeaderEpoch int32 `json:"leaderepoch" versions:"7-999"`
	// The set of all nodes that host this partition.
	ReplicaNodes []int32 `json:"replicanodes" versions:"0-999"`
	// The set of nodes that are in sync with the leader for this partition.
	IsrNodes []int32 `json:"isrnodes" versions:"0-999"`
	// The set of offline replicas of this partition.
	OfflineReplicas []int32 `json:"offlinereplicas" versions:"5-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for MetadataResponseMetadataResponsePartition.
func (m *MetadataResponseMetadataResponsePartition) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for MetadataResponseMetadataResponsePartition.
func (m *MetadataResponseMetadataResponsePartition) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for MetadataResponse.
func (m *MetadataResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for MetadataResponse.
func (m *MetadataResponse) readTaggedFields(r io.Reader, version int16) error {
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
