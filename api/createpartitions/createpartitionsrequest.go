package createpartitions

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	CreatePartitionsRequestApiKey        = 37
	CreatePartitionsRequestHeaderVersion = 1
)

// CreatePartitionsRequest represents a request message.
type CreatePartitionsRequest struct {
	// Each topic that we want to create new partitions inside.
	Topics []CreatePartitionsRequestCreatePartitionsTopic `json:"topics" versions:"0-999"`
	// The time in ms to wait for the partitions to be created.
	TimeoutMs int32 `json:"timeoutms" versions:"0-999"`
	// If true, then validate the request, but don't actually increase the number of partitions.
	ValidateOnly bool `json:"validateonly" versions:"0-999"`
}

// Encode encodes a CreatePartitionsRequest to a byte slice for the given version.
func (m *CreatePartitionsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a CreatePartitionsRequest from a byte slice for the given version.
func (m *CreatePartitionsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a CreatePartitionsRequest to an io.Writer for the given version.
func (m *CreatePartitionsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Topics
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(CreatePartitionsRequestCreatePartitionsTopic)
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
			// Count
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.Count); err != nil {
					return nil, err
				}
			}
			// Assignments
			if version >= 0 && version <= 999 {
				if structItem.Assignments == nil {
					if isFlexible {
						if err := protocol.WriteVaruint32(elemW, 0); err != nil {
							return nil, err
						}
					} else {
						if err := protocol.WriteInt32(elemW, -1); err != nil {
							return nil, err
						}
					}
				} else {
					if isFlexible {
						length := uint32(len(structItem.Assignments) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return nil, err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(structItem.Assignments))); err != nil {
							return nil, err
						}
					}
					for i := range structItem.Assignments {
						// BrokerIds
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, structItem.Assignments[i].BrokerIds); err != nil {
									return nil, err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, structItem.Assignments[i].BrokerIds); err != nil {
									return nil, err
								}
							}
						}
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
	// TimeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TimeoutMs); err != nil {
			return err
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

// Read reads a CreatePartitionsRequest from an io.Reader for the given version.
func (m *CreatePartitionsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem CreatePartitionsRequestCreatePartitionsTopic
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
			// Count
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.Count = val
			}
			// Assignments
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
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
				var tempElem CreatePartitionsRequestCreatePartitionsTopic
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
				// Count
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.Count = val
				}
				// Assignments
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem CreatePartitionsRequestCreatePartitionsAssignment
						elemR := bytes.NewReader(data)
						// BrokerIds
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.BrokerIds = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.BrokerIds = val
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
						if lengthUint == 0 {
							tempElem.Assignments = nil
							return nil
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
							var tempElem CreatePartitionsRequestCreatePartitionsAssignment
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								}
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.BrokerIds); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.BrokerIds); err != nil {
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
						tempElem.Assignments = make([]CreatePartitionsRequestCreatePartitionsAssignment, len(decoded))
						for i, item := range decoded {
							tempElem.Assignments[i] = item.(CreatePartitionsRequestCreatePartitionsAssignment)
						}
					} else {
						length, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						if length == -1 {
							tempElem.Assignments = nil
							return nil
						}
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem CreatePartitionsRequestCreatePartitionsAssignment
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								}
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.BrokerIds); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.BrokerIds); err != nil {
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
						tempElem.Assignments = make([]CreatePartitionsRequestCreatePartitionsAssignment, len(decoded))
						for i, item := range decoded {
							tempElem.Assignments[i] = item.(CreatePartitionsRequestCreatePartitionsAssignment)
						}
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
				// Count
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.Count); err != nil {
						return err
					}
				}
				// Assignments
				if version >= 0 && version <= 999 {
					if tempElem.Assignments == nil {
						if isFlexible {
							if err := protocol.WriteVaruint32(elemW, 0); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(elemW, -1); err != nil {
								return err
							}
						}
					} else {
						if isFlexible {
							length := uint32(len(tempElem.Assignments) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(tempElem.Assignments))); err != nil {
								return err
							}
						}
						for i := range tempElem.Assignments {
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.Assignments[i].BrokerIds); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.Assignments[i].BrokerIds); err != nil {
										return err
									}
								}
							}
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
			m.Topics = make([]CreatePartitionsRequestCreatePartitionsTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(CreatePartitionsRequestCreatePartitionsTopic)
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
				var tempElem CreatePartitionsRequestCreatePartitionsTopic
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
				// Count
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.Count = val
				}
				// Assignments
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem CreatePartitionsRequestCreatePartitionsAssignment
						elemR := bytes.NewReader(data)
						// BrokerIds
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.BrokerIds = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.BrokerIds = val
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
						if lengthUint == 0 {
							tempElem.Assignments = nil
							return nil
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
							var tempElem CreatePartitionsRequestCreatePartitionsAssignment
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								}
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.BrokerIds); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.BrokerIds); err != nil {
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
						tempElem.Assignments = make([]CreatePartitionsRequestCreatePartitionsAssignment, len(decoded))
						for i, item := range decoded {
							tempElem.Assignments[i] = item.(CreatePartitionsRequestCreatePartitionsAssignment)
						}
					} else {
						length, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						if length == -1 {
							tempElem.Assignments = nil
							return nil
						}
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem CreatePartitionsRequestCreatePartitionsAssignment
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.BrokerIds = val
								}
							}
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.BrokerIds); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.BrokerIds); err != nil {
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
						tempElem.Assignments = make([]CreatePartitionsRequestCreatePartitionsAssignment, len(decoded))
						for i, item := range decoded {
							tempElem.Assignments[i] = item.(CreatePartitionsRequestCreatePartitionsAssignment)
						}
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
				// Count
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.Count); err != nil {
						return err
					}
				}
				// Assignments
				if version >= 0 && version <= 999 {
					if tempElem.Assignments == nil {
						if isFlexible {
							if err := protocol.WriteVaruint32(elemW, 0); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(elemW, -1); err != nil {
								return err
							}
						}
					} else {
						if isFlexible {
							length := uint32(len(tempElem.Assignments) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(tempElem.Assignments))); err != nil {
								return err
							}
						}
						for i := range tempElem.Assignments {
							// BrokerIds
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.Assignments[i].BrokerIds); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.Assignments[i].BrokerIds); err != nil {
										return err
									}
								}
							}
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
			m.Topics = make([]CreatePartitionsRequestCreatePartitionsTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(CreatePartitionsRequestCreatePartitionsTopic)
			}
		}
	}
	// TimeoutMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.TimeoutMs = val
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

// CreatePartitionsRequestCreatePartitionsTopic represents Each topic that we want to create new partitions inside..
type CreatePartitionsRequestCreatePartitionsTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The new partition count.
	Count int32 `json:"count" versions:"0-999"`
	// The new partition assignments.
	Assignments []CreatePartitionsRequestCreatePartitionsAssignment `json:"assignments" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for CreatePartitionsRequestCreatePartitionsTopic.
func (m *CreatePartitionsRequestCreatePartitionsTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreatePartitionsRequestCreatePartitionsTopic.
func (m *CreatePartitionsRequestCreatePartitionsTopic) readTaggedFields(r io.Reader, version int16) error {
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

// CreatePartitionsRequestCreatePartitionsAssignment represents The new partition assignments..
type CreatePartitionsRequestCreatePartitionsAssignment struct {
	// The assigned broker IDs.
	BrokerIds []int32 `json:"brokerids" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for CreatePartitionsRequestCreatePartitionsAssignment.
func (m *CreatePartitionsRequestCreatePartitionsAssignment) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreatePartitionsRequestCreatePartitionsAssignment.
func (m *CreatePartitionsRequestCreatePartitionsAssignment) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for CreatePartitionsRequest.
func (m *CreatePartitionsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for CreatePartitionsRequest.
func (m *CreatePartitionsRequest) readTaggedFields(r io.Reader, version int16) error {
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
