package assignreplicastodirs

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AssignReplicasToDirsResponseApiKey        = 73
	AssignReplicasToDirsResponseHeaderVersion = 1
)

// AssignReplicasToDirsResponse represents a response message.
type AssignReplicasToDirsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The top level response error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The list of directories and their assigned partitions.
	Directories []AssignReplicasToDirsResponseDirectoryData `json:"directories" versions:"0-999"`
}

// Encode encodes a AssignReplicasToDirsResponse to a byte slice for the given version.
func (m *AssignReplicasToDirsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AssignReplicasToDirsResponse from a byte slice for the given version.
func (m *AssignReplicasToDirsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AssignReplicasToDirsResponse to an io.Writer for the given version.
func (m *AssignReplicasToDirsResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
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
	// Directories
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(AssignReplicasToDirsResponseDirectoryData)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Id
			if version >= 0 && version <= 999 {
				if err := protocol.WriteUUID(elemW, structItem.Id); err != nil {
					return nil, err
				}
			}
			// Topics
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Topics) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Topics))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Topics {
					// TopicId
					if version >= 0 && version <= 999 {
						if err := protocol.WriteUUID(elemW, structItem.Topics[i].TopicId); err != nil {
							return nil, err
						}
					}
					// Partitions
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Topics[i].Partitions) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Topics[i].Partitions))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Topics[i].Partitions {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Topics[i].Partitions[i].PartitionIndex); err != nil {
									return nil, err
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, structItem.Topics[i].Partitions[i].ErrorCode); err != nil {
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
		items := make([]interface{}, len(m.Directories))
		for i := range m.Directories {
			items[i] = m.Directories[i]
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
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a AssignReplicasToDirsResponse from an io.Reader for the given version.
func (m *AssignReplicasToDirsResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
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
	// Directories
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem AssignReplicasToDirsResponseDirectoryData
			elemR := bytes.NewReader(data)
			// Id
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadUUID(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.Id = val
			}
			// Topics
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
				var tempElem AssignReplicasToDirsResponseDirectoryData
				// Id
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.Id = val
				}
				// Topics
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AssignReplicasToDirsResponseTopicData
						elemR := bytes.NewReader(data)
						// TopicId
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadUUID(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.TopicId = val
						}
						// Partitions
						if version >= 0 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
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
							var tempElem AssignReplicasToDirsResponseTopicData
							// TopicId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.TopicId = val
							}
							// Partitions
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AssignReplicasToDirsResponsePartitionData
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// ErrorCode
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ErrorCode = val
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
										var tempElem AssignReplicasToDirsResponsePartitionData
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
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
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]AssignReplicasToDirsResponsePartitionData, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(AssignReplicasToDirsResponsePartitionData)
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
										var tempElem AssignReplicasToDirsResponsePartitionData
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
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
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]AssignReplicasToDirsResponsePartitionData, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(AssignReplicasToDirsResponsePartitionData)
									}
								}
							}
							// TopicId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
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
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// ErrorCode
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
											return err
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
						tempElem.Topics = make([]AssignReplicasToDirsResponseTopicData, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(AssignReplicasToDirsResponseTopicData)
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
							var tempElem AssignReplicasToDirsResponseTopicData
							// TopicId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.TopicId = val
							}
							// Partitions
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AssignReplicasToDirsResponsePartitionData
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// ErrorCode
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ErrorCode = val
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
										var tempElem AssignReplicasToDirsResponsePartitionData
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
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
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]AssignReplicasToDirsResponsePartitionData, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(AssignReplicasToDirsResponsePartitionData)
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
										var tempElem AssignReplicasToDirsResponsePartitionData
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
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
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]AssignReplicasToDirsResponsePartitionData, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(AssignReplicasToDirsResponsePartitionData)
									}
								}
							}
							// TopicId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
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
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// ErrorCode
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
											return err
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
						tempElem.Topics = make([]AssignReplicasToDirsResponseTopicData, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(AssignReplicasToDirsResponseTopicData)
						}
					}
				}
				// Id
				if version >= 0 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.Id); err != nil {
						return err
					}
				}
				// Topics
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Topics) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topics))); err != nil {
							return err
						}
					}
					for i := range tempElem.Topics {
						// TopicId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteUUID(elemW, tempElem.Topics[i].TopicId); err != nil {
								return err
							}
						}
						// Partitions
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Topics[i].Partitions) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topics[i].Partitions))); err != nil {
									return err
								}
							}
							for i := range tempElem.Topics[i].Partitions {
								// PartitionIndex
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Topics[i].Partitions[i].PartitionIndex); err != nil {
										return err
									}
								}
								// ErrorCode
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Topics[i].Partitions[i].ErrorCode); err != nil {
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
			m.Directories = make([]AssignReplicasToDirsResponseDirectoryData, len(decoded))
			for i, item := range decoded {
				m.Directories[i] = item.(AssignReplicasToDirsResponseDirectoryData)
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
				var tempElem AssignReplicasToDirsResponseDirectoryData
				// Id
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					tempElem.Id = val
				}
				// Topics
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AssignReplicasToDirsResponseTopicData
						elemR := bytes.NewReader(data)
						// TopicId
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadUUID(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.TopicId = val
						}
						// Partitions
						if version >= 0 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
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
							var tempElem AssignReplicasToDirsResponseTopicData
							// TopicId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.TopicId = val
							}
							// Partitions
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AssignReplicasToDirsResponsePartitionData
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// ErrorCode
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ErrorCode = val
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
										var tempElem AssignReplicasToDirsResponsePartitionData
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
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
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]AssignReplicasToDirsResponsePartitionData, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(AssignReplicasToDirsResponsePartitionData)
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
										var tempElem AssignReplicasToDirsResponsePartitionData
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
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
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]AssignReplicasToDirsResponsePartitionData, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(AssignReplicasToDirsResponsePartitionData)
									}
								}
							}
							// TopicId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
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
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// ErrorCode
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
											return err
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
						tempElem.Topics = make([]AssignReplicasToDirsResponseTopicData, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(AssignReplicasToDirsResponseTopicData)
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
							var tempElem AssignReplicasToDirsResponseTopicData
							// TopicId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadUUID(r)
								if err != nil {
									return err
								}
								tempElem.TopicId = val
							}
							// Partitions
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem AssignReplicasToDirsResponsePartitionData
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// ErrorCode
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.ErrorCode = val
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
										var tempElem AssignReplicasToDirsResponsePartitionData
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
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
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]AssignReplicasToDirsResponsePartitionData, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(AssignReplicasToDirsResponsePartitionData)
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
										var tempElem AssignReplicasToDirsResponsePartitionData
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
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
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
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
									tempElem.Partitions = make([]AssignReplicasToDirsResponsePartitionData, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(AssignReplicasToDirsResponsePartitionData)
									}
								}
							}
							// TopicId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteUUID(elemW, tempElem.TopicId); err != nil {
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
									// PartitionIndex
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
											return err
										}
									}
									// ErrorCode
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].ErrorCode); err != nil {
											return err
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
						tempElem.Topics = make([]AssignReplicasToDirsResponseTopicData, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(AssignReplicasToDirsResponseTopicData)
						}
					}
				}
				// Id
				if version >= 0 && version <= 999 {
					if err := protocol.WriteUUID(elemW, tempElem.Id); err != nil {
						return err
					}
				}
				// Topics
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Topics) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topics))); err != nil {
							return err
						}
					}
					for i := range tempElem.Topics {
						// TopicId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteUUID(elemW, tempElem.Topics[i].TopicId); err != nil {
								return err
							}
						}
						// Partitions
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Topics[i].Partitions) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topics[i].Partitions))); err != nil {
									return err
								}
							}
							for i := range tempElem.Topics[i].Partitions {
								// PartitionIndex
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Topics[i].Partitions[i].PartitionIndex); err != nil {
										return err
									}
								}
								// ErrorCode
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Topics[i].Partitions[i].ErrorCode); err != nil {
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
			m.Directories = make([]AssignReplicasToDirsResponseDirectoryData, len(decoded))
			for i, item := range decoded {
				m.Directories[i] = item.(AssignReplicasToDirsResponseDirectoryData)
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

// AssignReplicasToDirsResponseDirectoryData represents The list of directories and their assigned partitions..
type AssignReplicasToDirsResponseDirectoryData struct {
	// The ID of the directory.
	Id uuid.UUID `json:"id" versions:"0-999"`
	// The list of topics and their assigned partitions.
	Topics []AssignReplicasToDirsResponseTopicData `json:"topics" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AssignReplicasToDirsResponseDirectoryData.
func (m *AssignReplicasToDirsResponseDirectoryData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AssignReplicasToDirsResponseDirectoryData.
func (m *AssignReplicasToDirsResponseDirectoryData) readTaggedFields(r io.Reader, version int16) error {
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

// AssignReplicasToDirsResponseTopicData represents The list of topics and their assigned partitions..
type AssignReplicasToDirsResponseTopicData struct {
	// The ID of the assigned topic.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// The list of assigned partitions.
	Partitions []AssignReplicasToDirsResponsePartitionData `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AssignReplicasToDirsResponseTopicData.
func (m *AssignReplicasToDirsResponseTopicData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AssignReplicasToDirsResponseTopicData.
func (m *AssignReplicasToDirsResponseTopicData) readTaggedFields(r io.Reader, version int16) error {
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

// AssignReplicasToDirsResponsePartitionData represents The list of assigned partitions..
type AssignReplicasToDirsResponsePartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The partition level error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AssignReplicasToDirsResponsePartitionData.
func (m *AssignReplicasToDirsResponsePartitionData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AssignReplicasToDirsResponsePartitionData.
func (m *AssignReplicasToDirsResponsePartitionData) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for AssignReplicasToDirsResponse.
func (m *AssignReplicasToDirsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AssignReplicasToDirsResponse.
func (m *AssignReplicasToDirsResponse) readTaggedFields(r io.Reader, version int16) error {
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
