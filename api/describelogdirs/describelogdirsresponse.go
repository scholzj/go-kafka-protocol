package describelogdirs

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeLogDirsResponseApiKey        = 35
	DescribeLogDirsResponseHeaderVersion = 1
)

// DescribeLogDirsResponse represents a response message.
type DescribeLogDirsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"3-999"`
	// The log directories.
	Results []DescribeLogDirsResponseDescribeLogDirsResult `json:"results" versions:"0-999"`
}

// Encode encodes a DescribeLogDirsResponse to a byte slice for the given version.
func (m *DescribeLogDirsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeLogDirsResponse from a byte slice for the given version.
func (m *DescribeLogDirsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeLogDirsResponse to an io.Writer for the given version.
func (m *DescribeLogDirsResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// ErrorCode
	if version >= 3 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// Results
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DescribeLogDirsResponseDescribeLogDirsResult)
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
			// LogDir
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.LogDir); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.LogDir); err != nil {
						return nil, err
					}
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
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Topics[i].Name); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Topics[i].Name); err != nil {
								return nil, err
							}
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
							// PartitionSize
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Topics[i].Partitions[i].PartitionSize); err != nil {
									return nil, err
								}
							}
							// OffsetLag
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Topics[i].Partitions[i].OffsetLag); err != nil {
									return nil, err
								}
							}
							// IsFutureKey
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, structItem.Topics[i].Partitions[i].IsFutureKey); err != nil {
									return nil, err
								}
							}
						}
					}
				}
			}
			// TotalBytes
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt64(elemW, structItem.TotalBytes); err != nil {
					return nil, err
				}
			}
			// UsableBytes
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt64(elemW, structItem.UsableBytes); err != nil {
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
		items := make([]interface{}, len(m.Results))
		for i := range m.Results {
			items[i] = m.Results[i]
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

// Read reads a DescribeLogDirsResponse from an io.Reader for the given version.
func (m *DescribeLogDirsResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
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
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// Results
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DescribeLogDirsResponseDescribeLogDirsResult
			elemR := bytes.NewReader(data)
			// ErrorCode
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ErrorCode = val
			}
			// LogDir
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.LogDir = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.LogDir = val
				}
			}
			// Topics
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// TotalBytes
			if version >= 4 && version <= 999 {
				val, err := protocol.ReadInt64(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.TotalBytes = val
			}
			// UsableBytes
			if version >= 4 && version <= 999 {
				val, err := protocol.ReadInt64(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.UsableBytes = val
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
				var tempElem DescribeLogDirsResponseDescribeLogDirsResult
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// LogDir
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.LogDir = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.LogDir = val
					}
				}
				// Topics
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeLogDirsResponseDescribeLogDirsTopic
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
							var tempElem DescribeLogDirsResponseDescribeLogDirsTopic
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
							// Partitions
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeLogDirsResponseDescribeLogDirsPartition
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// PartitionSize
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionSize = val
									}
									// OffsetLag
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.OffsetLag = val
									}
									// IsFutureKey
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadBool(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.IsFutureKey = val
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
										var tempElem DescribeLogDirsResponseDescribeLogDirsPartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											tempElem.IsFutureKey = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.PartitionSize); err != nil {
												return err
											}
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.OffsetLag); err != nil {
												return err
											}
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											if err := protocol.WriteBool(elemW, tempElem.IsFutureKey); err != nil {
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
									tempElem.Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeLogDirsResponseDescribeLogDirsPartition)
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
										var tempElem DescribeLogDirsResponseDescribeLogDirsPartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											tempElem.IsFutureKey = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.PartitionSize); err != nil {
												return err
											}
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.OffsetLag); err != nil {
												return err
											}
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											if err := protocol.WriteBool(elemW, tempElem.IsFutureKey); err != nil {
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
									tempElem.Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeLogDirsResponseDescribeLogDirsPartition)
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
									// PartitionSize
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].PartitionSize); err != nil {
											return err
										}
									}
									// OffsetLag
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].OffsetLag); err != nil {
											return err
										}
									}
									// IsFutureKey
									if version >= 0 && version <= 999 {
										if err := protocol.WriteBool(elemW, tempElem.Partitions[i].IsFutureKey); err != nil {
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
						tempElem.Topics = make([]DescribeLogDirsResponseDescribeLogDirsTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeLogDirsResponseDescribeLogDirsTopic)
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
							var tempElem DescribeLogDirsResponseDescribeLogDirsTopic
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
							// Partitions
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeLogDirsResponseDescribeLogDirsPartition
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// PartitionSize
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionSize = val
									}
									// OffsetLag
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.OffsetLag = val
									}
									// IsFutureKey
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadBool(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.IsFutureKey = val
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
										var tempElem DescribeLogDirsResponseDescribeLogDirsPartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											tempElem.IsFutureKey = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.PartitionSize); err != nil {
												return err
											}
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.OffsetLag); err != nil {
												return err
											}
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											if err := protocol.WriteBool(elemW, tempElem.IsFutureKey); err != nil {
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
									tempElem.Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeLogDirsResponseDescribeLogDirsPartition)
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
										var tempElem DescribeLogDirsResponseDescribeLogDirsPartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											tempElem.IsFutureKey = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.PartitionSize); err != nil {
												return err
											}
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.OffsetLag); err != nil {
												return err
											}
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											if err := protocol.WriteBool(elemW, tempElem.IsFutureKey); err != nil {
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
									tempElem.Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeLogDirsResponseDescribeLogDirsPartition)
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
									// PartitionSize
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].PartitionSize); err != nil {
											return err
										}
									}
									// OffsetLag
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].OffsetLag); err != nil {
											return err
										}
									}
									// IsFutureKey
									if version >= 0 && version <= 999 {
										if err := protocol.WriteBool(elemW, tempElem.Partitions[i].IsFutureKey); err != nil {
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
						tempElem.Topics = make([]DescribeLogDirsResponseDescribeLogDirsTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeLogDirsResponseDescribeLogDirsTopic)
						}
					}
				}
				// TotalBytes
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.TotalBytes = val
				}
				// UsableBytes
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.UsableBytes = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// LogDir
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.LogDir); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.LogDir); err != nil {
							return err
						}
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
						// Name
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
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
								// PartitionSize
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Topics[i].Partitions[i].PartitionSize); err != nil {
										return err
									}
								}
								// OffsetLag
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Topics[i].Partitions[i].OffsetLag); err != nil {
										return err
									}
								}
								// IsFutureKey
								if version >= 0 && version <= 999 {
									if err := protocol.WriteBool(elemW, tempElem.Topics[i].Partitions[i].IsFutureKey); err != nil {
										return err
									}
								}
							}
						}
					}
				}
				// TotalBytes
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.TotalBytes); err != nil {
						return err
					}
				}
				// UsableBytes
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.UsableBytes); err != nil {
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
			m.Results = make([]DescribeLogDirsResponseDescribeLogDirsResult, len(decoded))
			for i, item := range decoded {
				m.Results[i] = item.(DescribeLogDirsResponseDescribeLogDirsResult)
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
				var tempElem DescribeLogDirsResponseDescribeLogDirsResult
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// LogDir
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.LogDir = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.LogDir = val
					}
				}
				// Topics
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeLogDirsResponseDescribeLogDirsTopic
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
							var tempElem DescribeLogDirsResponseDescribeLogDirsTopic
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
							// Partitions
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeLogDirsResponseDescribeLogDirsPartition
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// PartitionSize
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionSize = val
									}
									// OffsetLag
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.OffsetLag = val
									}
									// IsFutureKey
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadBool(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.IsFutureKey = val
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
										var tempElem DescribeLogDirsResponseDescribeLogDirsPartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											tempElem.IsFutureKey = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.PartitionSize); err != nil {
												return err
											}
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.OffsetLag); err != nil {
												return err
											}
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											if err := protocol.WriteBool(elemW, tempElem.IsFutureKey); err != nil {
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
									tempElem.Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeLogDirsResponseDescribeLogDirsPartition)
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
										var tempElem DescribeLogDirsResponseDescribeLogDirsPartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											tempElem.IsFutureKey = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.PartitionSize); err != nil {
												return err
											}
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.OffsetLag); err != nil {
												return err
											}
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											if err := protocol.WriteBool(elemW, tempElem.IsFutureKey); err != nil {
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
									tempElem.Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeLogDirsResponseDescribeLogDirsPartition)
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
									// PartitionSize
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].PartitionSize); err != nil {
											return err
										}
									}
									// OffsetLag
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].OffsetLag); err != nil {
											return err
										}
									}
									// IsFutureKey
									if version >= 0 && version <= 999 {
										if err := protocol.WriteBool(elemW, tempElem.Partitions[i].IsFutureKey); err != nil {
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
						tempElem.Topics = make([]DescribeLogDirsResponseDescribeLogDirsTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeLogDirsResponseDescribeLogDirsTopic)
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
							var tempElem DescribeLogDirsResponseDescribeLogDirsTopic
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
							// Partitions
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeLogDirsResponseDescribeLogDirsPartition
									elemR := bytes.NewReader(data)
									// PartitionIndex
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionIndex = val
									}
									// PartitionSize
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.PartitionSize = val
									}
									// OffsetLag
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.OffsetLag = val
									}
									// IsFutureKey
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadBool(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.IsFutureKey = val
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
										var tempElem DescribeLogDirsResponseDescribeLogDirsPartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											tempElem.IsFutureKey = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.PartitionSize); err != nil {
												return err
											}
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.OffsetLag); err != nil {
												return err
											}
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											if err := protocol.WriteBool(elemW, tempElem.IsFutureKey); err != nil {
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
									tempElem.Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeLogDirsResponseDescribeLogDirsPartition)
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
										var tempElem DescribeLogDirsResponseDescribeLogDirsPartition
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											tempElem.IsFutureKey = val
										}
										// PartitionIndex
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
												return err
											}
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.PartitionSize); err != nil {
												return err
											}
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.OffsetLag); err != nil {
												return err
											}
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											if err := protocol.WriteBool(elemW, tempElem.IsFutureKey); err != nil {
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
									tempElem.Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, len(decoded))
									for i, item := range decoded {
										tempElem.Partitions[i] = item.(DescribeLogDirsResponseDescribeLogDirsPartition)
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
									// PartitionSize
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].PartitionSize); err != nil {
											return err
										}
									}
									// OffsetLag
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].OffsetLag); err != nil {
											return err
										}
									}
									// IsFutureKey
									if version >= 0 && version <= 999 {
										if err := protocol.WriteBool(elemW, tempElem.Partitions[i].IsFutureKey); err != nil {
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
						tempElem.Topics = make([]DescribeLogDirsResponseDescribeLogDirsTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(DescribeLogDirsResponseDescribeLogDirsTopic)
						}
					}
				}
				// TotalBytes
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.TotalBytes = val
				}
				// UsableBytes
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.UsableBytes = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// LogDir
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.LogDir); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.LogDir); err != nil {
							return err
						}
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
						// Name
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
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
								// PartitionSize
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Topics[i].Partitions[i].PartitionSize); err != nil {
										return err
									}
								}
								// OffsetLag
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Topics[i].Partitions[i].OffsetLag); err != nil {
										return err
									}
								}
								// IsFutureKey
								if version >= 0 && version <= 999 {
									if err := protocol.WriteBool(elemW, tempElem.Topics[i].Partitions[i].IsFutureKey); err != nil {
										return err
									}
								}
							}
						}
					}
				}
				// TotalBytes
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.TotalBytes); err != nil {
						return err
					}
				}
				// UsableBytes
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.UsableBytes); err != nil {
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
			m.Results = make([]DescribeLogDirsResponseDescribeLogDirsResult, len(decoded))
			for i, item := range decoded {
				m.Results[i] = item.(DescribeLogDirsResponseDescribeLogDirsResult)
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

// DescribeLogDirsResponseDescribeLogDirsResult represents The log directories..
type DescribeLogDirsResponseDescribeLogDirsResult struct {
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The absolute log directory path.
	LogDir string `json:"logdir" versions:"0-999"`
	// The topics.
	Topics []DescribeLogDirsResponseDescribeLogDirsTopic `json:"topics" versions:"0-999"`
	// The total size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage.
	TotalBytes int64 `json:"totalbytes" versions:"4-999"`
	// The usable size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage.
	UsableBytes int64 `json:"usablebytes" versions:"4-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeLogDirsResponseDescribeLogDirsResult.
func (m *DescribeLogDirsResponseDescribeLogDirsResult) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeLogDirsResponseDescribeLogDirsResult.
func (m *DescribeLogDirsResponseDescribeLogDirsResult) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeLogDirsResponseDescribeLogDirsTopic represents The topics..
type DescribeLogDirsResponseDescribeLogDirsTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The partitions.
	Partitions []DescribeLogDirsResponseDescribeLogDirsPartition `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeLogDirsResponseDescribeLogDirsTopic.
func (m *DescribeLogDirsResponseDescribeLogDirsTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeLogDirsResponseDescribeLogDirsTopic.
func (m *DescribeLogDirsResponseDescribeLogDirsTopic) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeLogDirsResponseDescribeLogDirsPartition represents The partitions..
type DescribeLogDirsResponseDescribeLogDirsPartition struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The size of the log segments in this partition in bytes.
	PartitionSize int64 `json:"partitionsize" versions:"0-999"`
	// The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition) or current replica's LEO (if it is the future log for the partition).
	OffsetLag int64 `json:"offsetlag" versions:"0-999"`
	// True if this log is created by AlterReplicaLogDirsRequest and will replace the current log of the replica in the future.
	IsFutureKey bool `json:"isfuturekey" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeLogDirsResponseDescribeLogDirsPartition.
func (m *DescribeLogDirsResponseDescribeLogDirsPartition) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeLogDirsResponseDescribeLogDirsPartition.
func (m *DescribeLogDirsResponseDescribeLogDirsPartition) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for DescribeLogDirsResponse.
func (m *DescribeLogDirsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeLogDirsResponse.
func (m *DescribeLogDirsResponse) readTaggedFields(r io.Reader, version int16) error {
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
