package shareacknowledge

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ShareAcknowledgeRequestApiKey        = 79
	ShareAcknowledgeRequestHeaderVersion = 1
)

// ShareAcknowledgeRequest represents a request message.
type ShareAcknowledgeRequest struct {
	// The group identifier.
	GroupId *string `json:"groupid" versions:"0-999"`
	// The member ID.
	MemberId *string `json:"memberid" versions:"0-999"`
	// The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
	ShareSessionEpoch int32 `json:"sharesessionepoch" versions:"0-999"`
	// Whether Renew type acknowledgements present in AcknowledgementBatches.
	IsRenewAck bool `json:"isrenewack" versions:"2-999"`
	// The topics containing records to acknowledge.
	Topics []ShareAcknowledgeRequestAcknowledgeTopic `json:"topics" versions:"0-999"`
}

// Encode encodes a ShareAcknowledgeRequest to a byte slice for the given version.
func (m *ShareAcknowledgeRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ShareAcknowledgeRequest from a byte slice for the given version.
func (m *ShareAcknowledgeRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ShareAcknowledgeRequest to an io.Writer for the given version.
func (m *ShareAcknowledgeRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.GroupId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.GroupId); err != nil {
				return err
			}
		}
	}
	// MemberId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.MemberId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.MemberId); err != nil {
				return err
			}
		}
	}
	// ShareSessionEpoch
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ShareSessionEpoch); err != nil {
			return err
		}
	}
	// IsRenewAck
	if version >= 2 && version <= 999 {
		if err := protocol.WriteBool(w, m.IsRenewAck); err != nil {
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
			structItem, ok := item.(ShareAcknowledgeRequestAcknowledgeTopic)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// TopicId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteUUID(elemW, structItem.TopicId); err != nil {
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
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].PartitionIndex); err != nil {
							return nil, err
						}
					}
					// AcknowledgementBatches
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Partitions[i].AcknowledgementBatches) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions[i].AcknowledgementBatches))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Partitions[i].AcknowledgementBatches {
							// FirstOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].AcknowledgementBatches[i].FirstOffset); err != nil {
									return nil, err
								}
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].AcknowledgementBatches[i].LastOffset); err != nil {
									return nil, err
								}
							}
							// AcknowledgeTypes
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt8Array(elemW, structItem.Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteInt8Array(elemW, structItem.Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
										return nil, err
									}
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
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a ShareAcknowledgeRequest from an io.Reader for the given version.
func (m *ShareAcknowledgeRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		}
	}
	// MemberId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.MemberId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.MemberId = val
		}
	}
	// ShareSessionEpoch
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ShareSessionEpoch = val
	}
	// IsRenewAck
	if version >= 2 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IsRenewAck = val
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ShareAcknowledgeRequestAcknowledgeTopic
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
				var tempElem ShareAcknowledgeRequestAcknowledgeTopic
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
						var elem ShareAcknowledgeRequestAcknowledgePartition
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// AcknowledgementBatches
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
							var tempElem ShareAcknowledgeRequestAcknowledgePartition
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// AcknowledgementBatches
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ShareAcknowledgeRequestAcknowledgementBatch
									elemR := bytes.NewReader(data)
									// FirstOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastOffset = val
									}
									// AcknowledgeTypes
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactInt8Array(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.AcknowledgeTypes = val
										} else {
											val, err := protocol.ReadInt8Array(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.AcknowledgeTypes = val
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
										var tempElem ShareAcknowledgeRequestAcknowledgementBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											} else {
												val, err := protocol.ReadInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											}
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
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
									tempElem.AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, len(decoded))
									for i, item := range decoded {
										tempElem.AcknowledgementBatches[i] = item.(ShareAcknowledgeRequestAcknowledgementBatch)
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
										var tempElem ShareAcknowledgeRequestAcknowledgementBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											} else {
												val, err := protocol.ReadInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											}
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
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
									tempElem.AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, len(decoded))
									for i, item := range decoded {
										tempElem.AcknowledgementBatches[i] = item.(ShareAcknowledgeRequestAcknowledgementBatch)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// AcknowledgementBatches
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.AcknowledgementBatches) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.AcknowledgementBatches))); err != nil {
										return err
									}
								}
								for i := range tempElem.AcknowledgementBatches {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcknowledgementBatches[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcknowledgementBatches[i].LastOffset); err != nil {
											return err
										}
									}
									// AcknowledgeTypes
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
												return err
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
						tempElem.Partitions = make([]ShareAcknowledgeRequestAcknowledgePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ShareAcknowledgeRequestAcknowledgePartition)
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
							var tempElem ShareAcknowledgeRequestAcknowledgePartition
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// AcknowledgementBatches
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ShareAcknowledgeRequestAcknowledgementBatch
									elemR := bytes.NewReader(data)
									// FirstOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastOffset = val
									}
									// AcknowledgeTypes
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactInt8Array(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.AcknowledgeTypes = val
										} else {
											val, err := protocol.ReadInt8Array(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.AcknowledgeTypes = val
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
										var tempElem ShareAcknowledgeRequestAcknowledgementBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											} else {
												val, err := protocol.ReadInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											}
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
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
									tempElem.AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, len(decoded))
									for i, item := range decoded {
										tempElem.AcknowledgementBatches[i] = item.(ShareAcknowledgeRequestAcknowledgementBatch)
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
										var tempElem ShareAcknowledgeRequestAcknowledgementBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											} else {
												val, err := protocol.ReadInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											}
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
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
									tempElem.AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, len(decoded))
									for i, item := range decoded {
										tempElem.AcknowledgementBatches[i] = item.(ShareAcknowledgeRequestAcknowledgementBatch)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// AcknowledgementBatches
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.AcknowledgementBatches) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.AcknowledgementBatches))); err != nil {
										return err
									}
								}
								for i := range tempElem.AcknowledgementBatches {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcknowledgementBatches[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcknowledgementBatches[i].LastOffset); err != nil {
											return err
										}
									}
									// AcknowledgeTypes
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
												return err
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
						tempElem.Partitions = make([]ShareAcknowledgeRequestAcknowledgePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ShareAcknowledgeRequestAcknowledgePartition)
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
						// AcknowledgementBatches
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].AcknowledgementBatches) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].AcknowledgementBatches))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].AcknowledgementBatches {
								// FirstOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AcknowledgementBatches[i].FirstOffset); err != nil {
										return err
									}
								}
								// LastOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AcknowledgementBatches[i].LastOffset); err != nil {
										return err
									}
								}
								// AcknowledgeTypes
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactInt8Array(elemW, tempElem.Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt8Array(elemW, tempElem.Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
											return err
										}
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
			m.Topics = make([]ShareAcknowledgeRequestAcknowledgeTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(ShareAcknowledgeRequestAcknowledgeTopic)
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
				var tempElem ShareAcknowledgeRequestAcknowledgeTopic
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
						var elem ShareAcknowledgeRequestAcknowledgePartition
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// AcknowledgementBatches
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
							var tempElem ShareAcknowledgeRequestAcknowledgePartition
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// AcknowledgementBatches
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ShareAcknowledgeRequestAcknowledgementBatch
									elemR := bytes.NewReader(data)
									// FirstOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastOffset = val
									}
									// AcknowledgeTypes
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactInt8Array(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.AcknowledgeTypes = val
										} else {
											val, err := protocol.ReadInt8Array(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.AcknowledgeTypes = val
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
										var tempElem ShareAcknowledgeRequestAcknowledgementBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											} else {
												val, err := protocol.ReadInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											}
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
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
									tempElem.AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, len(decoded))
									for i, item := range decoded {
										tempElem.AcknowledgementBatches[i] = item.(ShareAcknowledgeRequestAcknowledgementBatch)
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
										var tempElem ShareAcknowledgeRequestAcknowledgementBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											} else {
												val, err := protocol.ReadInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											}
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
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
									tempElem.AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, len(decoded))
									for i, item := range decoded {
										tempElem.AcknowledgementBatches[i] = item.(ShareAcknowledgeRequestAcknowledgementBatch)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// AcknowledgementBatches
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.AcknowledgementBatches) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.AcknowledgementBatches))); err != nil {
										return err
									}
								}
								for i := range tempElem.AcknowledgementBatches {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcknowledgementBatches[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcknowledgementBatches[i].LastOffset); err != nil {
											return err
										}
									}
									// AcknowledgeTypes
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
												return err
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
						tempElem.Partitions = make([]ShareAcknowledgeRequestAcknowledgePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ShareAcknowledgeRequestAcknowledgePartition)
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
							var tempElem ShareAcknowledgeRequestAcknowledgePartition
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// AcknowledgementBatches
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ShareAcknowledgeRequestAcknowledgementBatch
									elemR := bytes.NewReader(data)
									// FirstOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.FirstOffset = val
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.LastOffset = val
									}
									// AcknowledgeTypes
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactInt8Array(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.AcknowledgeTypes = val
										} else {
											val, err := protocol.ReadInt8Array(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.AcknowledgeTypes = val
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
										var tempElem ShareAcknowledgeRequestAcknowledgementBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											} else {
												val, err := protocol.ReadInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											}
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
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
									tempElem.AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, len(decoded))
									for i, item := range decoded {
										tempElem.AcknowledgementBatches[i] = item.(ShareAcknowledgeRequestAcknowledgementBatch)
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
										var tempElem ShareAcknowledgeRequestAcknowledgementBatch
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.LastOffset = val
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											} else {
												val, err := protocol.ReadInt8Array(r)
												if err != nil {
													return err
												}
												tempElem.AcknowledgeTypes = val
											}
										}
										// FirstOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.FirstOffset); err != nil {
												return err
											}
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.LastOffset); err != nil {
												return err
											}
										}
										// AcknowledgeTypes
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgeTypes); err != nil {
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
									tempElem.AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, len(decoded))
									for i, item := range decoded {
										tempElem.AcknowledgementBatches[i] = item.(ShareAcknowledgeRequestAcknowledgementBatch)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// AcknowledgementBatches
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.AcknowledgementBatches) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.AcknowledgementBatches))); err != nil {
										return err
									}
								}
								for i := range tempElem.AcknowledgementBatches {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcknowledgementBatches[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcknowledgementBatches[i].LastOffset); err != nil {
											return err
										}
									}
									// AcknowledgeTypes
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactInt8Array(elemW, tempElem.AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteInt8Array(elemW, tempElem.AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
												return err
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
						tempElem.Partitions = make([]ShareAcknowledgeRequestAcknowledgePartition, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ShareAcknowledgeRequestAcknowledgePartition)
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
						// AcknowledgementBatches
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].AcknowledgementBatches) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].AcknowledgementBatches))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].AcknowledgementBatches {
								// FirstOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AcknowledgementBatches[i].FirstOffset); err != nil {
										return err
									}
								}
								// LastOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AcknowledgementBatches[i].LastOffset); err != nil {
										return err
									}
								}
								// AcknowledgeTypes
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactInt8Array(elemW, tempElem.Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt8Array(elemW, tempElem.Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes); err != nil {
											return err
										}
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
			m.Topics = make([]ShareAcknowledgeRequestAcknowledgeTopic, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(ShareAcknowledgeRequestAcknowledgeTopic)
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

// ShareAcknowledgeRequestAcknowledgeTopic represents The topics containing records to acknowledge..
type ShareAcknowledgeRequestAcknowledgeTopic struct {
	// The unique topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// The partitions containing records to acknowledge.
	Partitions []ShareAcknowledgeRequestAcknowledgePartition `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ShareAcknowledgeRequestAcknowledgeTopic.
func (m *ShareAcknowledgeRequestAcknowledgeTopic) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareAcknowledgeRequestAcknowledgeTopic.
func (m *ShareAcknowledgeRequestAcknowledgeTopic) readTaggedFields(r io.Reader, version int16) error {
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

// ShareAcknowledgeRequestAcknowledgePartition represents The partitions containing records to acknowledge..
type ShareAcknowledgeRequestAcknowledgePartition struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// Record batches to acknowledge.
	AcknowledgementBatches []ShareAcknowledgeRequestAcknowledgementBatch `json:"acknowledgementbatches" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ShareAcknowledgeRequestAcknowledgePartition.
func (m *ShareAcknowledgeRequestAcknowledgePartition) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareAcknowledgeRequestAcknowledgePartition.
func (m *ShareAcknowledgeRequestAcknowledgePartition) readTaggedFields(r io.Reader, version int16) error {
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

// ShareAcknowledgeRequestAcknowledgementBatch represents Record batches to acknowledge..
type ShareAcknowledgeRequestAcknowledgementBatch struct {
	// First offset of batch of records to acknowledge.
	FirstOffset int64 `json:"firstoffset" versions:"0-999"`
	// Last offset (inclusive) of batch of records to acknowledge.
	LastOffset int64 `json:"lastoffset" versions:"0-999"`
	// Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
	AcknowledgeTypes []int8 `json:"acknowledgetypes" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ShareAcknowledgeRequestAcknowledgementBatch.
func (m *ShareAcknowledgeRequestAcknowledgementBatch) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareAcknowledgeRequestAcknowledgementBatch.
func (m *ShareAcknowledgeRequestAcknowledgementBatch) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for ShareAcknowledgeRequest.
func (m *ShareAcknowledgeRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareAcknowledgeRequest.
func (m *ShareAcknowledgeRequest) readTaggedFields(r io.Reader, version int16) error {
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
