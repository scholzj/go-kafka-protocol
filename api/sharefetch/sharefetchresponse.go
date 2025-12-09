package sharefetch

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ShareFetchResponseApiKey        = 78
	ShareFetchResponseHeaderVersion = 1
)

// ShareFetchResponse represents a response message.
type ShareFetchResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The top-level response error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The top-level error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The time in milliseconds for which the acquired records are locked.
	AcquisitionLockTimeoutMs int32 `json:"acquisitionlocktimeoutms" versions:"1-999"`
	// The response topics.
	Responses []ShareFetchResponseShareFetchableTopicResponse `json:"responses" versions:"0-999"`
	// Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
	NodeEndpoints []ShareFetchResponseNodeEndpoint `json:"nodeendpoints" versions:"0-999"`
}

// Encode encodes a ShareFetchResponse to a byte slice for the given version.
func (m *ShareFetchResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ShareFetchResponse from a byte slice for the given version.
func (m *ShareFetchResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ShareFetchResponse to an io.Writer for the given version.
func (m *ShareFetchResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 2 {
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
	// AcquisitionLockTimeoutMs
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.AcquisitionLockTimeoutMs); err != nil {
			return err
		}
	}
	// Responses
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ShareFetchResponseShareFetchableTopicResponse)
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
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(elemW, structItem.Partitions[i].ErrorCode); err != nil {
							return nil, err
						}
					}
					// ErrorMessage
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.Partitions[i].ErrorMessage); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.Partitions[i].ErrorMessage); err != nil {
								return nil, err
							}
						}
					}
					// AcknowledgeErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(elemW, structItem.Partitions[i].AcknowledgeErrorCode); err != nil {
							return nil, err
						}
					}
					// AcknowledgeErrorMessage
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.Partitions[i].AcknowledgeErrorMessage); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.Partitions[i].AcknowledgeErrorMessage); err != nil {
								return nil, err
							}
						}
					}
					// CurrentLeader
					if version >= 0 && version <= 999 {
						// LeaderId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, structItem.Partitions[i].CurrentLeader.LeaderId); err != nil {
								return nil, err
							}
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, structItem.Partitions[i].CurrentLeader.LeaderEpoch); err != nil {
								return nil, err
							}
						}
					}
					// Records
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableBytes(elemW, structItem.Partitions[i].Records); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableBytes(elemW, structItem.Partitions[i].Records); err != nil {
								return nil, err
							}
						}
					}
					// AcquiredRecords
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Partitions[i].AcquiredRecords) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions[i].AcquiredRecords))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Partitions[i].AcquiredRecords {
							// FirstOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].AcquiredRecords[i].FirstOffset); err != nil {
									return nil, err
								}
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Partitions[i].AcquiredRecords[i].LastOffset); err != nil {
									return nil, err
								}
							}
							// DeliveryCount
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, structItem.Partitions[i].AcquiredRecords[i].DeliveryCount); err != nil {
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
		items := make([]interface{}, len(m.Responses))
		for i := range m.Responses {
			items[i] = m.Responses[i]
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
	// NodeEndpoints
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ShareFetchResponseNodeEndpoint)
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
			if version >= 0 && version <= 999 {
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
		items := make([]interface{}, len(m.NodeEndpoints))
		for i := range m.NodeEndpoints {
			items[i] = m.NodeEndpoints[i]
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

// Read reads a ShareFetchResponse from an io.Reader for the given version.
func (m *ShareFetchResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 2 {
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
	// AcquisitionLockTimeoutMs
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.AcquisitionLockTimeoutMs = val
	}
	// Responses
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ShareFetchResponseShareFetchableTopicResponse
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
				var tempElem ShareFetchResponseShareFetchableTopicResponse
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
						var elem ShareFetchResponsePartitionData
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
						// ErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ErrorMessage = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ErrorMessage = val
							}
						}
						// AcknowledgeErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.AcknowledgeErrorCode = val
						}
						// AcknowledgeErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.AcknowledgeErrorMessage = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.AcknowledgeErrorMessage = val
							}
						}
						// CurrentLeader
						if version >= 0 && version <= 999 {
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.CurrentLeader.LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.CurrentLeader.LeaderEpoch = val
							}
						}
						// Records
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableBytes(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Records = val
							} else {
								val, err := protocol.ReadNullableBytes(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Records = val
							}
						}
						// AcquiredRecords
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
							var tempElem ShareFetchResponsePartitionData
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
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								}
							}
							// AcknowledgeErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.AcknowledgeErrorCode = val
							}
							// AcknowledgeErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.AcknowledgeErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.AcknowledgeErrorMessage = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.CurrentLeader.LeaderId = val
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.CurrentLeader.LeaderEpoch = val
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								}
							}
							// AcquiredRecords
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ShareFetchResponseAcquiredRecords
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
									// DeliveryCount
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryCount = val
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
										var tempElem ShareFetchResponseAcquiredRecords
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.AcquiredRecords = make([]ShareFetchResponseAcquiredRecords, len(decoded))
									for i, item := range decoded {
										tempElem.AcquiredRecords[i] = item.(ShareFetchResponseAcquiredRecords)
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
										var tempElem ShareFetchResponseAcquiredRecords
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.AcquiredRecords = make([]ShareFetchResponseAcquiredRecords, len(decoded))
									for i, item := range decoded {
										tempElem.AcquiredRecords[i] = item.(ShareFetchResponseAcquiredRecords)
									}
								}
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
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								}
							}
							// AcknowledgeErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.AcknowledgeErrorCode); err != nil {
									return err
								}
							}
							// AcknowledgeErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.AcknowledgeErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.AcknowledgeErrorMessage); err != nil {
										return err
									}
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.CurrentLeader.LeaderId); err != nil {
										return err
									}
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.CurrentLeader.LeaderEpoch); err != nil {
										return err
									}
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								}
							}
							// AcquiredRecords
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.AcquiredRecords) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.AcquiredRecords))); err != nil {
										return err
									}
								}
								for i := range tempElem.AcquiredRecords {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcquiredRecords[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcquiredRecords[i].LastOffset); err != nil {
											return err
										}
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.AcquiredRecords[i].DeliveryCount); err != nil {
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
						tempElem.Partitions = make([]ShareFetchResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ShareFetchResponsePartitionData)
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
							var tempElem ShareFetchResponsePartitionData
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
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								}
							}
							// AcknowledgeErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.AcknowledgeErrorCode = val
							}
							// AcknowledgeErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.AcknowledgeErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.AcknowledgeErrorMessage = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.CurrentLeader.LeaderId = val
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.CurrentLeader.LeaderEpoch = val
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								}
							}
							// AcquiredRecords
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ShareFetchResponseAcquiredRecords
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
									// DeliveryCount
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryCount = val
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
										var tempElem ShareFetchResponseAcquiredRecords
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.AcquiredRecords = make([]ShareFetchResponseAcquiredRecords, len(decoded))
									for i, item := range decoded {
										tempElem.AcquiredRecords[i] = item.(ShareFetchResponseAcquiredRecords)
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
										var tempElem ShareFetchResponseAcquiredRecords
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.AcquiredRecords = make([]ShareFetchResponseAcquiredRecords, len(decoded))
									for i, item := range decoded {
										tempElem.AcquiredRecords[i] = item.(ShareFetchResponseAcquiredRecords)
									}
								}
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
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								}
							}
							// AcknowledgeErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.AcknowledgeErrorCode); err != nil {
									return err
								}
							}
							// AcknowledgeErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.AcknowledgeErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.AcknowledgeErrorMessage); err != nil {
										return err
									}
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.CurrentLeader.LeaderId); err != nil {
										return err
									}
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.CurrentLeader.LeaderEpoch); err != nil {
										return err
									}
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								}
							}
							// AcquiredRecords
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.AcquiredRecords) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.AcquiredRecords))); err != nil {
										return err
									}
								}
								for i := range tempElem.AcquiredRecords {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcquiredRecords[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcquiredRecords[i].LastOffset); err != nil {
											return err
										}
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.AcquiredRecords[i].DeliveryCount); err != nil {
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
						tempElem.Partitions = make([]ShareFetchResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ShareFetchResponsePartitionData)
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
						// ErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].ErrorMessage); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].ErrorMessage); err != nil {
									return err
								}
							}
						}
						// AcknowledgeErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].AcknowledgeErrorCode); err != nil {
								return err
							}
						}
						// AcknowledgeErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].AcknowledgeErrorMessage); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].AcknowledgeErrorMessage); err != nil {
									return err
								}
							}
						}
						// CurrentLeader
						if version >= 0 && version <= 999 {
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CurrentLeader.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CurrentLeader.LeaderEpoch); err != nil {
									return err
								}
							}
						}
						// Records
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Partitions[i].Records); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableBytes(elemW, tempElem.Partitions[i].Records); err != nil {
									return err
								}
							}
						}
						// AcquiredRecords
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].AcquiredRecords) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].AcquiredRecords))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].AcquiredRecords {
								// FirstOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AcquiredRecords[i].FirstOffset); err != nil {
										return err
									}
								}
								// LastOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AcquiredRecords[i].LastOffset); err != nil {
										return err
									}
								}
								// DeliveryCount
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].AcquiredRecords[i].DeliveryCount); err != nil {
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
			m.Responses = make([]ShareFetchResponseShareFetchableTopicResponse, len(decoded))
			for i, item := range decoded {
				m.Responses[i] = item.(ShareFetchResponseShareFetchableTopicResponse)
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
				var tempElem ShareFetchResponseShareFetchableTopicResponse
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
						var elem ShareFetchResponsePartitionData
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
						// ErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ErrorMessage = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ErrorMessage = val
							}
						}
						// AcknowledgeErrorCode
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt16(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.AcknowledgeErrorCode = val
						}
						// AcknowledgeErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.AcknowledgeErrorMessage = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.AcknowledgeErrorMessage = val
							}
						}
						// CurrentLeader
						if version >= 0 && version <= 999 {
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.CurrentLeader.LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.CurrentLeader.LeaderEpoch = val
							}
						}
						// Records
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableBytes(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Records = val
							} else {
								val, err := protocol.ReadNullableBytes(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Records = val
							}
						}
						// AcquiredRecords
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
							var tempElem ShareFetchResponsePartitionData
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
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								}
							}
							// AcknowledgeErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.AcknowledgeErrorCode = val
							}
							// AcknowledgeErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.AcknowledgeErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.AcknowledgeErrorMessage = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.CurrentLeader.LeaderId = val
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.CurrentLeader.LeaderEpoch = val
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								}
							}
							// AcquiredRecords
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ShareFetchResponseAcquiredRecords
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
									// DeliveryCount
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryCount = val
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
										var tempElem ShareFetchResponseAcquiredRecords
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.AcquiredRecords = make([]ShareFetchResponseAcquiredRecords, len(decoded))
									for i, item := range decoded {
										tempElem.AcquiredRecords[i] = item.(ShareFetchResponseAcquiredRecords)
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
										var tempElem ShareFetchResponseAcquiredRecords
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.AcquiredRecords = make([]ShareFetchResponseAcquiredRecords, len(decoded))
									for i, item := range decoded {
										tempElem.AcquiredRecords[i] = item.(ShareFetchResponseAcquiredRecords)
									}
								}
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
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								}
							}
							// AcknowledgeErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.AcknowledgeErrorCode); err != nil {
									return err
								}
							}
							// AcknowledgeErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.AcknowledgeErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.AcknowledgeErrorMessage); err != nil {
										return err
									}
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.CurrentLeader.LeaderId); err != nil {
										return err
									}
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.CurrentLeader.LeaderEpoch); err != nil {
										return err
									}
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								}
							}
							// AcquiredRecords
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.AcquiredRecords) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.AcquiredRecords))); err != nil {
										return err
									}
								}
								for i := range tempElem.AcquiredRecords {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcquiredRecords[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcquiredRecords[i].LastOffset); err != nil {
											return err
										}
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.AcquiredRecords[i].DeliveryCount); err != nil {
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
						tempElem.Partitions = make([]ShareFetchResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ShareFetchResponsePartitionData)
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
							var tempElem ShareFetchResponsePartitionData
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
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.ErrorMessage = val
								}
							}
							// AcknowledgeErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								tempElem.AcknowledgeErrorCode = val
							}
							// AcknowledgeErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.AcknowledgeErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.AcknowledgeErrorMessage = val
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.CurrentLeader.LeaderId = val
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									val, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									tempElem.CurrentLeader.LeaderEpoch = val
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									tempElem.Records = val
								}
							}
							// AcquiredRecords
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem ShareFetchResponseAcquiredRecords
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
									// DeliveryCount
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt16(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.DeliveryCount = val
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
										var tempElem ShareFetchResponseAcquiredRecords
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.AcquiredRecords = make([]ShareFetchResponseAcquiredRecords, len(decoded))
									for i, item := range decoded {
										tempElem.AcquiredRecords[i] = item.(ShareFetchResponseAcquiredRecords)
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
										var tempElem ShareFetchResponseAcquiredRecords
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											tempElem.DeliveryCount = val
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
										// DeliveryCount
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.DeliveryCount); err != nil {
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
									tempElem.AcquiredRecords = make([]ShareFetchResponseAcquiredRecords, len(decoded))
									for i, item := range decoded {
										tempElem.AcquiredRecords[i] = item.(ShareFetchResponseAcquiredRecords)
									}
								}
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
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
										return err
									}
								}
							}
							// AcknowledgeErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(elemW, tempElem.AcknowledgeErrorCode); err != nil {
									return err
								}
							}
							// AcknowledgeErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.AcknowledgeErrorMessage); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.AcknowledgeErrorMessage); err != nil {
										return err
									}
								}
							}
							// CurrentLeader
							if version >= 0 && version <= 999 {
								// LeaderId
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.CurrentLeader.LeaderId); err != nil {
										return err
									}
								}
								// LeaderEpoch
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.CurrentLeader.LeaderEpoch); err != nil {
										return err
									}
								}
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableBytes(elemW, tempElem.Records); err != nil {
										return err
									}
								}
							}
							// AcquiredRecords
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.AcquiredRecords) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.AcquiredRecords))); err != nil {
										return err
									}
								}
								for i := range tempElem.AcquiredRecords {
									// FirstOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcquiredRecords[i].FirstOffset); err != nil {
											return err
										}
									}
									// LastOffset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.AcquiredRecords[i].LastOffset); err != nil {
											return err
										}
									}
									// DeliveryCount
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, tempElem.AcquiredRecords[i].DeliveryCount); err != nil {
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
						tempElem.Partitions = make([]ShareFetchResponsePartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(ShareFetchResponsePartitionData)
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
						// ErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].ErrorMessage); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].ErrorMessage); err != nil {
									return err
								}
							}
						}
						// AcknowledgeErrorCode
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].AcknowledgeErrorCode); err != nil {
								return err
							}
						}
						// AcknowledgeErrorMessage
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Partitions[i].AcknowledgeErrorMessage); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Partitions[i].AcknowledgeErrorMessage); err != nil {
									return err
								}
							}
						}
						// CurrentLeader
						if version >= 0 && version <= 999 {
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CurrentLeader.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].CurrentLeader.LeaderEpoch); err != nil {
									return err
								}
							}
						}
						// Records
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableBytes(elemW, tempElem.Partitions[i].Records); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableBytes(elemW, tempElem.Partitions[i].Records); err != nil {
									return err
								}
							}
						}
						// AcquiredRecords
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].AcquiredRecords) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].AcquiredRecords))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].AcquiredRecords {
								// FirstOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AcquiredRecords[i].FirstOffset); err != nil {
										return err
									}
								}
								// LastOffset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Partitions[i].AcquiredRecords[i].LastOffset); err != nil {
										return err
									}
								}
								// DeliveryCount
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt16(elemW, tempElem.Partitions[i].AcquiredRecords[i].DeliveryCount); err != nil {
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
			m.Responses = make([]ShareFetchResponseShareFetchableTopicResponse, len(decoded))
			for i, item := range decoded {
				m.Responses[i] = item.(ShareFetchResponseShareFetchableTopicResponse)
			}
		}
	}
	// NodeEndpoints
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ShareFetchResponseNodeEndpoint
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
			if version >= 0 && version <= 999 {
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
				var tempElem ShareFetchResponseNodeEndpoint
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
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
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
			m.NodeEndpoints = make([]ShareFetchResponseNodeEndpoint, len(decoded))
			for i, item := range decoded {
				m.NodeEndpoints[i] = item.(ShareFetchResponseNodeEndpoint)
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
				var tempElem ShareFetchResponseNodeEndpoint
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
				if version >= 0 && version <= 999 {
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
				if version >= 0 && version <= 999 {
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
			m.NodeEndpoints = make([]ShareFetchResponseNodeEndpoint, len(decoded))
			for i, item := range decoded {
				m.NodeEndpoints[i] = item.(ShareFetchResponseNodeEndpoint)
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

// ShareFetchResponseShareFetchableTopicResponse represents The response topics..
type ShareFetchResponseShareFetchableTopicResponse struct {
	// The unique topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// The topic partitions.
	Partitions []ShareFetchResponsePartitionData `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ShareFetchResponseShareFetchableTopicResponse.
func (m *ShareFetchResponseShareFetchableTopicResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareFetchResponseShareFetchableTopicResponse.
func (m *ShareFetchResponseShareFetchableTopicResponse) readTaggedFields(r io.Reader, version int16) error {
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

// ShareFetchResponsePartitionData represents The topic partitions..
type ShareFetchResponsePartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The fetch error code, or 0 if there was no fetch error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The fetch error message, or null if there was no fetch error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The acknowledge error code, or 0 if there was no acknowledge error.
	AcknowledgeErrorCode int16 `json:"acknowledgeerrorcode" versions:"0-999"`
	// The acknowledge error message, or null if there was no acknowledge error.
	AcknowledgeErrorMessage *string `json:"acknowledgeerrormessage" versions:"0-999"`
	// The current leader of the partition.
	CurrentLeader ShareFetchResponseLeaderIdAndEpoch `json:"currentleader" versions:"0-999"`
	// The record data.
	Records *[]byte `json:"records" versions:"0-999"`
	// The acquired records.
	AcquiredRecords []ShareFetchResponseAcquiredRecords `json:"acquiredrecords" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ShareFetchResponsePartitionData.
func (m *ShareFetchResponsePartitionData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareFetchResponsePartitionData.
func (m *ShareFetchResponsePartitionData) readTaggedFields(r io.Reader, version int16) error {
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

// ShareFetchResponseLeaderIdAndEpoch represents The current leader of the partition..
type ShareFetchResponseLeaderIdAndEpoch struct {
	// The ID of the current leader or -1 if the leader is unknown.
	LeaderId int32 `json:"leaderid" versions:"0-999"`
	// The latest known leader epoch.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ShareFetchResponseLeaderIdAndEpoch.
func (m *ShareFetchResponseLeaderIdAndEpoch) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareFetchResponseLeaderIdAndEpoch.
func (m *ShareFetchResponseLeaderIdAndEpoch) readTaggedFields(r io.Reader, version int16) error {
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

// ShareFetchResponseAcquiredRecords represents The acquired records..
type ShareFetchResponseAcquiredRecords struct {
	// The earliest offset in this batch of acquired records.
	FirstOffset int64 `json:"firstoffset" versions:"0-999"`
	// The last offset of this batch of acquired records.
	LastOffset int64 `json:"lastoffset" versions:"0-999"`
	// The delivery count of this batch of acquired records.
	DeliveryCount int16 `json:"deliverycount" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ShareFetchResponseAcquiredRecords.
func (m *ShareFetchResponseAcquiredRecords) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareFetchResponseAcquiredRecords.
func (m *ShareFetchResponseAcquiredRecords) readTaggedFields(r io.Reader, version int16) error {
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

// ShareFetchResponseNodeEndpoint represents Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER..
type ShareFetchResponseNodeEndpoint struct {
	// The ID of the associated node.
	NodeId int32 `json:"nodeid" versions:"0-999"`
	// The node's hostname.
	Host string `json:"host" versions:"0-999"`
	// The node's port.
	Port int32 `json:"port" versions:"0-999"`
	// The rack of the node, or null if it has not been assigned to a rack.
	Rack *string `json:"rack" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ShareFetchResponseNodeEndpoint.
func (m *ShareFetchResponseNodeEndpoint) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareFetchResponseNodeEndpoint.
func (m *ShareFetchResponseNodeEndpoint) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for ShareFetchResponse.
func (m *ShareFetchResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareFetchResponse.
func (m *ShareFetchResponse) readTaggedFields(r io.Reader, version int16) error {
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
