package sharegroupdescribe

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ShareGroupDescribeResponseApiKey        = 77
	ShareGroupDescribeResponseHeaderVersion = 1
)

// ShareGroupDescribeResponse represents a response message.
type ShareGroupDescribeResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// Each described group.
	Groups []ShareGroupDescribeResponseDescribedGroup `json:"groups" versions:"0-999"`
}

// Encode encodes a ShareGroupDescribeResponse to a byte slice for the given version.
func (m *ShareGroupDescribeResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ShareGroupDescribeResponse from a byte slice for the given version.
func (m *ShareGroupDescribeResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ShareGroupDescribeResponse to an io.Writer for the given version.
func (m *ShareGroupDescribeResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 1 {
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
	// Groups
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ShareGroupDescribeResponseDescribedGroup)
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
			// ErrorMessage
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.ErrorMessage); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.ErrorMessage); err != nil {
						return nil, err
					}
				}
			}
			// GroupId
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.GroupId); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.GroupId); err != nil {
						return nil, err
					}
				}
			}
			// GroupState
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.GroupState); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.GroupState); err != nil {
						return nil, err
					}
				}
			}
			// GroupEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.GroupEpoch); err != nil {
					return nil, err
				}
			}
			// AssignmentEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.AssignmentEpoch); err != nil {
					return nil, err
				}
			}
			// AssignorName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.AssignorName); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.AssignorName); err != nil {
						return nil, err
					}
				}
			}
			// Members
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Members) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Members))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Members {
					// MemberId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Members[i].MemberId); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Members[i].MemberId); err != nil {
								return nil, err
							}
						}
					}
					// RackId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.Members[i].RackId); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.Members[i].RackId); err != nil {
								return nil, err
							}
						}
					}
					// MemberEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Members[i].MemberEpoch); err != nil {
							return nil, err
						}
					}
					// ClientId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Members[i].ClientId); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Members[i].ClientId); err != nil {
								return nil, err
							}
						}
					}
					// ClientHost
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Members[i].ClientHost); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Members[i].ClientHost); err != nil {
								return nil, err
							}
						}
					}
					// SubscribedTopicNames
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactStringArray(elemW, structItem.Members[i].SubscribedTopicNames); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteStringArray(elemW, structItem.Members[i].SubscribedTopicNames); err != nil {
								return nil, err
							}
						}
					}
					// Assignment
					if version >= 0 && version <= 999 {
					}
				}
			}
			// AuthorizedOperations
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.AuthorizedOperations); err != nil {
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
		items := make([]interface{}, len(m.Groups))
		for i := range m.Groups {
			items[i] = m.Groups[i]
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

// Read reads a ShareGroupDescribeResponse from an io.Reader for the given version.
func (m *ShareGroupDescribeResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 1 {
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
	// Groups
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ShareGroupDescribeResponseDescribedGroup
			elemR := bytes.NewReader(data)
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
			// GroupId
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupId = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupId = val
				}
			}
			// GroupState
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupState = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupState = val
				}
			}
			// GroupEpoch
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.GroupEpoch = val
			}
			// AssignmentEpoch
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.AssignmentEpoch = val
			}
			// AssignorName
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.AssignorName = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.AssignorName = val
				}
			}
			// Members
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// AuthorizedOperations
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.AuthorizedOperations = val
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
				var tempElem ShareGroupDescribeResponseDescribedGroup
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
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupState = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupState = val
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.GroupEpoch = val
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.AssignmentEpoch = val
				}
				// AssignorName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.AssignorName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.AssignorName = val
					}
				}
				// Members
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem ShareGroupDescribeResponseMember
						elemR := bytes.NewReader(data)
						// MemberId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.MemberId = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.MemberId = val
							}
						}
						// RackId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.RackId = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.RackId = val
							}
						}
						// MemberEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.MemberEpoch = val
						}
						// ClientId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientId = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientId = val
							}
						}
						// ClientHost
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientHost = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientHost = val
							}
						}
						// SubscribedTopicNames
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactStringArray(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.SubscribedTopicNames = val
							} else {
								val, err := protocol.ReadStringArray(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.SubscribedTopicNames = val
							}
						}
						// Assignment
						if version >= 0 && version <= 999 {
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
							var tempElem ShareGroupDescribeResponseMember
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.MemberEpoch = val
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								}
							}
							// SubscribedTopicNames
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactStringArray(r)
									if err != nil {
										return err
									}
									tempElem.SubscribedTopicNames = val
								} else {
									val, err := protocol.ReadStringArray(r)
									if err != nil {
										return err
									}
									tempElem.SubscribedTopicNames = val
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.MemberEpoch); err != nil {
									return err
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								}
							}
							// SubscribedTopicNames
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactStringArray(elemW, tempElem.SubscribedTopicNames); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteStringArray(elemW, tempElem.SubscribedTopicNames); err != nil {
										return err
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
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
						tempElem.Members = make([]ShareGroupDescribeResponseMember, len(decoded))
						for i, item := range decoded {
							tempElem.Members[i] = item.(ShareGroupDescribeResponseMember)
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
							var tempElem ShareGroupDescribeResponseMember
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.MemberEpoch = val
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								}
							}
							// SubscribedTopicNames
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactStringArray(r)
									if err != nil {
										return err
									}
									tempElem.SubscribedTopicNames = val
								} else {
									val, err := protocol.ReadStringArray(r)
									if err != nil {
										return err
									}
									tempElem.SubscribedTopicNames = val
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.MemberEpoch); err != nil {
									return err
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								}
							}
							// SubscribedTopicNames
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactStringArray(elemW, tempElem.SubscribedTopicNames); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteStringArray(elemW, tempElem.SubscribedTopicNames); err != nil {
										return err
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
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
						tempElem.Members = make([]ShareGroupDescribeResponseMember, len(decoded))
						for i, item := range decoded {
							tempElem.Members[i] = item.(ShareGroupDescribeResponseMember)
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.AuthorizedOperations = val
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
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupState); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupState); err != nil {
							return err
						}
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.GroupEpoch); err != nil {
						return err
					}
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.AssignmentEpoch); err != nil {
						return err
					}
				}
				// AssignorName
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.AssignorName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.AssignorName); err != nil {
							return err
						}
					}
				}
				// Members
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Members) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Members))); err != nil {
							return err
						}
					}
					for i := range tempElem.Members {
						// MemberId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].MemberId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].MemberId); err != nil {
									return err
								}
							}
						}
						// RackId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Members[i].RackId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Members[i].RackId); err != nil {
									return err
								}
							}
						}
						// MemberEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Members[i].MemberEpoch); err != nil {
								return err
							}
						}
						// ClientId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].ClientId); err != nil {
									return err
								}
							}
						}
						// ClientHost
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientHost); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].ClientHost); err != nil {
									return err
								}
							}
						}
						// SubscribedTopicNames
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactStringArray(elemW, tempElem.Members[i].SubscribedTopicNames); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteStringArray(elemW, tempElem.Members[i].SubscribedTopicNames); err != nil {
									return err
								}
							}
						}
						// Assignment
						if version >= 0 && version <= 999 {
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.AuthorizedOperations); err != nil {
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
			m.Groups = make([]ShareGroupDescribeResponseDescribedGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(ShareGroupDescribeResponseDescribedGroup)
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
				var tempElem ShareGroupDescribeResponseDescribedGroup
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
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupState = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupState = val
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.GroupEpoch = val
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.AssignmentEpoch = val
				}
				// AssignorName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.AssignorName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.AssignorName = val
					}
				}
				// Members
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem ShareGroupDescribeResponseMember
						elemR := bytes.NewReader(data)
						// MemberId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.MemberId = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.MemberId = val
							}
						}
						// RackId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.RackId = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.RackId = val
							}
						}
						// MemberEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.MemberEpoch = val
						}
						// ClientId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientId = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientId = val
							}
						}
						// ClientHost
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientHost = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientHost = val
							}
						}
						// SubscribedTopicNames
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactStringArray(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.SubscribedTopicNames = val
							} else {
								val, err := protocol.ReadStringArray(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.SubscribedTopicNames = val
							}
						}
						// Assignment
						if version >= 0 && version <= 999 {
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
							var tempElem ShareGroupDescribeResponseMember
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.MemberEpoch = val
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								}
							}
							// SubscribedTopicNames
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactStringArray(r)
									if err != nil {
										return err
									}
									tempElem.SubscribedTopicNames = val
								} else {
									val, err := protocol.ReadStringArray(r)
									if err != nil {
										return err
									}
									tempElem.SubscribedTopicNames = val
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.MemberEpoch); err != nil {
									return err
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								}
							}
							// SubscribedTopicNames
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactStringArray(elemW, tempElem.SubscribedTopicNames); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteStringArray(elemW, tempElem.SubscribedTopicNames); err != nil {
										return err
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
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
						tempElem.Members = make([]ShareGroupDescribeResponseMember, len(decoded))
						for i, item := range decoded {
							tempElem.Members[i] = item.(ShareGroupDescribeResponseMember)
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
							var tempElem ShareGroupDescribeResponseMember
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.MemberEpoch = val
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								}
							}
							// SubscribedTopicNames
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactStringArray(r)
									if err != nil {
										return err
									}
									tempElem.SubscribedTopicNames = val
								} else {
									val, err := protocol.ReadStringArray(r)
									if err != nil {
										return err
									}
									tempElem.SubscribedTopicNames = val
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.MemberEpoch); err != nil {
									return err
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								}
							}
							// SubscribedTopicNames
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactStringArray(elemW, tempElem.SubscribedTopicNames); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteStringArray(elemW, tempElem.SubscribedTopicNames); err != nil {
										return err
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
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
						tempElem.Members = make([]ShareGroupDescribeResponseMember, len(decoded))
						for i, item := range decoded {
							tempElem.Members[i] = item.(ShareGroupDescribeResponseMember)
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.AuthorizedOperations = val
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
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupState); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupState); err != nil {
							return err
						}
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.GroupEpoch); err != nil {
						return err
					}
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.AssignmentEpoch); err != nil {
						return err
					}
				}
				// AssignorName
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.AssignorName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.AssignorName); err != nil {
							return err
						}
					}
				}
				// Members
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Members) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Members))); err != nil {
							return err
						}
					}
					for i := range tempElem.Members {
						// MemberId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].MemberId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].MemberId); err != nil {
									return err
								}
							}
						}
						// RackId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Members[i].RackId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Members[i].RackId); err != nil {
									return err
								}
							}
						}
						// MemberEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Members[i].MemberEpoch); err != nil {
								return err
							}
						}
						// ClientId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].ClientId); err != nil {
									return err
								}
							}
						}
						// ClientHost
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientHost); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].ClientHost); err != nil {
									return err
								}
							}
						}
						// SubscribedTopicNames
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactStringArray(elemW, tempElem.Members[i].SubscribedTopicNames); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteStringArray(elemW, tempElem.Members[i].SubscribedTopicNames); err != nil {
									return err
								}
							}
						}
						// Assignment
						if version >= 0 && version <= 999 {
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.AuthorizedOperations); err != nil {
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
			m.Groups = make([]ShareGroupDescribeResponseDescribedGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(ShareGroupDescribeResponseDescribedGroup)
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

// ShareGroupDescribeResponseDescribedGroup represents Each described group..
type ShareGroupDescribeResponseDescribedGroup struct {
	// The describe error, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The top-level error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The group ID string.
	GroupId string `json:"groupid" versions:"0-999"`
	// The group state string, or the empty string.
	GroupState string `json:"groupstate" versions:"0-999"`
	// The group epoch.
	GroupEpoch int32 `json:"groupepoch" versions:"0-999"`
	// The assignment epoch.
	AssignmentEpoch int32 `json:"assignmentepoch" versions:"0-999"`
	// The selected assignor.
	AssignorName string `json:"assignorname" versions:"0-999"`
	// The members.
	Members []ShareGroupDescribeResponseMember `json:"members" versions:"0-999"`
	// 32-bit bitfield to represent authorized operations for this group.
	AuthorizedOperations int32 `json:"authorizedoperations" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ShareGroupDescribeResponseDescribedGroup.
func (m *ShareGroupDescribeResponseDescribedGroup) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareGroupDescribeResponseDescribedGroup.
func (m *ShareGroupDescribeResponseDescribedGroup) readTaggedFields(r io.Reader, version int16) error {
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

// ShareGroupDescribeResponseMember represents The members..
type ShareGroupDescribeResponseMember struct {
	// The member ID.
	MemberId string `json:"memberid" versions:"0-999"`
	// The member rack ID.
	RackId *string `json:"rackid" versions:"0-999"`
	// The current member epoch.
	MemberEpoch int32 `json:"memberepoch" versions:"0-999"`
	// The client ID.
	ClientId string `json:"clientid" versions:"0-999"`
	// The client host.
	ClientHost string `json:"clienthost" versions:"0-999"`
	// The subscribed topic names.
	SubscribedTopicNames []string `json:"subscribedtopicnames" versions:"0-999"`
	// The current assignment.
	Assignment Assignment `json:"assignment" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ShareGroupDescribeResponseMember.
func (m *ShareGroupDescribeResponseMember) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareGroupDescribeResponseMember.
func (m *ShareGroupDescribeResponseMember) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for ShareGroupDescribeResponse.
func (m *ShareGroupDescribeResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareGroupDescribeResponse.
func (m *ShareGroupDescribeResponse) readTaggedFields(r io.Reader, version int16) error {
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
