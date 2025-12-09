package consumergroupdescribe

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ConsumerGroupDescribeResponseApiKey        = 69
	ConsumerGroupDescribeResponseHeaderVersion = 1
)

// ConsumerGroupDescribeResponse represents a response message.
type ConsumerGroupDescribeResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// Each described group.
	Groups []ConsumerGroupDescribeResponseDescribedGroup `json:"groups" versions:"0-999"`
}

// Encode encodes a ConsumerGroupDescribeResponse to a byte slice for the given version.
func (m *ConsumerGroupDescribeResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ConsumerGroupDescribeResponse from a byte slice for the given version.
func (m *ConsumerGroupDescribeResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ConsumerGroupDescribeResponse to an io.Writer for the given version.
func (m *ConsumerGroupDescribeResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
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
		if isFlexible {
			length := uint32(len(m.Groups) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Groups))); err != nil {
				return err
			}
		}
		for i := range m.Groups {
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Groups[i].ErrorCode); err != nil {
					return err
				}
			}
			// ErrorMessage
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Groups[i].ErrorMessage); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Groups[i].ErrorMessage); err != nil {
						return err
					}
				}
			}
			// GroupId
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Groups[i].GroupId); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Groups[i].GroupId); err != nil {
						return err
					}
				}
			}
			// GroupState
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Groups[i].GroupState); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Groups[i].GroupState); err != nil {
						return err
					}
				}
			}
			// GroupEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Groups[i].GroupEpoch); err != nil {
					return err
				}
			}
			// AssignmentEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Groups[i].AssignmentEpoch); err != nil {
					return err
				}
			}
			// AssignorName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Groups[i].AssignorName); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Groups[i].AssignorName); err != nil {
						return err
					}
				}
			}
			// Members
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Groups[i].Members) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Members))); err != nil {
						return err
					}
				}
				for i := range m.Groups[i].Members {
					// MemberId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].MemberId); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Groups[i].Members[i].MemberId); err != nil {
								return err
							}
						}
					}
					// InstanceId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Groups[i].Members[i].InstanceId); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Groups[i].Members[i].InstanceId); err != nil {
								return err
							}
						}
					}
					// RackId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Groups[i].Members[i].RackId); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Groups[i].Members[i].RackId); err != nil {
								return err
							}
						}
					}
					// MemberEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Groups[i].Members[i].MemberEpoch); err != nil {
							return err
						}
					}
					// ClientId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].ClientId); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Groups[i].Members[i].ClientId); err != nil {
								return err
							}
						}
					}
					// ClientHost
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].ClientHost); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Groups[i].Members[i].ClientHost); err != nil {
								return err
							}
						}
					}
					// SubscribedTopicNames
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Groups[i].Members[i].SubscribedTopicNames) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Members[i].SubscribedTopicNames))); err != nil {
								return err
							}
						}
						for i := range m.Groups[i].Members[i].SubscribedTopicNames {
							if isFlexible {
								if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].SubscribedTopicNames[i]); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(w, m.Groups[i].Members[i].SubscribedTopicNames[i]); err != nil {
									return err
								}
							}
							_ = i
						}
					}
					// SubscribedTopicRegex
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Groups[i].Members[i].SubscribedTopicRegex); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Groups[i].Members[i].SubscribedTopicRegex); err != nil {
								return err
							}
						}
					}
					// Assignment
					if version >= 0 && version <= 999 {
					}
					// TargetAssignment
					if version >= 0 && version <= 999 {
					}
					// MemberType
					if version >= 1 && version <= 999 {
						if err := protocol.WriteInt8(w, m.Groups[i].Members[i].MemberType); err != nil {
							return err
						}
					}
				}
			}
			// AuthorizedOperations
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Groups[i].AuthorizedOperations); err != nil {
					return err
				}
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

// Read reads a ConsumerGroupDescribeResponse from an io.Reader for the given version.
func (m *ConsumerGroupDescribeResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
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
			m.Groups = make([]ConsumerGroupDescribeResponseDescribedGroup, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Groups[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ErrorMessage = val
					}
				}
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupId = val
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupState = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupState = val
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].GroupEpoch = val
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].AssignmentEpoch = val
				}
				// AssignorName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].AssignorName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].AssignorName = val
					}
				}
				// Members
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
						m.Groups[i].Members = make([]ConsumerGroupDescribeResponseMember, length)
						for i := int32(0); i < length; i++ {
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								}
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberEpoch = val
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								}
							}
							// SubscribedTopicNames
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
									m.Groups[i].Members[i].SubscribedTopicNames = make([]string, length)
									for i := int32(0); i < length; i++ {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicNames = make([]string, length)
									for i := int32(0); i < length; i++ {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										}
									}
								}
							}
							// SubscribedTopicRegex
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicRegex = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicRegex = val
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// MemberType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberType = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Groups[i].Members = make([]ConsumerGroupDescribeResponseMember, length)
						for i := int32(0); i < length; i++ {
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								}
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberEpoch = val
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								}
							}
							// SubscribedTopicNames
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
									m.Groups[i].Members[i].SubscribedTopicNames = make([]string, length)
									for i := int32(0); i < length; i++ {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicNames = make([]string, length)
									for i := int32(0); i < length; i++ {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										}
									}
								}
							}
							// SubscribedTopicRegex
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicRegex = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicRegex = val
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// MemberType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberType = val
							}
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].AuthorizedOperations = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Groups = make([]ConsumerGroupDescribeResponseDescribedGroup, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Groups[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ErrorMessage = val
					}
				}
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupId = val
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupState = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupState = val
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].GroupEpoch = val
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].AssignmentEpoch = val
				}
				// AssignorName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].AssignorName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].AssignorName = val
					}
				}
				// Members
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
						m.Groups[i].Members = make([]ConsumerGroupDescribeResponseMember, length)
						for i := int32(0); i < length; i++ {
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								}
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberEpoch = val
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								}
							}
							// SubscribedTopicNames
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
									m.Groups[i].Members[i].SubscribedTopicNames = make([]string, length)
									for i := int32(0); i < length; i++ {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicNames = make([]string, length)
									for i := int32(0); i < length; i++ {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										}
									}
								}
							}
							// SubscribedTopicRegex
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicRegex = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicRegex = val
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// MemberType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberType = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Groups[i].Members = make([]ConsumerGroupDescribeResponseMember, length)
						for i := int32(0); i < length; i++ {
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								}
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberEpoch = val
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								}
							}
							// SubscribedTopicNames
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
									m.Groups[i].Members[i].SubscribedTopicNames = make([]string, length)
									for i := int32(0); i < length; i++ {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicNames = make([]string, length)
									for i := int32(0); i < length; i++ {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].SubscribedTopicNames[i] = val
										}
									}
								}
							}
							// SubscribedTopicRegex
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicRegex = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].SubscribedTopicRegex = val
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// MemberType
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberType = val
							}
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].AuthorizedOperations = val
				}
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

// ConsumerGroupDescribeResponseDescribedGroup represents Each described group..
type ConsumerGroupDescribeResponseDescribedGroup struct {
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
	Members []ConsumerGroupDescribeResponseMember `json:"members" versions:"0-999"`
	// 32-bit bitfield to represent authorized operations for this group.
	AuthorizedOperations int32 `json:"authorizedoperations" versions:"0-999"`
}

// ConsumerGroupDescribeResponseMember represents The members..
type ConsumerGroupDescribeResponseMember struct {
	// The member ID.
	MemberId string `json:"memberid" versions:"0-999"`
	// The member instance ID.
	InstanceId *string `json:"instanceid" versions:"0-999"`
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
	// the subscribed topic regex otherwise or null of not provided.
	SubscribedTopicRegex *string `json:"subscribedtopicregex" versions:"0-999"`
	// The current assignment.
	Assignment Assignment `json:"assignment" versions:"0-999"`
	// The target assignment.
	TargetAssignment Assignment `json:"targetassignment" versions:"0-999"`
	// -1 for unknown. 0 for classic member. +1 for consumer member.
	MemberType int8 `json:"membertype" versions:"1-999"`
}

// writeTaggedFields writes tagged fields for ConsumerGroupDescribeResponse.
func (m *ConsumerGroupDescribeResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ConsumerGroupDescribeResponse.
func (m *ConsumerGroupDescribeResponse) readTaggedFields(r io.Reader, version int16) error {
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
