package describegroups

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeGroupsResponseApiKey        = 15
	DescribeGroupsResponseHeaderVersion = 1
)

// DescribeGroupsResponse represents a response message.
type DescribeGroupsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"1-999"`
	// Each described group.
	Groups []DescribeGroupsResponseDescribedGroup `json:"groups" versions:"0-999"`
}

// Encode encodes a DescribeGroupsResponse to a byte slice for the given version.
func (m *DescribeGroupsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeGroupsResponse from a byte slice for the given version.
func (m *DescribeGroupsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeGroupsResponse to an io.Writer for the given version.
func (m *DescribeGroupsResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 5 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
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
			if version >= 6 && version <= 999 {
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
			// ProtocolType
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Groups[i].ProtocolType); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Groups[i].ProtocolType); err != nil {
						return err
					}
				}
			}
			// ProtocolData
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Groups[i].ProtocolData); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Groups[i].ProtocolData); err != nil {
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
					// GroupInstanceId
					if version >= 4 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Groups[i].Members[i].GroupInstanceId); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Groups[i].Members[i].GroupInstanceId); err != nil {
								return err
							}
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
					// MemberMetadata
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactBytes(w, m.Groups[i].Members[i].MemberMetadata); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteBytes(w, m.Groups[i].Members[i].MemberMetadata); err != nil {
								return err
							}
						}
					}
					// MemberAssignment
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactBytes(w, m.Groups[i].Members[i].MemberAssignment); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteBytes(w, m.Groups[i].Members[i].MemberAssignment); err != nil {
								return err
							}
						}
					}
				}
			}
			// AuthorizedOperations
			if version >= 3 && version <= 999 {
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

// Read reads a DescribeGroupsResponse from an io.Reader for the given version.
func (m *DescribeGroupsResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 5 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
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
			m.Groups = make([]DescribeGroupsResponseDescribedGroup, length)
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
				if version >= 6 && version <= 999 {
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
				// ProtocolType
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ProtocolType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ProtocolType = val
					}
				}
				// ProtocolData
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ProtocolData = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ProtocolData = val
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
						m.Groups[i].Members = make([]DescribeGroupsResponseDescribedGroupMember, length)
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
							// GroupInstanceId
							if version >= 4 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].GroupInstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].GroupInstanceId = val
								}
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
							// MemberMetadata
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberMetadata = val
								} else {
									val, err := protocol.ReadBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberMetadata = val
								}
							}
							// MemberAssignment
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberAssignment = val
								} else {
									val, err := protocol.ReadBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberAssignment = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Groups[i].Members = make([]DescribeGroupsResponseDescribedGroupMember, length)
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
							// GroupInstanceId
							if version >= 4 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].GroupInstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].GroupInstanceId = val
								}
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
							// MemberMetadata
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberMetadata = val
								} else {
									val, err := protocol.ReadBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberMetadata = val
								}
							}
							// MemberAssignment
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberAssignment = val
								} else {
									val, err := protocol.ReadBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberAssignment = val
								}
							}
						}
					}
				}
				// AuthorizedOperations
				if version >= 3 && version <= 999 {
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
			m.Groups = make([]DescribeGroupsResponseDescribedGroup, length)
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
				if version >= 6 && version <= 999 {
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
				// ProtocolType
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ProtocolType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ProtocolType = val
					}
				}
				// ProtocolData
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ProtocolData = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ProtocolData = val
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
						m.Groups[i].Members = make([]DescribeGroupsResponseDescribedGroupMember, length)
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
							// GroupInstanceId
							if version >= 4 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].GroupInstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].GroupInstanceId = val
								}
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
							// MemberMetadata
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberMetadata = val
								} else {
									val, err := protocol.ReadBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberMetadata = val
								}
							}
							// MemberAssignment
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberAssignment = val
								} else {
									val, err := protocol.ReadBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberAssignment = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Groups[i].Members = make([]DescribeGroupsResponseDescribedGroupMember, length)
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
							// GroupInstanceId
							if version >= 4 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].GroupInstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].GroupInstanceId = val
								}
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
							// MemberMetadata
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberMetadata = val
								} else {
									val, err := protocol.ReadBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberMetadata = val
								}
							}
							// MemberAssignment
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberAssignment = val
								} else {
									val, err := protocol.ReadBytes(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberAssignment = val
								}
							}
						}
					}
				}
				// AuthorizedOperations
				if version >= 3 && version <= 999 {
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

// DescribeGroupsResponseDescribedGroup represents Each described group..
type DescribeGroupsResponseDescribedGroup struct {
	// The describe error, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The describe error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"6-999"`
	// The group ID string.
	GroupId string `json:"groupid" versions:"0-999"`
	// The group state string, or the empty string.
	GroupState string `json:"groupstate" versions:"0-999"`
	// The group protocol type, or the empty string.
	ProtocolType string `json:"protocoltype" versions:"0-999"`
	// The group protocol data, or the empty string.
	ProtocolData string `json:"protocoldata" versions:"0-999"`
	// The group members.
	Members []DescribeGroupsResponseDescribedGroupMember `json:"members" versions:"0-999"`
	// 32-bit bitfield to represent authorized operations for this group.
	AuthorizedOperations int32 `json:"authorizedoperations" versions:"3-999"`
}

// DescribeGroupsResponseDescribedGroupMember represents The group members..
type DescribeGroupsResponseDescribedGroupMember struct {
	// The member id.
	MemberId string `json:"memberid" versions:"0-999"`
	// The unique identifier of the consumer instance provided by end user.
	GroupInstanceId *string `json:"groupinstanceid" versions:"4-999"`
	// The client ID used in the member's latest join group request.
	ClientId string `json:"clientid" versions:"0-999"`
	// The client host.
	ClientHost string `json:"clienthost" versions:"0-999"`
	// The metadata corresponding to the current group protocol in use.
	MemberMetadata []byte `json:"membermetadata" versions:"0-999"`
	// The current assignment provided by the group leader.
	MemberAssignment []byte `json:"memberassignment" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeGroupsResponse.
func (m *DescribeGroupsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeGroupsResponse.
func (m *DescribeGroupsResponse) readTaggedFields(r io.Reader, version int16) error {
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
