package alterpartitionreassignments

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AlterPartitionReassignmentsResponseApiKey        = 45
	AlterPartitionReassignmentsResponseHeaderVersion = 1
)

// AlterPartitionReassignmentsResponse represents a response message.
type AlterPartitionReassignmentsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The option indicating whether changing the replication factor of any given partition as part of the request was allowed.
	AllowReplicationFactorChange bool `json:"allowreplicationfactorchange" versions:"1-999"`
	// The top-level error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The top-level error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The responses to topics to reassign.
	Responses []AlterPartitionReassignmentsResponseReassignableTopicResponse `json:"responses" versions:"0-999"`
}

// Encode encodes a AlterPartitionReassignmentsResponse to a byte slice for the given version.
func (m *AlterPartitionReassignmentsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AlterPartitionReassignmentsResponse from a byte slice for the given version.
func (m *AlterPartitionReassignmentsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AlterPartitionReassignmentsResponse to an io.Writer for the given version.
func (m *AlterPartitionReassignmentsResponse) Write(w io.Writer, version int16) error {
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
	// AllowReplicationFactorChange
	if version >= 1 && version <= 999 {
		if err := protocol.WriteBool(w, m.AllowReplicationFactorChange); err != nil {
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
	// Responses
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Responses) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Responses))); err != nil {
				return err
			}
		}
		for i := range m.Responses {
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Responses[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Responses[i].Name); err != nil {
						return err
					}
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Responses[i].Partitions) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Responses[i].Partitions))); err != nil {
						return err
					}
				}
				for i := range m.Responses[i].Partitions {
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Responses[i].Partitions[i].PartitionIndex); err != nil {
							return err
						}
					}
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(w, m.Responses[i].Partitions[i].ErrorCode); err != nil {
							return err
						}
					}
					// ErrorMessage
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Responses[i].Partitions[i].ErrorMessage); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Responses[i].Partitions[i].ErrorMessage); err != nil {
								return err
							}
						}
					}
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

// Read reads a AlterPartitionReassignmentsResponse from an io.Reader for the given version.
func (m *AlterPartitionReassignmentsResponse) Read(r io.Reader, version int16) error {
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
	// AllowReplicationFactorChange
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.AllowReplicationFactorChange = val
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
	// Responses
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
			m.Responses = make([]AlterPartitionReassignmentsResponseReassignableTopicResponse, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Name = val
					}
				}
				// Partitions
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
						m.Responses[i].Partitions = make([]AlterPartitionReassignmentsResponseReassignablePartitionResponse, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Responses[i].Partitions = make([]AlterPartitionReassignmentsResponseReassignablePartitionResponse, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								}
							}
						}
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Responses = make([]AlterPartitionReassignmentsResponseReassignableTopicResponse, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Name = val
					}
				}
				// Partitions
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
						m.Responses[i].Partitions = make([]AlterPartitionReassignmentsResponseReassignablePartitionResponse, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Responses[i].Partitions = make([]AlterPartitionReassignmentsResponseReassignablePartitionResponse, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].ErrorMessage = val
								}
							}
						}
					}
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

// AlterPartitionReassignmentsResponseReassignableTopicResponse represents The responses to topics to reassign..
type AlterPartitionReassignmentsResponseReassignableTopicResponse struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The responses to partitions to reassign.
	Partitions []AlterPartitionReassignmentsResponseReassignablePartitionResponse `json:"partitions" versions:"0-999"`
}

// AlterPartitionReassignmentsResponseReassignablePartitionResponse represents The responses to partitions to reassign..
type AlterPartitionReassignmentsResponseReassignablePartitionResponse struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The error code for this partition, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message for this partition, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for AlterPartitionReassignmentsResponse.
func (m *AlterPartitionReassignmentsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterPartitionReassignmentsResponse.
func (m *AlterPartitionReassignmentsResponse) readTaggedFields(r io.Reader, version int16) error {
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
