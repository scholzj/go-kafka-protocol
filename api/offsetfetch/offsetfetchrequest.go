package offsetfetch

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	OffsetFetchRequestApiKey        = 9
	OffsetFetchRequestHeaderVersion = 1
)

// OffsetFetchRequest represents a request message.
type OffsetFetchRequest struct {
	// The group to fetch offsets for.
	GroupId string `json:"groupid" versions:"0-7"`
	// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
	Topics []OffsetFetchRequestOffsetFetchRequestTopic `json:"topics" versions:"0-7"`
	// Each group we would like to fetch offsets for.
	Groups []OffsetFetchRequestOffsetFetchRequestGroup `json:"groups" versions:"8-999"`
	// Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
	RequireStable bool `json:"requirestable" versions:"7-999"`
}

// Encode encodes a OffsetFetchRequest to a byte slice for the given version.
func (m *OffsetFetchRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a OffsetFetchRequest from a byte slice for the given version.
func (m *OffsetFetchRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a OffsetFetchRequest to an io.Writer for the given version.
func (m *OffsetFetchRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 10 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 7 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.GroupId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.GroupId); err != nil {
				return err
			}
		}
	}
	// Topics
	if version >= 0 && version <= 7 {
		if m.Topics == nil {
			if isFlexible {
				if err := protocol.WriteVaruint32(w, 0); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteInt32(w, -1); err != nil {
					return err
				}
			}
		} else {
			if isFlexible {
				length := uint32(len(m.Topics) + 1)
				if err := protocol.WriteVaruint32(w, length); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteInt32(w, int32(len(m.Topics))); err != nil {
					return err
				}
			}
			for i := range m.Topics {
				// Name
				if version >= 0 && version <= 7 {
					if isFlexible {
						if err := protocol.WriteCompactString(w, m.Topics[i].Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(w, m.Topics[i].Name); err != nil {
							return err
						}
					}
				}
				// PartitionIndexes
				if version >= 0 && version <= 7 {
					if isFlexible {
						length := uint32(len(m.Topics[i].PartitionIndexes) + 1)
						if err := protocol.WriteVaruint32(w, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(w, int32(len(m.Topics[i].PartitionIndexes))); err != nil {
							return err
						}
					}
					for i := range m.Topics[i].PartitionIndexes {
						if err := protocol.WriteInt32(w, m.Topics[i].PartitionIndexes[i]); err != nil {
							return err
						}
						_ = i
					}
				}
			}
		}
	}
	// Groups
	if version >= 8 && version <= 999 {
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
			// GroupId
			if version >= 8 && version <= 999 {
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
			// MemberId
			if version >= 9 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Groups[i].MemberId); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Groups[i].MemberId); err != nil {
						return err
					}
				}
			}
			// MemberEpoch
			if version >= 9 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Groups[i].MemberEpoch); err != nil {
					return err
				}
			}
			// Topics
			if version >= 8 && version <= 999 {
				if m.Groups[i].Topics == nil {
					if isFlexible {
						if err := protocol.WriteVaruint32(w, 0); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(w, -1); err != nil {
							return err
						}
					}
				} else {
					if isFlexible {
						length := uint32(len(m.Groups[i].Topics) + 1)
						if err := protocol.WriteVaruint32(w, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Topics))); err != nil {
							return err
						}
					}
					for i := range m.Groups[i].Topics {
						// Name
						if version >= 8 && version <= 9 {
							if isFlexible {
								if err := protocol.WriteCompactString(w, m.Groups[i].Topics[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(w, m.Groups[i].Topics[i].Name); err != nil {
									return err
								}
							}
						}
						// TopicId
						if version >= 10 && version <= 999 {
							if err := protocol.WriteUUID(w, m.Groups[i].Topics[i].TopicId); err != nil {
								return err
							}
						}
						// PartitionIndexes
						if version >= 8 && version <= 999 {
							if isFlexible {
								length := uint32(len(m.Groups[i].Topics[i].PartitionIndexes) + 1)
								if err := protocol.WriteVaruint32(w, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Topics[i].PartitionIndexes))); err != nil {
									return err
								}
							}
							for i := range m.Groups[i].Topics[i].PartitionIndexes {
								if err := protocol.WriteInt32(w, m.Groups[i].Topics[i].PartitionIndexes[i]); err != nil {
									return err
								}
								_ = i
							}
						}
					}
				}
			}
		}
	}
	// RequireStable
	if version >= 7 && version <= 999 {
		if err := protocol.WriteBool(w, m.RequireStable); err != nil {
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

// Read reads a OffsetFetchRequest from an io.Reader for the given version.
func (m *OffsetFetchRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 10 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 6 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 7 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		}
	}
	// Topics
	if version >= 0 && version <= 7 {
		var length int32
		if isFlexible {
			var lengthUint uint32
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint == 0 {
				m.Topics = nil
			} else {
				if lengthUint < 1 {
					return errors.New("invalid compact array length")
				}
				length = int32(lengthUint - 1)
				m.Topics = make([]OffsetFetchRequestOffsetFetchRequestTopic, length)
				for i := int32(0); i < length; i++ {
					// Name
					if version >= 0 && version <= 7 {
						if isFlexible {
							val, err := protocol.ReadCompactString(r)
							if err != nil {
								return err
							}
							m.Topics[i].Name = val
						} else {
							val, err := protocol.ReadString(r)
							if err != nil {
								return err
							}
							m.Topics[i].Name = val
						}
					}
					// PartitionIndexes
					if version >= 0 && version <= 7 {
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
							m.Topics[i].PartitionIndexes = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].PartitionIndexes[i] = val
							}
						} else {
							var err error
							length, err = protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.Topics[i].PartitionIndexes = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].PartitionIndexes[i] = val
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
			if length == -1 {
				m.Topics = nil
			} else {
				m.Topics = make([]OffsetFetchRequestOffsetFetchRequestTopic, length)
				for i := int32(0); i < length; i++ {
					// Name
					if version >= 0 && version <= 7 {
						if isFlexible {
							val, err := protocol.ReadCompactString(r)
							if err != nil {
								return err
							}
							m.Topics[i].Name = val
						} else {
							val, err := protocol.ReadString(r)
							if err != nil {
								return err
							}
							m.Topics[i].Name = val
						}
					}
					// PartitionIndexes
					if version >= 0 && version <= 7 {
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
							m.Topics[i].PartitionIndexes = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].PartitionIndexes[i] = val
							}
						} else {
							var err error
							length, err = protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.Topics[i].PartitionIndexes = make([]int32, length)
							for i := int32(0); i < length; i++ {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].PartitionIndexes[i] = val
							}
						}
					}
				}
			}
		}
	}
	// Groups
	if version >= 8 && version <= 999 {
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
			m.Groups = make([]OffsetFetchRequestOffsetFetchRequestGroup, length)
			for i := int32(0); i < length; i++ {
				// GroupId
				if version >= 8 && version <= 999 {
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
				// MemberId
				if version >= 9 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].MemberId = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].MemberId = val
					}
				}
				// MemberEpoch
				if version >= 9 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].MemberEpoch = val
				}
				// Topics
				if version >= 8 && version <= 999 {
					var length int32
					if isFlexible {
						var lengthUint uint32
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint == 0 {
							m.Groups[i].Topics = nil
						} else {
							if lengthUint < 1 {
								return errors.New("invalid compact array length")
							}
							length = int32(lengthUint - 1)
							m.Groups[i].Topics = make([]OffsetFetchRequestOffsetFetchRequestTopics, length)
							for i := int32(0); i < length; i++ {
								// Name
								if version >= 8 && version <= 9 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Name = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Name = val
									}
								}
								// TopicId
								if version >= 10 && version <= 999 {
									val, err := protocol.ReadUUID(r)
									if err != nil {
										return err
									}
									m.Groups[i].Topics[i].TopicId = val
								}
								// PartitionIndexes
								if version >= 8 && version <= 999 {
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
										m.Groups[i].Topics[i].PartitionIndexes = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].PartitionIndexes[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].PartitionIndexes = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].PartitionIndexes[i] = val
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
						if length == -1 {
							m.Groups[i].Topics = nil
						} else {
							m.Groups[i].Topics = make([]OffsetFetchRequestOffsetFetchRequestTopics, length)
							for i := int32(0); i < length; i++ {
								// Name
								if version >= 8 && version <= 9 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Name = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Name = val
									}
								}
								// TopicId
								if version >= 10 && version <= 999 {
									val, err := protocol.ReadUUID(r)
									if err != nil {
										return err
									}
									m.Groups[i].Topics[i].TopicId = val
								}
								// PartitionIndexes
								if version >= 8 && version <= 999 {
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
										m.Groups[i].Topics[i].PartitionIndexes = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].PartitionIndexes[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].PartitionIndexes = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].PartitionIndexes[i] = val
										}
									}
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
			m.Groups = make([]OffsetFetchRequestOffsetFetchRequestGroup, length)
			for i := int32(0); i < length; i++ {
				// GroupId
				if version >= 8 && version <= 999 {
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
				// MemberId
				if version >= 9 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].MemberId = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].MemberId = val
					}
				}
				// MemberEpoch
				if version >= 9 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].MemberEpoch = val
				}
				// Topics
				if version >= 8 && version <= 999 {
					var length int32
					if isFlexible {
						var lengthUint uint32
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint == 0 {
							m.Groups[i].Topics = nil
						} else {
							if lengthUint < 1 {
								return errors.New("invalid compact array length")
							}
							length = int32(lengthUint - 1)
							m.Groups[i].Topics = make([]OffsetFetchRequestOffsetFetchRequestTopics, length)
							for i := int32(0); i < length; i++ {
								// Name
								if version >= 8 && version <= 9 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Name = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Name = val
									}
								}
								// TopicId
								if version >= 10 && version <= 999 {
									val, err := protocol.ReadUUID(r)
									if err != nil {
										return err
									}
									m.Groups[i].Topics[i].TopicId = val
								}
								// PartitionIndexes
								if version >= 8 && version <= 999 {
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
										m.Groups[i].Topics[i].PartitionIndexes = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].PartitionIndexes[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].PartitionIndexes = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].PartitionIndexes[i] = val
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
						if length == -1 {
							m.Groups[i].Topics = nil
						} else {
							m.Groups[i].Topics = make([]OffsetFetchRequestOffsetFetchRequestTopics, length)
							for i := int32(0); i < length; i++ {
								// Name
								if version >= 8 && version <= 9 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Name = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Name = val
									}
								}
								// TopicId
								if version >= 10 && version <= 999 {
									val, err := protocol.ReadUUID(r)
									if err != nil {
										return err
									}
									m.Groups[i].Topics[i].TopicId = val
								}
								// PartitionIndexes
								if version >= 8 && version <= 999 {
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
										m.Groups[i].Topics[i].PartitionIndexes = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].PartitionIndexes[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].PartitionIndexes = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].PartitionIndexes[i] = val
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	// RequireStable
	if version >= 7 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.RequireStable = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// OffsetFetchRequestOffsetFetchRequestTopic represents Each topic we would like to fetch offsets for, or null to fetch offsets for all topics..
type OffsetFetchRequestOffsetFetchRequestTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-7"`
	// The partition indexes we would like to fetch offsets for.
	PartitionIndexes []int32 `json:"partitionindexes" versions:"0-7"`
}

// OffsetFetchRequestOffsetFetchRequestGroup represents Each group we would like to fetch offsets for..
type OffsetFetchRequestOffsetFetchRequestGroup struct {
	// The group ID.
	GroupId string `json:"groupid" versions:"8-999"`
	// The member id.
	MemberId *string `json:"memberid" versions:"9-999"`
	// The member epoch if using the new consumer protocol (KIP-848).
	MemberEpoch int32 `json:"memberepoch" versions:"9-999"`
	// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
	Topics []OffsetFetchRequestOffsetFetchRequestTopics `json:"topics" versions:"8-999"`
}

// OffsetFetchRequestOffsetFetchRequestTopics represents Each topic we would like to fetch offsets for, or null to fetch offsets for all topics..
type OffsetFetchRequestOffsetFetchRequestTopics struct {
	// The topic name.
	Name string `json:"name" versions:"8-9"`
	// The topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"10-999"`
	// The partition indexes we would like to fetch offsets for.
	PartitionIndexes []int32 `json:"partitionindexes" versions:"8-999"`
}

// writeTaggedFields writes tagged fields for OffsetFetchRequest.
func (m *OffsetFetchRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for OffsetFetchRequest.
func (m *OffsetFetchRequest) readTaggedFields(r io.Reader, version int16) error {
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
