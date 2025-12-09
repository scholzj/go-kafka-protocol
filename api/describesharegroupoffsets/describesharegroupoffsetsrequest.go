package describesharegroupoffsets

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeShareGroupOffsetsRequestApiKey        = 90
	DescribeShareGroupOffsetsRequestHeaderVersion = 1
)

// DescribeShareGroupOffsetsRequest represents a request message.
type DescribeShareGroupOffsetsRequest struct {
	// The groups to describe offsets for.
	Groups []DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup `json:"groups" versions:"0-999"`
}

// Encode encodes a DescribeShareGroupOffsetsRequest to a byte slice for the given version.
func (m *DescribeShareGroupOffsetsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeShareGroupOffsetsRequest from a byte slice for the given version.
func (m *DescribeShareGroupOffsetsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeShareGroupOffsetsRequest to an io.Writer for the given version.
func (m *DescribeShareGroupOffsetsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
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
			// Topics
			if version >= 0 && version <= 999 {
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
						// TopicName
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(w, m.Groups[i].Topics[i].TopicName); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(w, m.Groups[i].Topics[i].TopicName); err != nil {
									return err
								}
							}
						}
						// Partitions
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(m.Groups[i].Topics[i].Partitions) + 1)
								if err := protocol.WriteVaruint32(w, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Topics[i].Partitions))); err != nil {
									return err
								}
							}
							for i := range m.Groups[i].Topics[i].Partitions {
								if err := protocol.WriteInt32(w, m.Groups[i].Topics[i].Partitions[i]); err != nil {
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
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a DescribeShareGroupOffsetsRequest from an io.Reader for the given version.
func (m *DescribeShareGroupOffsetsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
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
			m.Groups = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup, length)
			for i := int32(0); i < length; i++ {
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
				// Topics
				if version >= 0 && version <= 999 {
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
							m.Groups[i].Topics = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic, length)
							for i := int32(0); i < length; i++ {
								// TopicName
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].TopicName = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].TopicName = val
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
										m.Groups[i].Topics[i].Partitions = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].Partitions[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Partitions = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].Partitions[i] = val
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
							m.Groups[i].Topics = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic, length)
							for i := int32(0); i < length; i++ {
								// TopicName
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].TopicName = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].TopicName = val
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
										m.Groups[i].Topics[i].Partitions = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].Partitions[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Partitions = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].Partitions[i] = val
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
			m.Groups = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup, length)
			for i := int32(0); i < length; i++ {
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
				// Topics
				if version >= 0 && version <= 999 {
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
							m.Groups[i].Topics = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic, length)
							for i := int32(0); i < length; i++ {
								// TopicName
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].TopicName = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].TopicName = val
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
										m.Groups[i].Topics[i].Partitions = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].Partitions[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Partitions = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].Partitions[i] = val
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
							m.Groups[i].Topics = make([]DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic, length)
							for i := int32(0); i < length; i++ {
								// TopicName
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].TopicName = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].TopicName = val
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
										m.Groups[i].Topics[i].Partitions = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].Partitions[i] = val
										}
									} else {
										var err error
										length, err = protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Groups[i].Topics[i].Partitions = make([]int32, length)
										for i := int32(0); i < length; i++ {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topics[i].Partitions[i] = val
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
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup represents The groups to describe offsets for..
type DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup struct {
	// The group identifier.
	GroupId string `json:"groupid" versions:"0-999"`
	// The topics to describe offsets for, or null for all topic-partitions.
	Topics []DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic `json:"topics" versions:"0-999"`
}

// DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic represents The topics to describe offsets for, or null for all topic-partitions..
type DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic struct {
	// The topic name.
	TopicName string `json:"topicname" versions:"0-999"`
	// The partitions.
	Partitions []int32 `json:"partitions" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeShareGroupOffsetsRequest.
func (m *DescribeShareGroupOffsetsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeShareGroupOffsetsRequest.
func (m *DescribeShareGroupOffsetsRequest) readTaggedFields(r io.Reader, version int16) error {
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
