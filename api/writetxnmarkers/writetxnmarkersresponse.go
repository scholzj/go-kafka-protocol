package writetxnmarkers

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	WriteTxnMarkersResponseApiKey        = 27
	WriteTxnMarkersResponseHeaderVersion = 1
)

// WriteTxnMarkersResponse represents a response message.
type WriteTxnMarkersResponse struct {
	// The results for writing makers.
	Markers []WriteTxnMarkersResponseWritableTxnMarkerResult `json:"markers" versions:"0-999"`
}

// Encode encodes a WriteTxnMarkersResponse to a byte slice for the given version.
func (m *WriteTxnMarkersResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a WriteTxnMarkersResponse from a byte slice for the given version.
func (m *WriteTxnMarkersResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a WriteTxnMarkersResponse to an io.Writer for the given version.
func (m *WriteTxnMarkersResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
		isFlexible = true
	}

	// Markers
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Markers) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Markers))); err != nil {
				return err
			}
		}
		for i := range m.Markers {
			// ProducerId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(w, m.Markers[i].ProducerId); err != nil {
					return err
				}
			}
			// Topics
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Markers[i].Topics) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Markers[i].Topics))); err != nil {
						return err
					}
				}
				for i := range m.Markers[i].Topics {
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Markers[i].Topics[i].Name); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Markers[i].Topics[i].Name); err != nil {
								return err
							}
						}
					}
					// Partitions
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Markers[i].Topics[i].Partitions) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Markers[i].Topics[i].Partitions))); err != nil {
								return err
							}
						}
						for i := range m.Markers[i].Topics[i].Partitions {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Markers[i].Topics[i].Partitions[i].PartitionIndex); err != nil {
									return err
								}
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(w, m.Markers[i].Topics[i].Partitions[i].ErrorCode); err != nil {
									return err
								}
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

// Read reads a WriteTxnMarkersResponse from an io.Reader for the given version.
func (m *WriteTxnMarkersResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
		isFlexible = true
	}

	// Markers
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
			m.Markers = make([]WriteTxnMarkersResponseWritableTxnMarkerResult, length)
			for i := int32(0); i < length; i++ {
				// ProducerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Markers[i].ProducerId = val
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
						if lengthUint < 1 {
							return errors.New("invalid compact array length")
						}
						length = int32(lengthUint - 1)
						m.Markers[i].Topics = make([]WriteTxnMarkersResponseWritableTxnMarkerTopicResult, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Name = val
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
									m.Markers[i].Topics[i].Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].ErrorCode = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].ErrorCode = val
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
						m.Markers[i].Topics = make([]WriteTxnMarkersResponseWritableTxnMarkerTopicResult, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Name = val
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
									m.Markers[i].Topics[i].Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].ErrorCode = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].ErrorCode = val
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
			m.Markers = make([]WriteTxnMarkersResponseWritableTxnMarkerResult, length)
			for i := int32(0); i < length; i++ {
				// ProducerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Markers[i].ProducerId = val
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
						if lengthUint < 1 {
							return errors.New("invalid compact array length")
						}
						length = int32(lengthUint - 1)
						m.Markers[i].Topics = make([]WriteTxnMarkersResponseWritableTxnMarkerTopicResult, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Name = val
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
									m.Markers[i].Topics[i].Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].ErrorCode = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].ErrorCode = val
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
						m.Markers[i].Topics = make([]WriteTxnMarkersResponseWritableTxnMarkerTopicResult, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Name = val
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
									m.Markers[i].Topics[i].Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].ErrorCode = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].Partitions = make([]WriteTxnMarkersResponseWritableTxnMarkerPartitionResult, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// ErrorCode
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Markers[i].Topics[i].Partitions[i].ErrorCode = val
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

// WriteTxnMarkersResponseWritableTxnMarkerResult represents The results for writing makers..
type WriteTxnMarkersResponseWritableTxnMarkerResult struct {
	// The current producer ID in use by the transactional ID.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// The results by topic.
	Topics []WriteTxnMarkersResponseWritableTxnMarkerTopicResult `json:"topics" versions:"0-999"`
}

// WriteTxnMarkersResponseWritableTxnMarkerTopicResult represents The results by topic..
type WriteTxnMarkersResponseWritableTxnMarkerTopicResult struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The results by partition.
	Partitions []WriteTxnMarkersResponseWritableTxnMarkerPartitionResult `json:"partitions" versions:"0-999"`
}

// WriteTxnMarkersResponseWritableTxnMarkerPartitionResult represents The results by partition..
type WriteTxnMarkersResponseWritableTxnMarkerPartitionResult struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for WriteTxnMarkersResponse.
func (m *WriteTxnMarkersResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for WriteTxnMarkersResponse.
func (m *WriteTxnMarkersResponse) readTaggedFields(r io.Reader, version int16) error {
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
