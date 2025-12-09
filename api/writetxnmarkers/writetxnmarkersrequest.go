package writetxnmarkers

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	WriteTxnMarkersRequestApiKey        = 27
	WriteTxnMarkersRequestHeaderVersion = 1
)

// WriteTxnMarkersRequest represents a request message.
type WriteTxnMarkersRequest struct {
	// The transaction markers to be written.
	Markers []WriteTxnMarkersRequestWritableTxnMarker `json:"markers" versions:"0-999"`
}

// Encode encodes a WriteTxnMarkersRequest to a byte slice for the given version.
func (m *WriteTxnMarkersRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a WriteTxnMarkersRequest from a byte slice for the given version.
func (m *WriteTxnMarkersRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a WriteTxnMarkersRequest to an io.Writer for the given version.
func (m *WriteTxnMarkersRequest) Write(w io.Writer, version int16) error {
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
			// ProducerEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Markers[i].ProducerEpoch); err != nil {
					return err
				}
			}
			// TransactionResult
			if version >= 0 && version <= 999 {
				if err := protocol.WriteBool(w, m.Markers[i].TransactionResult); err != nil {
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
					// PartitionIndexes
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Markers[i].Topics[i].PartitionIndexes) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Markers[i].Topics[i].PartitionIndexes))); err != nil {
								return err
							}
						}
						for i := range m.Markers[i].Topics[i].PartitionIndexes {
							if err := protocol.WriteInt32(w, m.Markers[i].Topics[i].PartitionIndexes[i]); err != nil {
								return err
							}
							_ = i
						}
					}
				}
			}
			// CoordinatorEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Markers[i].CoordinatorEpoch); err != nil {
					return err
				}
			}
			// TransactionVersion
			if version >= 2 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Markers[i].TransactionVersion); err != nil {
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

// Read reads a WriteTxnMarkersRequest from an io.Reader for the given version.
func (m *WriteTxnMarkersRequest) Read(r io.Reader, version int16) error {
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
			m.Markers = make([]WriteTxnMarkersRequestWritableTxnMarker, length)
			for i := int32(0); i < length; i++ {
				// ProducerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Markers[i].ProducerId = val
				}
				// ProducerEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Markers[i].ProducerEpoch = val
				}
				// TransactionResult
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					m.Markers[i].TransactionResult = val
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
						m.Markers[i].Topics = make([]WriteTxnMarkersRequestWritableTxnMarkerTopic, length)
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
							// PartitionIndexes
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
									m.Markers[i].Topics[i].PartitionIndexes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Markers[i].Topics[i].PartitionIndexes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].PartitionIndexes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Markers[i].Topics[i].PartitionIndexes[i] = val
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
						m.Markers[i].Topics = make([]WriteTxnMarkersRequestWritableTxnMarkerTopic, length)
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
							// PartitionIndexes
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
									m.Markers[i].Topics[i].PartitionIndexes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Markers[i].Topics[i].PartitionIndexes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].PartitionIndexes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Markers[i].Topics[i].PartitionIndexes[i] = val
									}
								}
							}
						}
					}
				}
				// CoordinatorEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Markers[i].CoordinatorEpoch = val
				}
				// TransactionVersion
				if version >= 2 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Markers[i].TransactionVersion = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Markers = make([]WriteTxnMarkersRequestWritableTxnMarker, length)
			for i := int32(0); i < length; i++ {
				// ProducerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Markers[i].ProducerId = val
				}
				// ProducerEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Markers[i].ProducerEpoch = val
				}
				// TransactionResult
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					m.Markers[i].TransactionResult = val
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
						m.Markers[i].Topics = make([]WriteTxnMarkersRequestWritableTxnMarkerTopic, length)
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
							// PartitionIndexes
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
									m.Markers[i].Topics[i].PartitionIndexes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Markers[i].Topics[i].PartitionIndexes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].PartitionIndexes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Markers[i].Topics[i].PartitionIndexes[i] = val
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
						m.Markers[i].Topics = make([]WriteTxnMarkersRequestWritableTxnMarkerTopic, length)
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
							// PartitionIndexes
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
									m.Markers[i].Topics[i].PartitionIndexes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Markers[i].Topics[i].PartitionIndexes[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Markers[i].Topics[i].PartitionIndexes = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Markers[i].Topics[i].PartitionIndexes[i] = val
									}
								}
							}
						}
					}
				}
				// CoordinatorEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Markers[i].CoordinatorEpoch = val
				}
				// TransactionVersion
				if version >= 2 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Markers[i].TransactionVersion = val
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

// WriteTxnMarkersRequestWritableTxnMarker represents The transaction markers to be written..
type WriteTxnMarkersRequestWritableTxnMarker struct {
	// The current producer ID.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// The current epoch associated with the producer ID.
	ProducerEpoch int16 `json:"producerepoch" versions:"0-999"`
	// The result of the transaction to write to the partitions (false = ABORT, true = COMMIT).
	TransactionResult bool `json:"transactionresult" versions:"0-999"`
	// Each topic that we want to write transaction marker(s) for.
	Topics []WriteTxnMarkersRequestWritableTxnMarkerTopic `json:"topics" versions:"0-999"`
	// Epoch associated with the transaction state partition hosted by this transaction coordinator.
	CoordinatorEpoch int32 `json:"coordinatorepoch" versions:"0-999"`
	// Transaction version of the marker. Ex: 0/1 = legacy (TV0/TV1), 2 = TV2 etc.
	TransactionVersion int8 `json:"transactionversion" versions:"2-999"`
}

// WriteTxnMarkersRequestWritableTxnMarkerTopic represents Each topic that we want to write transaction marker(s) for..
type WriteTxnMarkersRequestWritableTxnMarkerTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The indexes of the partitions to write transaction markers for.
	PartitionIndexes []int32 `json:"partitionindexes" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for WriteTxnMarkersRequest.
func (m *WriteTxnMarkersRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for WriteTxnMarkersRequest.
func (m *WriteTxnMarkersRequest) readTaggedFields(r io.Reader, version int16) error {
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
