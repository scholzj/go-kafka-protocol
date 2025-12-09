package describeproducers

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeProducersResponseApiKey        = 61
	DescribeProducersResponseHeaderVersion = 1
)

// DescribeProducersResponse represents a response message.
type DescribeProducersResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// Each topic in the response.
	Topics []DescribeProducersResponseTopicResponse `json:"topics" versions:"0-999"`
}

// Encode encodes a DescribeProducersResponse to a byte slice for the given version.
func (m *DescribeProducersResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeProducersResponse from a byte slice for the given version.
func (m *DescribeProducersResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeProducersResponse to an io.Writer for the given version.
func (m *DescribeProducersResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
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
	// Topics
	if version >= 0 && version <= 999 {
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
			if version >= 0 && version <= 999 {
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
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Topics[i].Partitions) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions))); err != nil {
						return err
					}
				}
				for i := range m.Topics[i].Partitions {
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].PartitionIndex); err != nil {
							return err
						}
					}
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(w, m.Topics[i].Partitions[i].ErrorCode); err != nil {
							return err
						}
					}
					// ErrorMessage
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Topics[i].Partitions[i].ErrorMessage); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Topics[i].Partitions[i].ErrorMessage); err != nil {
								return err
							}
						}
					}
					// ActiveProducers
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].ActiveProducers) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].ActiveProducers))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].ActiveProducers {
							// ProducerId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].ActiveProducers[i].ProducerId); err != nil {
									return err
								}
							}
							// ProducerEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].ActiveProducers[i].ProducerEpoch); err != nil {
									return err
								}
							}
							// LastSequence
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].ActiveProducers[i].LastSequence); err != nil {
									return err
								}
							}
							// LastTimestamp
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].ActiveProducers[i].LastTimestamp); err != nil {
									return err
								}
							}
							// CoordinatorEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].ActiveProducers[i].CoordinatorEpoch); err != nil {
									return err
								}
							}
							// CurrentTxnStartOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].ActiveProducers[i].CurrentTxnStartOffset); err != nil {
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

// Read reads a DescribeProducersResponse from an io.Reader for the given version.
func (m *DescribeProducersResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
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
			m.Topics = make([]DescribeProducersResponseTopicResponse, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
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
						m.Topics[i].Partitions = make([]DescribeProducersResponsePartitionResponse, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								}
							}
							// ActiveProducers
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
									m.Topics[i].Partitions[i].ActiveProducers = make([]DescribeProducersResponseProducerState, length)
									for i := int32(0); i < length; i++ {
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CurrentTxnStartOffset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ActiveProducers = make([]DescribeProducersResponseProducerState, length)
									for i := int32(0); i < length; i++ {
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CurrentTxnStartOffset = val
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
						m.Topics[i].Partitions = make([]DescribeProducersResponsePartitionResponse, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								}
							}
							// ActiveProducers
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
									m.Topics[i].Partitions[i].ActiveProducers = make([]DescribeProducersResponseProducerState, length)
									for i := int32(0); i < length; i++ {
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CurrentTxnStartOffset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ActiveProducers = make([]DescribeProducersResponseProducerState, length)
									for i := int32(0); i < length; i++ {
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CurrentTxnStartOffset = val
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
			m.Topics = make([]DescribeProducersResponseTopicResponse, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
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
						m.Topics[i].Partitions = make([]DescribeProducersResponsePartitionResponse, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								}
							}
							// ActiveProducers
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
									m.Topics[i].Partitions[i].ActiveProducers = make([]DescribeProducersResponseProducerState, length)
									for i := int32(0); i < length; i++ {
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CurrentTxnStartOffset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ActiveProducers = make([]DescribeProducersResponseProducerState, length)
									for i := int32(0); i < length; i++ {
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CurrentTxnStartOffset = val
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
						m.Topics[i].Partitions = make([]DescribeProducersResponsePartitionResponse, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ErrorMessage = val
								}
							}
							// ActiveProducers
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
									m.Topics[i].Partitions[i].ActiveProducers = make([]DescribeProducersResponseProducerState, length)
									for i := int32(0); i < length; i++ {
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CurrentTxnStartOffset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].ActiveProducers = make([]DescribeProducersResponseProducerState, length)
									for i := int32(0); i < length; i++ {
										// ProducerId
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerId = val
										}
										// ProducerEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].ProducerEpoch = val
										}
										// LastSequence
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastSequence = val
										}
										// LastTimestamp
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].LastTimestamp = val
										}
										// CoordinatorEpoch
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CoordinatorEpoch = val
										}
										// CurrentTxnStartOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].ActiveProducers[i].CurrentTxnStartOffset = val
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

// DescribeProducersResponseTopicResponse represents Each topic in the response..
type DescribeProducersResponseTopicResponse struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// Each partition in the response.
	Partitions []DescribeProducersResponsePartitionResponse `json:"partitions" versions:"0-999"`
}

// DescribeProducersResponsePartitionResponse represents Each partition in the response..
type DescribeProducersResponsePartitionResponse struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The partition error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The partition error message, which may be null if no additional details are available.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The active producers for the partition.
	ActiveProducers []DescribeProducersResponseProducerState `json:"activeproducers" versions:"0-999"`
}

// DescribeProducersResponseProducerState represents The active producers for the partition..
type DescribeProducersResponseProducerState struct {
	// The producer id.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// The producer epoch.
	ProducerEpoch int32 `json:"producerepoch" versions:"0-999"`
	// The last sequence number sent by the producer.
	LastSequence int32 `json:"lastsequence" versions:"0-999"`
	// The last timestamp sent by the producer.
	LastTimestamp int64 `json:"lasttimestamp" versions:"0-999"`
	// The current epoch of the producer group.
	CoordinatorEpoch int32 `json:"coordinatorepoch" versions:"0-999"`
	// The current transaction start offset of the producer.
	CurrentTxnStartOffset int64 `json:"currenttxnstartoffset" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeProducersResponse.
func (m *DescribeProducersResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeProducersResponse.
func (m *DescribeProducersResponse) readTaggedFields(r io.Reader, version int16) error {
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
