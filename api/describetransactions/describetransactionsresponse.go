package describetransactions

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeTransactionsResponseApiKey        = 65
	DescribeTransactionsResponseHeaderVersion = 1
)

// DescribeTransactionsResponse represents a response message.
type DescribeTransactionsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The current state of the transaction.
	TransactionStates []DescribeTransactionsResponseTransactionState `json:"transactionstates" versions:"0-999"`
}

// Encode encodes a DescribeTransactionsResponse to a byte slice for the given version.
func (m *DescribeTransactionsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeTransactionsResponse from a byte slice for the given version.
func (m *DescribeTransactionsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeTransactionsResponse to an io.Writer for the given version.
func (m *DescribeTransactionsResponse) Write(w io.Writer, version int16) error {
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
	// TransactionStates
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.TransactionStates) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.TransactionStates))); err != nil {
				return err
			}
		}
		for i := range m.TransactionStates {
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.TransactionStates[i].ErrorCode); err != nil {
					return err
				}
			}
			// TransactionalId
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.TransactionStates[i].TransactionalId); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.TransactionStates[i].TransactionalId); err != nil {
						return err
					}
				}
			}
			// TransactionState
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.TransactionStates[i].TransactionState); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.TransactionStates[i].TransactionState); err != nil {
						return err
					}
				}
			}
			// TransactionTimeoutMs
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.TransactionStates[i].TransactionTimeoutMs); err != nil {
					return err
				}
			}
			// TransactionStartTimeMs
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(w, m.TransactionStates[i].TransactionStartTimeMs); err != nil {
					return err
				}
			}
			// ProducerId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(w, m.TransactionStates[i].ProducerId); err != nil {
					return err
				}
			}
			// ProducerEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.TransactionStates[i].ProducerEpoch); err != nil {
					return err
				}
			}
			// Topics
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.TransactionStates[i].Topics) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.TransactionStates[i].Topics))); err != nil {
						return err
					}
				}
				for i := range m.TransactionStates[i].Topics {
					// Topic
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.TransactionStates[i].Topics[i].Topic); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.TransactionStates[i].Topics[i].Topic); err != nil {
								return err
							}
						}
					}
					// Partitions
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.TransactionStates[i].Topics[i].Partitions) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.TransactionStates[i].Topics[i].Partitions))); err != nil {
								return err
							}
						}
						for i := range m.TransactionStates[i].Topics[i].Partitions {
							if err := protocol.WriteInt32(w, m.TransactionStates[i].Topics[i].Partitions[i]); err != nil {
								return err
							}
							_ = i
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

// Read reads a DescribeTransactionsResponse from an io.Reader for the given version.
func (m *DescribeTransactionsResponse) Read(r io.Reader, version int16) error {
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
	// TransactionStates
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
			m.TransactionStates = make([]DescribeTransactionsResponseTransactionState, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].ErrorCode = val
				}
				// TransactionalId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.TransactionStates[i].TransactionalId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.TransactionStates[i].TransactionalId = val
					}
				}
				// TransactionState
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.TransactionStates[i].TransactionState = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.TransactionStates[i].TransactionState = val
					}
				}
				// TransactionTimeoutMs
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].TransactionTimeoutMs = val
				}
				// TransactionStartTimeMs
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].TransactionStartTimeMs = val
				}
				// ProducerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].ProducerId = val
				}
				// ProducerEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].ProducerEpoch = val
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
						m.TransactionStates[i].Topics = make([]DescribeTransactionsResponseTopicData, length)
						for i := int32(0); i < length; i++ {
							// Topic
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Topic = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Topic = val
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
									m.TransactionStates[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.TransactionStates[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.TransactionStates[i].Topics[i].Partitions[i] = val
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
						m.TransactionStates[i].Topics = make([]DescribeTransactionsResponseTopicData, length)
						for i := int32(0); i < length; i++ {
							// Topic
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Topic = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Topic = val
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
									m.TransactionStates[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.TransactionStates[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.TransactionStates[i].Topics[i].Partitions[i] = val
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
			m.TransactionStates = make([]DescribeTransactionsResponseTransactionState, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].ErrorCode = val
				}
				// TransactionalId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.TransactionStates[i].TransactionalId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.TransactionStates[i].TransactionalId = val
					}
				}
				// TransactionState
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.TransactionStates[i].TransactionState = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.TransactionStates[i].TransactionState = val
					}
				}
				// TransactionTimeoutMs
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].TransactionTimeoutMs = val
				}
				// TransactionStartTimeMs
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].TransactionStartTimeMs = val
				}
				// ProducerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].ProducerId = val
				}
				// ProducerEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].ProducerEpoch = val
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
						m.TransactionStates[i].Topics = make([]DescribeTransactionsResponseTopicData, length)
						for i := int32(0); i < length; i++ {
							// Topic
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Topic = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Topic = val
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
									m.TransactionStates[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.TransactionStates[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.TransactionStates[i].Topics[i].Partitions[i] = val
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
						m.TransactionStates[i].Topics = make([]DescribeTransactionsResponseTopicData, length)
						for i := int32(0); i < length; i++ {
							// Topic
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Topic = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Topic = val
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
									m.TransactionStates[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.TransactionStates[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.TransactionStates[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.TransactionStates[i].Topics[i].Partitions[i] = val
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

// DescribeTransactionsResponseTransactionState represents The current state of the transaction..
type DescribeTransactionsResponseTransactionState struct {
	// The error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The transactional id.
	TransactionalId string `json:"transactionalid" versions:"0-999"`
	// The current transaction state of the producer.
	TransactionState string `json:"transactionstate" versions:"0-999"`
	// The timeout in milliseconds for the transaction.
	TransactionTimeoutMs int32 `json:"transactiontimeoutms" versions:"0-999"`
	// The start time of the transaction in milliseconds.
	TransactionStartTimeMs int64 `json:"transactionstarttimems" versions:"0-999"`
	// The current producer id associated with the transaction.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// The current epoch associated with the producer id.
	ProducerEpoch int16 `json:"producerepoch" versions:"0-999"`
	// The set of partitions included in the current transaction (if active). When a transaction is preparing to commit or abort, this will include only partitions which do not have markers.
	Topics []DescribeTransactionsResponseTopicData `json:"topics" versions:"0-999"`
}

// DescribeTransactionsResponseTopicData represents The set of partitions included in the current transaction (if active). When a transaction is preparing to commit or abort, this will include only partitions which do not have markers..
type DescribeTransactionsResponseTopicData struct {
	// The topic name.
	Topic string `json:"topic" versions:"0-999"`
	// The partition ids included in the current transaction.
	Partitions []int32 `json:"partitions" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeTransactionsResponse.
func (m *DescribeTransactionsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeTransactionsResponse.
func (m *DescribeTransactionsResponse) readTaggedFields(r io.Reader, version int16) error {
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
