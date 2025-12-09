package addpartitionstotxn

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AddPartitionsToTxnRequestApiKey        = 24
	AddPartitionsToTxnRequestHeaderVersion = 1
)

// AddPartitionsToTxnRequest represents a request message.
type AddPartitionsToTxnRequest struct {
	// List of transactions to add partitions to.
	Transactions []AddPartitionsToTxnRequestAddPartitionsToTxnTransaction `json:"transactions" versions:"4-999"`
	// The transactional id corresponding to the transaction.
	V3AndBelowTransactionalId string `json:"v3andbelowtransactionalid" versions:"0-3"`
	// Current producer id in use by the transactional id.
	V3AndBelowProducerId int64 `json:"v3andbelowproducerid" versions:"0-3"`
	// Current epoch associated with the producer id.
	V3AndBelowProducerEpoch int16 `json:"v3andbelowproducerepoch" versions:"0-3"`
	// The partitions to add to the transaction.
	V3AndBelowTopics []AddPartitionsToTxnTopic `json:"v3andbelowtopics" versions:"0-3"`
}

// Encode encodes a AddPartitionsToTxnRequest to a byte slice for the given version.
func (m *AddPartitionsToTxnRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AddPartitionsToTxnRequest from a byte slice for the given version.
func (m *AddPartitionsToTxnRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AddPartitionsToTxnRequest to an io.Writer for the given version.
func (m *AddPartitionsToTxnRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// Transactions
	if version >= 4 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Transactions) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Transactions))); err != nil {
				return err
			}
		}
		for i := range m.Transactions {
			// TransactionalId
			if version >= 4 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Transactions[i].TransactionalId); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Transactions[i].TransactionalId); err != nil {
						return err
					}
				}
			}
			// ProducerId
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt64(w, m.Transactions[i].ProducerId); err != nil {
					return err
				}
			}
			// ProducerEpoch
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Transactions[i].ProducerEpoch); err != nil {
					return err
				}
			}
			// VerifyOnly
			if version >= 4 && version <= 999 {
				if err := protocol.WriteBool(w, m.Transactions[i].VerifyOnly); err != nil {
					return err
				}
			}
			// Topics
			if version >= 4 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Transactions[i].Topics) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Transactions[i].Topics))); err != nil {
						return err
					}
				}
				for i := range m.Transactions[i].Topics {
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Transactions[i].Topics[i].Name); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Transactions[i].Topics[i].Name); err != nil {
								return err
							}
						}
					}
					// Partitions
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Transactions[i].Topics[i].Partitions) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Transactions[i].Topics[i].Partitions))); err != nil {
								return err
							}
						}
						for i := range m.Transactions[i].Topics[i].Partitions {
							if err := protocol.WriteInt32(w, m.Transactions[i].Topics[i].Partitions[i]); err != nil {
								return err
							}
							_ = i
						}
					}
				}
			}
		}
	}
	// V3AndBelowTransactionalId
	if version >= 0 && version <= 3 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.V3AndBelowTransactionalId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.V3AndBelowTransactionalId); err != nil {
				return err
			}
		}
	}
	// V3AndBelowProducerId
	if version >= 0 && version <= 3 {
		if err := protocol.WriteInt64(w, m.V3AndBelowProducerId); err != nil {
			return err
		}
	}
	// V3AndBelowProducerEpoch
	if version >= 0 && version <= 3 {
		if err := protocol.WriteInt16(w, m.V3AndBelowProducerEpoch); err != nil {
			return err
		}
	}
	// V3AndBelowTopics
	if version >= 0 && version <= 3 {
		if isFlexible {
			length := uint32(len(m.V3AndBelowTopics) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.V3AndBelowTopics))); err != nil {
				return err
			}
		}
		for i := range m.V3AndBelowTopics {
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.V3AndBelowTopics[i].Name); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.V3AndBelowTopics[i].Name); err != nil {
						return err
					}
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.V3AndBelowTopics[i].Partitions) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.V3AndBelowTopics[i].Partitions))); err != nil {
						return err
					}
				}
				for i := range m.V3AndBelowTopics[i].Partitions {
					if err := protocol.WriteInt32(w, m.V3AndBelowTopics[i].Partitions[i]); err != nil {
						return err
					}
					_ = i
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

// Read reads a AddPartitionsToTxnRequest from an io.Reader for the given version.
func (m *AddPartitionsToTxnRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// Transactions
	if version >= 4 && version <= 999 {
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
			m.Transactions = make([]AddPartitionsToTxnRequestAddPartitionsToTxnTransaction, length)
			for i := int32(0); i < length; i++ {
				// TransactionalId
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Transactions[i].TransactionalId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Transactions[i].TransactionalId = val
					}
				}
				// ProducerId
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Transactions[i].ProducerId = val
				}
				// ProducerEpoch
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Transactions[i].ProducerEpoch = val
				}
				// VerifyOnly
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					m.Transactions[i].VerifyOnly = val
				}
				// Topics
				if version >= 4 && version <= 999 {
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
						m.Transactions[i].Topics = make([]AddPartitionsToTxnTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Name = val
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
									m.Transactions[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Transactions[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Transactions[i].Topics[i].Partitions[i] = val
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
						m.Transactions[i].Topics = make([]AddPartitionsToTxnTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Name = val
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
									m.Transactions[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Transactions[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Transactions[i].Topics[i].Partitions[i] = val
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
			m.Transactions = make([]AddPartitionsToTxnRequestAddPartitionsToTxnTransaction, length)
			for i := int32(0); i < length; i++ {
				// TransactionalId
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Transactions[i].TransactionalId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Transactions[i].TransactionalId = val
					}
				}
				// ProducerId
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Transactions[i].ProducerId = val
				}
				// ProducerEpoch
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Transactions[i].ProducerEpoch = val
				}
				// VerifyOnly
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					m.Transactions[i].VerifyOnly = val
				}
				// Topics
				if version >= 4 && version <= 999 {
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
						m.Transactions[i].Topics = make([]AddPartitionsToTxnTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Name = val
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
									m.Transactions[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Transactions[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Transactions[i].Topics[i].Partitions[i] = val
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
						m.Transactions[i].Topics = make([]AddPartitionsToTxnTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Name = val
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
									m.Transactions[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Transactions[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Transactions[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Transactions[i].Topics[i].Partitions[i] = val
									}
								}
							}
						}
					}
				}
			}
		}
	}
	// V3AndBelowTransactionalId
	if version >= 0 && version <= 3 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.V3AndBelowTransactionalId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.V3AndBelowTransactionalId = val
		}
	}
	// V3AndBelowProducerId
	if version >= 0 && version <= 3 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.V3AndBelowProducerId = val
	}
	// V3AndBelowProducerEpoch
	if version >= 0 && version <= 3 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.V3AndBelowProducerEpoch = val
	}
	// V3AndBelowTopics
	if version >= 0 && version <= 3 {
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
			m.V3AndBelowTopics = make([]AddPartitionsToTxnTopic, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.V3AndBelowTopics[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.V3AndBelowTopics[i].Name = val
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
						m.V3AndBelowTopics[i].Partitions = make([]int32, length)
						for i := int32(0); i < length; i++ {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.V3AndBelowTopics[i].Partitions[i] = val
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.V3AndBelowTopics[i].Partitions = make([]int32, length)
						for i := int32(0); i < length; i++ {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.V3AndBelowTopics[i].Partitions[i] = val
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
			m.V3AndBelowTopics = make([]AddPartitionsToTxnTopic, length)
			for i := int32(0); i < length; i++ {
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.V3AndBelowTopics[i].Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.V3AndBelowTopics[i].Name = val
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
						m.V3AndBelowTopics[i].Partitions = make([]int32, length)
						for i := int32(0); i < length; i++ {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.V3AndBelowTopics[i].Partitions[i] = val
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.V3AndBelowTopics[i].Partitions = make([]int32, length)
						for i := int32(0); i < length; i++ {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.V3AndBelowTopics[i].Partitions[i] = val
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

// AddPartitionsToTxnRequestAddPartitionsToTxnTransaction represents List of transactions to add partitions to..
type AddPartitionsToTxnRequestAddPartitionsToTxnTransaction struct {
	// The transactional id corresponding to the transaction.
	TransactionalId string `json:"transactionalid" versions:"4-999"`
	// Current producer id in use by the transactional id.
	ProducerId int64 `json:"producerid" versions:"4-999"`
	// Current epoch associated with the producer id.
	ProducerEpoch int16 `json:"producerepoch" versions:"4-999"`
	// Boolean to signify if we want to check if the partition is in the transaction rather than add it.
	VerifyOnly bool `json:"verifyonly" versions:"4-999"`
	// The partitions to add to the transaction.
	Topics []AddPartitionsToTxnTopic `json:"topics" versions:"4-999"`
}

// AddPartitionsToTxnTopic represents .
type AddPartitionsToTxnTopic struct {
	// The name of the topic.
	Name string `json:"name" versions:"0-999"`
	// The partition indexes to add to the transaction.
	Partitions []int32 `json:"partitions" versions:"0-999"`
}

// AddPartitionsToTxnTopicResult represents .
type AddPartitionsToTxnTopicResult struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The results for each partition.
	ResultsByPartition []AddPartitionsToTxnPartitionResult `json:"resultsbypartition" versions:"0-999"`
}

// AddPartitionsToTxnPartitionResult represents .
type AddPartitionsToTxnPartitionResult struct {
	// The partition indexes.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The response error code.
	PartitionErrorCode int16 `json:"partitionerrorcode" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for AddPartitionsToTxnRequest.
func (m *AddPartitionsToTxnRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AddPartitionsToTxnRequest.
func (m *AddPartitionsToTxnRequest) readTaggedFields(r io.Reader, version int16) error {
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
