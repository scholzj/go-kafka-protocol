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
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(AddPartitionsToTxnRequestAddPartitionsToTxnTransaction)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// TransactionalId
			if version >= 4 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.TransactionalId); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.TransactionalId); err != nil {
						return nil, err
					}
				}
			}
			// ProducerId
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt64(elemW, structItem.ProducerId); err != nil {
					return nil, err
				}
			}
			// ProducerEpoch
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ProducerEpoch); err != nil {
					return nil, err
				}
			}
			// VerifyOnly
			if version >= 4 && version <= 999 {
				if err := protocol.WriteBool(elemW, structItem.VerifyOnly); err != nil {
					return nil, err
				}
			}
			// Topics
			if version >= 4 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Topics) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Topics))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Topics {
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Topics[i].Name); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Topics[i].Name); err != nil {
								return nil, err
							}
						}
					}
					// Partitions
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Topics[i].Partitions); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Topics[i].Partitions); err != nil {
								return nil, err
							}
						}
					}
				}
			}
			// Write tagged fields if flexible
			if isFlexible {
				if err := structItem.writeTaggedFields(elemW, version); err != nil {
					return nil, err
				}
			}
			return elemBuf.Bytes(), nil
		}
		items := make([]interface{}, len(m.Transactions))
		for i := range m.Transactions {
			items[i] = m.Transactions[i]
		}
		if isFlexible {
			if err := protocol.WriteCompactArray(w, items, encoder); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, items, encoder); err != nil {
				return err
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
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(AddPartitionsToTxnTopic)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32Array(elemW, structItem.Partitions); err != nil {
						return nil, err
					}
				}
			}
			// Write tagged fields if flexible
			if isFlexible {
				if err := structItem.writeTaggedFields(elemW, version); err != nil {
					return nil, err
				}
			}
			return elemBuf.Bytes(), nil
		}
		items := make([]interface{}, len(m.V3AndBelowTopics))
		for i := range m.V3AndBelowTopics {
			items[i] = m.V3AndBelowTopics[i]
		}
		if isFlexible {
			if err := protocol.WriteCompactArray(w, items, encoder); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, items, encoder); err != nil {
				return err
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
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem AddPartitionsToTxnRequestAddPartitionsToTxnTransaction
			elemR := bytes.NewReader(data)
			// TransactionalId
			if version >= 4 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TransactionalId = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TransactionalId = val
				}
			}
			// ProducerId
			if version >= 4 && version <= 999 {
				val, err := protocol.ReadInt64(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ProducerId = val
			}
			// ProducerEpoch
			if version >= 4 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ProducerEpoch = val
			}
			// VerifyOnly
			if version >= 4 && version <= 999 {
				val, err := protocol.ReadBool(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.VerifyOnly = val
			}
			// Topics
			if version >= 4 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// Read tagged fields if flexible
			if isFlexible {
				if err := elem.readTaggedFields(elemR, version); err != nil {
					return nil, 0, err
				}
			}
			consumed := len(data) - elemR.Len()
			return elem, consumed, nil
		}
		if isFlexible {
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint < 1 {
				return errors.New("invalid compact array length")
			}
			length := int32(lengthUint - 1)
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem AddPartitionsToTxnRequestAddPartitionsToTxnTransaction
				// TransactionalId
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TransactionalId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TransactionalId = val
					}
				}
				// ProducerId
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.ProducerId = val
				}
				// ProducerEpoch
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ProducerEpoch = val
				}
				// VerifyOnly
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					tempElem.VerifyOnly = val
				}
				// Topics
				if version >= 4 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AddPartitionsToTxnTopic
						elemR := bytes.NewReader(data)
						// Name
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Name = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Name = val
							}
						}
						// Partitions
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Partitions = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Partitions = val
							}
						}
						consumed := len(data) - elemR.Len()
						return elem, consumed, nil
					}
					if isFlexible {
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint < 1 {
							return errors.New("invalid compact array length")
						}
						length := int32(lengthUint - 1)
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem AddPartitionsToTxnTopic
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Name = val
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.Partitions = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.Partitions = val
								}
							}
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
										return err
									}
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.Partitions); err != nil {
										return err
									}
								}
							}
							// Append to array buffer
							arrayBuf.Write(elemBuf.Bytes())
						}
						// Prepend length and decode using DecodeCompactArray
						lengthBytes := protocol.EncodeVaruint32(lengthUint)
						fullData := append(lengthBytes, arrayBuf.Bytes()...)
						decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
						if err != nil {
							return err
						}
						// Convert []interface{} to typed slice
						tempElem.Topics = make([]AddPartitionsToTxnTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(AddPartitionsToTxnTopic)
						}
					} else {
						length, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem AddPartitionsToTxnTopic
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Name = val
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.Partitions = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.Partitions = val
								}
							}
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
										return err
									}
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.Partitions); err != nil {
										return err
									}
								}
							}
							// Append to array buffer
							arrayBuf.Write(elemBuf.Bytes())
						}
						// Prepend length and decode using DecodeArray
						lengthBytes := protocol.EncodeInt32(length)
						fullData := append(lengthBytes, arrayBuf.Bytes()...)
						decoded, _, err := protocol.DecodeArray(fullData, decoder)
						if err != nil {
							return err
						}
						// Convert []interface{} to typed slice
						tempElem.Topics = make([]AddPartitionsToTxnTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(AddPartitionsToTxnTopic)
						}
					}
				}
				// TransactionalId
				if version >= 4 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TransactionalId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TransactionalId); err != nil {
							return err
						}
					}
				}
				// ProducerId
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
						return err
					}
				}
				// ProducerEpoch
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ProducerEpoch); err != nil {
						return err
					}
				}
				// VerifyOnly
				if version >= 4 && version <= 999 {
					if err := protocol.WriteBool(elemW, tempElem.VerifyOnly); err != nil {
						return err
					}
				}
				// Topics
				if version >= 4 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Topics) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topics))); err != nil {
							return err
						}
					}
					for i := range tempElem.Topics {
						// Name
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
							}
						}
						// Partitions
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Topics[i].Partitions); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Topics[i].Partitions); err != nil {
									return err
								}
							}
						}
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeCompactArray
			lengthBytes := protocol.EncodeVaruint32(lengthUint)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.Transactions = make([]AddPartitionsToTxnRequestAddPartitionsToTxnTransaction, len(decoded))
			for i, item := range decoded {
				m.Transactions[i] = item.(AddPartitionsToTxnRequestAddPartitionsToTxnTransaction)
			}
		} else {
			length, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem AddPartitionsToTxnRequestAddPartitionsToTxnTransaction
				// TransactionalId
				if version >= 4 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TransactionalId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TransactionalId = val
					}
				}
				// ProducerId
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					tempElem.ProducerId = val
				}
				// ProducerEpoch
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ProducerEpoch = val
				}
				// VerifyOnly
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					tempElem.VerifyOnly = val
				}
				// Topics
				if version >= 4 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem AddPartitionsToTxnTopic
						elemR := bytes.NewReader(data)
						// Name
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Name = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Name = val
							}
						}
						// Partitions
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Partitions = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Partitions = val
							}
						}
						consumed := len(data) - elemR.Len()
						return elem, consumed, nil
					}
					if isFlexible {
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint < 1 {
							return errors.New("invalid compact array length")
						}
						length := int32(lengthUint - 1)
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem AddPartitionsToTxnTopic
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Name = val
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.Partitions = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.Partitions = val
								}
							}
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
										return err
									}
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.Partitions); err != nil {
										return err
									}
								}
							}
							// Append to array buffer
							arrayBuf.Write(elemBuf.Bytes())
						}
						// Prepend length and decode using DecodeCompactArray
						lengthBytes := protocol.EncodeVaruint32(lengthUint)
						fullData := append(lengthBytes, arrayBuf.Bytes()...)
						decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
						if err != nil {
							return err
						}
						// Convert []interface{} to typed slice
						tempElem.Topics = make([]AddPartitionsToTxnTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(AddPartitionsToTxnTopic)
						}
					} else {
						length, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem AddPartitionsToTxnTopic
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.Name = val
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.Partitions = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.Partitions = val
								}
							}
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
										return err
									}
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.Partitions); err != nil {
										return err
									}
								}
							}
							// Append to array buffer
							arrayBuf.Write(elemBuf.Bytes())
						}
						// Prepend length and decode using DecodeArray
						lengthBytes := protocol.EncodeInt32(length)
						fullData := append(lengthBytes, arrayBuf.Bytes()...)
						decoded, _, err := protocol.DecodeArray(fullData, decoder)
						if err != nil {
							return err
						}
						// Convert []interface{} to typed slice
						tempElem.Topics = make([]AddPartitionsToTxnTopic, len(decoded))
						for i, item := range decoded {
							tempElem.Topics[i] = item.(AddPartitionsToTxnTopic)
						}
					}
				}
				// TransactionalId
				if version >= 4 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TransactionalId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TransactionalId); err != nil {
							return err
						}
					}
				}
				// ProducerId
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt64(elemW, tempElem.ProducerId); err != nil {
						return err
					}
				}
				// ProducerEpoch
				if version >= 4 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ProducerEpoch); err != nil {
						return err
					}
				}
				// VerifyOnly
				if version >= 4 && version <= 999 {
					if err := protocol.WriteBool(elemW, tempElem.VerifyOnly); err != nil {
						return err
					}
				}
				// Topics
				if version >= 4 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Topics) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topics))); err != nil {
							return err
						}
					}
					for i := range tempElem.Topics {
						// Name
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Topics[i].Name); err != nil {
									return err
								}
							}
						}
						// Partitions
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Topics[i].Partitions); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Topics[i].Partitions); err != nil {
									return err
								}
							}
						}
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeArray
			lengthBytes := protocol.EncodeInt32(length)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.Transactions = make([]AddPartitionsToTxnRequestAddPartitionsToTxnTransaction, len(decoded))
			for i, item := range decoded {
				m.Transactions[i] = item.(AddPartitionsToTxnRequestAddPartitionsToTxnTransaction)
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
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem AddPartitionsToTxnTopic
			elemR := bytes.NewReader(data)
			// Name
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactInt32Array(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Partitions = val
				} else {
					val, err := protocol.ReadInt32Array(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Partitions = val
				}
			}
			// Read tagged fields if flexible
			if isFlexible {
				if err := elem.readTaggedFields(elemR, version); err != nil {
					return nil, 0, err
				}
			}
			consumed := len(data) - elemR.Len()
			return elem, consumed, nil
		}
		if isFlexible {
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint < 1 {
				return errors.New("invalid compact array length")
			}
			length := int32(lengthUint - 1)
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem AddPartitionsToTxnTopic
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.Partitions = val
					} else {
						val, err := protocol.ReadInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.Partitions = val
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32Array(elemW, tempElem.Partitions); err != nil {
							return err
						}
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeCompactArray
			lengthBytes := protocol.EncodeVaruint32(lengthUint)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.V3AndBelowTopics = make([]AddPartitionsToTxnTopic, len(decoded))
			for i, item := range decoded {
				m.V3AndBelowTopics[i] = item.(AddPartitionsToTxnTopic)
			}
		} else {
			length, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem AddPartitionsToTxnTopic
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.Partitions = val
					} else {
						val, err := protocol.ReadInt32Array(r)
						if err != nil {
							return err
						}
						tempElem.Partitions = val
					}
				}
				// Name
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32Array(elemW, tempElem.Partitions); err != nil {
							return err
						}
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeArray
			lengthBytes := protocol.EncodeInt32(length)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.V3AndBelowTopics = make([]AddPartitionsToTxnTopic, len(decoded))
			for i, item := range decoded {
				m.V3AndBelowTopics[i] = item.(AddPartitionsToTxnTopic)
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
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for AddPartitionsToTxnRequestAddPartitionsToTxnTransaction.
func (m *AddPartitionsToTxnRequestAddPartitionsToTxnTransaction) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AddPartitionsToTxnRequestAddPartitionsToTxnTransaction.
func (m *AddPartitionsToTxnRequestAddPartitionsToTxnTransaction) readTaggedFields(r io.Reader, version int16) error {
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
		}
	}

	return nil
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
