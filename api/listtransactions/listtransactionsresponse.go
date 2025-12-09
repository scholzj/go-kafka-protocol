package listtransactions

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ListTransactionsResponseApiKey        = 66
	ListTransactionsResponseHeaderVersion = 1
)

// ListTransactionsResponse represents a response message.
type ListTransactionsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// Set of state filters provided in the request which were unknown to the transaction coordinator.
	UnknownStateFilters []string `json:"unknownstatefilters" versions:"0-999"`
	// The current state of the transaction for the transactional id.
	TransactionStates []ListTransactionsResponseTransactionState `json:"transactionstates" versions:"0-999"`
}

// Encode encodes a ListTransactionsResponse to a byte slice for the given version.
func (m *ListTransactionsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ListTransactionsResponse from a byte slice for the given version.
func (m *ListTransactionsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ListTransactionsResponse to an io.Writer for the given version.
func (m *ListTransactionsResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
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
	// ErrorCode
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// UnknownStateFilters
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.UnknownStateFilters) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.UnknownStateFilters))); err != nil {
				return err
			}
		}
		for i := range m.UnknownStateFilters {
			if isFlexible {
				if err := protocol.WriteCompactString(w, m.UnknownStateFilters[i]); err != nil {
					return err
				}
			} else {
				if err := protocol.WriteString(w, m.UnknownStateFilters[i]); err != nil {
					return err
				}
			}
			_ = i
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
			// ProducerId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(w, m.TransactionStates[i].ProducerId); err != nil {
					return err
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

// Read reads a ListTransactionsResponse from an io.Reader for the given version.
func (m *ListTransactionsResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
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
	// ErrorCode
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// UnknownStateFilters
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
			m.UnknownStateFilters = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.UnknownStateFilters[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.UnknownStateFilters[i] = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.UnknownStateFilters = make([]string, length)
			for i := int32(0); i < length; i++ {
				if isFlexible {
					val, err := protocol.ReadCompactString(r)
					if err != nil {
						return err
					}
					m.UnknownStateFilters[i] = val
				} else {
					val, err := protocol.ReadString(r)
					if err != nil {
						return err
					}
					m.UnknownStateFilters[i] = val
				}
			}
		}
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
			m.TransactionStates = make([]ListTransactionsResponseTransactionState, length)
			for i := int32(0); i < length; i++ {
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
				// ProducerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].ProducerId = val
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
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.TransactionStates = make([]ListTransactionsResponseTransactionState, length)
			for i := int32(0); i < length; i++ {
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
				// ProducerId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.TransactionStates[i].ProducerId = val
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

// ListTransactionsResponseTransactionState represents The current state of the transaction for the transactional id..
type ListTransactionsResponseTransactionState struct {
	// The transactional id.
	TransactionalId string `json:"transactionalid" versions:"0-999"`
	// The producer id.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// The current transaction state of the producer.
	TransactionState string `json:"transactionstate" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for ListTransactionsResponse.
func (m *ListTransactionsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ListTransactionsResponse.
func (m *ListTransactionsResponse) readTaggedFields(r io.Reader, version int16) error {
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
