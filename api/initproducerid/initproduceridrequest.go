package initproducerid

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	InitProducerIdRequestApiKey        = 22
	InitProducerIdRequestHeaderVersion = 1
)

// InitProducerIdRequest represents a request message.
type InitProducerIdRequest struct {
	// The transactional id, or null if the producer is not transactional.
	TransactionalId *string `json:"transactionalid" versions:"0-999"`
	// The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
	TransactionTimeoutMs int32 `json:"transactiontimeoutms" versions:"0-999"`
	// The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
	ProducerId int64 `json:"producerid" versions:"3-999"`
	// The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
	ProducerEpoch int16 `json:"producerepoch" versions:"3-999"`
	// True if the client wants to enable two-phase commit (2PC) protocol for transactions.
	Enable2Pc bool `json:"enable2pc" versions:"6-999"`
	// True if the client wants to keep the currently ongoing transaction instead of aborting it.
	KeepPreparedTxn bool `json:"keeppreparedtxn" versions:"6-999"`
}

// Encode encodes a InitProducerIdRequest to a byte slice for the given version.
func (m *InitProducerIdRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a InitProducerIdRequest from a byte slice for the given version.
func (m *InitProducerIdRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a InitProducerIdRequest to an io.Writer for the given version.
func (m *InitProducerIdRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// TransactionalId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.TransactionalId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.TransactionalId); err != nil {
				return err
			}
		}
	}
	// TransactionTimeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TransactionTimeoutMs); err != nil {
			return err
		}
	}
	// ProducerId
	if version >= 3 && version <= 999 {
		if err := protocol.WriteInt64(w, m.ProducerId); err != nil {
			return err
		}
	}
	// ProducerEpoch
	if version >= 3 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ProducerEpoch); err != nil {
			return err
		}
	}
	// Enable2Pc
	if version >= 6 && version <= 999 {
		if err := protocol.WriteBool(w, m.Enable2Pc); err != nil {
			return err
		}
	}
	// KeepPreparedTxn
	if version >= 6 && version <= 999 {
		if err := protocol.WriteBool(w, m.KeepPreparedTxn); err != nil {
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

// Read reads a InitProducerIdRequest from an io.Reader for the given version.
func (m *InitProducerIdRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// TransactionalId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.TransactionalId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.TransactionalId = val
		}
	}
	// TransactionTimeoutMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.TransactionTimeoutMs = val
	}
	// ProducerId
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.ProducerId = val
	}
	// ProducerEpoch
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ProducerEpoch = val
	}
	// Enable2Pc
	if version >= 6 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.Enable2Pc = val
	}
	// KeepPreparedTxn
	if version >= 6 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.KeepPreparedTxn = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for InitProducerIdRequest.
func (m *InitProducerIdRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for InitProducerIdRequest.
func (m *InitProducerIdRequest) readTaggedFields(r io.Reader, version int16) error {
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
