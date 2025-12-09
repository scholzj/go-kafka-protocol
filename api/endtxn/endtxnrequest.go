package endtxn

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	EndTxnRequestApiKey        = 26
	EndTxnRequestHeaderVersion = 1
)

// EndTxnRequest represents a request message.
type EndTxnRequest struct {
	// The ID of the transaction to end.
	TransactionalId string `json:"transactionalid" versions:"0-999"`
	// The producer ID.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// The current epoch associated with the producer.
	ProducerEpoch int16 `json:"producerepoch" versions:"0-999"`
	// True if the transaction was committed, false if it was aborted.
	Committed bool `json:"committed" versions:"0-999"`
}

// Encode encodes a EndTxnRequest to a byte slice for the given version.
func (m *EndTxnRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a EndTxnRequest from a byte slice for the given version.
func (m *EndTxnRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a EndTxnRequest to an io.Writer for the given version.
func (m *EndTxnRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// TransactionalId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.TransactionalId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.TransactionalId); err != nil {
				return err
			}
		}
	}
	// ProducerId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.ProducerId); err != nil {
			return err
		}
	}
	// ProducerEpoch
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ProducerEpoch); err != nil {
			return err
		}
	}
	// Committed
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.Committed); err != nil {
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

// Read reads a EndTxnRequest from an io.Reader for the given version.
func (m *EndTxnRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 5 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// TransactionalId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.TransactionalId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.TransactionalId = val
		}
	}
	// ProducerId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.ProducerId = val
	}
	// ProducerEpoch
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ProducerEpoch = val
	}
	// Committed
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.Committed = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for EndTxnRequest.
func (m *EndTxnRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for EndTxnRequest.
func (m *EndTxnRequest) readTaggedFields(r io.Reader, version int16) error {
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
