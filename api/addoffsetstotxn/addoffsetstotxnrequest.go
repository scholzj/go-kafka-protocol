package addoffsetstotxn

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AddOffsetsToTxnRequestApiKey        = 25
	AddOffsetsToTxnRequestHeaderVersion = 1
)

// AddOffsetsToTxnRequest represents a request message.
type AddOffsetsToTxnRequest struct {
	// The transactional id corresponding to the transaction.
	TransactionalId string `json:"transactionalid" versions:"0-999"`
	// Current producer id in use by the transactional id.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// Current epoch associated with the producer id.
	ProducerEpoch int16 `json:"producerepoch" versions:"0-999"`
	// The unique group identifier.
	GroupId string `json:"groupid" versions:"0-999"`
}

// Encode encodes a AddOffsetsToTxnRequest to a byte slice for the given version.
func (m *AddOffsetsToTxnRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AddOffsetsToTxnRequest from a byte slice for the given version.
func (m *AddOffsetsToTxnRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AddOffsetsToTxnRequest to an io.Writer for the given version.
func (m *AddOffsetsToTxnRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 4 {
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
	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactString(w, m.GroupId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, m.GroupId); err != nil {
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

// Read reads a AddOffsetsToTxnRequest from an io.Reader for the given version.
func (m *AddOffsetsToTxnRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 4 {
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
	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		} else {
			val, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
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

// writeTaggedFields writes tagged fields for AddOffsetsToTxnRequest.
func (m *AddOffsetsToTxnRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AddOffsetsToTxnRequest.
func (m *AddOffsetsToTxnRequest) readTaggedFields(r io.Reader, version int16) error {
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
