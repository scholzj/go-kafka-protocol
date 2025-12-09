package initproducerid

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	InitProducerIdResponseApiKey        = 22
	InitProducerIdResponseHeaderVersion = 1
)

// InitProducerIdResponse represents a response message.
type InitProducerIdResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The current producer id.
	ProducerId int64 `json:"producerid" versions:"0-999"`
	// The current epoch associated with the producer id.
	ProducerEpoch int16 `json:"producerepoch" versions:"0-999"`
	// The producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
	OngoingTxnProducerId int64 `json:"ongoingtxnproducerid" versions:"6-999"`
	// The epoch associated with the  producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing.
	OngoingTxnProducerEpoch int16 `json:"ongoingtxnproducerepoch" versions:"6-999"`
}

// Encode encodes a InitProducerIdResponse to a byte slice for the given version.
func (m *InitProducerIdResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a InitProducerIdResponse from a byte slice for the given version.
func (m *InitProducerIdResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a InitProducerIdResponse to an io.Writer for the given version.
func (m *InitProducerIdResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
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
	// OngoingTxnProducerId
	if version >= 6 && version <= 999 {
		if err := protocol.WriteInt64(w, m.OngoingTxnProducerId); err != nil {
			return err
		}
	}
	// OngoingTxnProducerEpoch
	if version >= 6 && version <= 999 {
		if err := protocol.WriteInt16(w, m.OngoingTxnProducerEpoch); err != nil {
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

// Read reads a InitProducerIdResponse from an io.Reader for the given version.
func (m *InitProducerIdResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 6 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
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
	// OngoingTxnProducerId
	if version >= 6 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.OngoingTxnProducerId = val
	}
	// OngoingTxnProducerEpoch
	if version >= 6 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.OngoingTxnProducerEpoch = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for InitProducerIdResponse.
func (m *InitProducerIdResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for InitProducerIdResponse.
func (m *InitProducerIdResponse) readTaggedFields(r io.Reader, version int16) error {
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
