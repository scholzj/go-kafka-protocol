package allocateproducerids

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AllocateProducerIdsRequestApiKey        = 67
	AllocateProducerIdsRequestHeaderVersion = 1
)

// AllocateProducerIdsRequest represents a request message.
type AllocateProducerIdsRequest struct {
	// The ID of the requesting broker.
	BrokerId int32 `json:"brokerid" versions:"0-999"`
	// The epoch of the requesting broker.
	BrokerEpoch int64 `json:"brokerepoch" versions:"0-999"`
}

// Encode encodes a AllocateProducerIdsRequest to a byte slice for the given version.
func (m *AllocateProducerIdsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AllocateProducerIdsRequest from a byte slice for the given version.
func (m *AllocateProducerIdsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AllocateProducerIdsRequest to an io.Writer for the given version.
func (m *AllocateProducerIdsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// BrokerId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.BrokerId); err != nil {
			return err
		}
	}
	// BrokerEpoch
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.BrokerEpoch); err != nil {
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

// Read reads a AllocateProducerIdsRequest from an io.Reader for the given version.
func (m *AllocateProducerIdsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// BrokerId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.BrokerId = val
	}
	// BrokerEpoch
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.BrokerEpoch = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for AllocateProducerIdsRequest.
func (m *AllocateProducerIdsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AllocateProducerIdsRequest.
func (m *AllocateProducerIdsRequest) readTaggedFields(r io.Reader, version int16) error {
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
