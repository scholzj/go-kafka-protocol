package describecluster

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeClusterRequestApiKey        = 60
	DescribeClusterRequestHeaderVersion = 1
)

// DescribeClusterRequest represents a request message.
type DescribeClusterRequest struct {
	// Whether to include cluster authorized operations.
	IncludeClusterAuthorizedOperations bool `json:"includeclusterauthorizedoperations" versions:"0-999"`
	// The endpoint type to describe. 1=brokers, 2=controllers.
	EndpointType int8 `json:"endpointtype" versions:"1-999"`
	// Whether to include fenced brokers when listing brokers.
	IncludeFencedBrokers bool `json:"includefencedbrokers" versions:"2-999"`
}

// Encode encodes a DescribeClusterRequest to a byte slice for the given version.
func (m *DescribeClusterRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeClusterRequest from a byte slice for the given version.
func (m *DescribeClusterRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeClusterRequest to an io.Writer for the given version.
func (m *DescribeClusterRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// IncludeClusterAuthorizedOperations
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.IncludeClusterAuthorizedOperations); err != nil {
			return err
		}
	}
	// EndpointType
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt8(w, m.EndpointType); err != nil {
			return err
		}
	}
	// IncludeFencedBrokers
	if version >= 2 && version <= 999 {
		if err := protocol.WriteBool(w, m.IncludeFencedBrokers); err != nil {
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

// Read reads a DescribeClusterRequest from an io.Reader for the given version.
func (m *DescribeClusterRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// IncludeClusterAuthorizedOperations
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IncludeClusterAuthorizedOperations = val
	}
	// EndpointType
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		m.EndpointType = val
	}
	// IncludeFencedBrokers
	if version >= 2 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IncludeFencedBrokers = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for DescribeClusterRequest.
func (m *DescribeClusterRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeClusterRequest.
func (m *DescribeClusterRequest) readTaggedFields(r io.Reader, version int16) error {
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
