package listtransactions

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ListTransactionsRequestApiKey        = 66
	ListTransactionsRequestHeaderVersion = 1
)

// ListTransactionsRequest represents a request message.
type ListTransactionsRequest struct {
	// The transaction states to filter by: if empty, all transactions are returned; if non-empty, then only transactions matching one of the filtered states will be returned.
	StateFilters []string `json:"statefilters" versions:"0-999"`
	// The producerIds to filter by: if empty, all transactions will be returned; if non-empty, only transactions which match one of the filtered producerIds will be returned.
	ProducerIdFilters []int64 `json:"produceridfilters" versions:"0-999"`
	// Duration (in millis) to filter by: if < 0, all transactions will be returned; otherwise, only transactions running longer than this duration will be returned.
	DurationFilter int64 `json:"durationfilter" versions:"1-999"`
	// The transactional ID regular expression pattern to filter by: if it is empty or null, all transactions are returned; Otherwise then only the transactions matching the given regular expression will be returned.
	TransactionalIdPattern *string `json:"transactionalidpattern" versions:"2-999"`
}

// Encode encodes a ListTransactionsRequest to a byte slice for the given version.
func (m *ListTransactionsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ListTransactionsRequest from a byte slice for the given version.
func (m *ListTransactionsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ListTransactionsRequest to an io.Writer for the given version.
func (m *ListTransactionsRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// StateFilters
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactStringArray(w, m.StateFilters); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteStringArray(w, m.StateFilters); err != nil {
				return err
			}
		}
	}
	// ProducerIdFilters
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactInt64Array(w, m.ProducerIdFilters); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt64Array(w, m.ProducerIdFilters); err != nil {
				return err
			}
		}
	}
	// DurationFilter
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt64(w, m.DurationFilter); err != nil {
			return err
		}
	}
	// TransactionalIdPattern
	if version >= 2 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.TransactionalIdPattern); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.TransactionalIdPattern); err != nil {
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

// Read reads a ListTransactionsRequest from an io.Reader for the given version.
func (m *ListTransactionsRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// StateFilters
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactStringArray(r)
			if err != nil {
				return err
			}
			m.StateFilters = val
		} else {
			val, err := protocol.ReadStringArray(r)
			if err != nil {
				return err
			}
			m.StateFilters = val
		}
	}
	// ProducerIdFilters
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactInt64Array(r)
			if err != nil {
				return err
			}
			m.ProducerIdFilters = val
		} else {
			val, err := protocol.ReadInt64Array(r)
			if err != nil {
				return err
			}
			m.ProducerIdFilters = val
		}
	}
	// DurationFilter
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.DurationFilter = val
	}
	// TransactionalIdPattern
	if version >= 2 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.TransactionalIdPattern = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.TransactionalIdPattern = val
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

// writeTaggedFields writes tagged fields for ListTransactionsRequest.
func (m *ListTransactionsRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ListTransactionsRequest.
func (m *ListTransactionsRequest) readTaggedFields(r io.Reader, version int16) error {
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
