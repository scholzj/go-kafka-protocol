package removeraftvoter

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	RemoveRaftVoterRequestApiKey        = 81
	RemoveRaftVoterRequestHeaderVersion = 1
)

// RemoveRaftVoterRequest represents a request message.
type RemoveRaftVoterRequest struct {
	// The cluster id of the request.
	ClusterId *string `json:"clusterid" versions:"0-999"`
	// The replica id of the voter getting removed from the topic partition.
	VoterId int32 `json:"voterid" versions:"0-999"`
	// The directory id of the voter getting removed from the topic partition.
	VoterDirectoryId uuid.UUID `json:"voterdirectoryid" versions:"0-999"`
}

// Encode encodes a RemoveRaftVoterRequest to a byte slice for the given version.
func (m *RemoveRaftVoterRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a RemoveRaftVoterRequest from a byte slice for the given version.
func (m *RemoveRaftVoterRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a RemoveRaftVoterRequest to an io.Writer for the given version.
func (m *RemoveRaftVoterRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ClusterId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ClusterId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ClusterId); err != nil {
				return err
			}
		}
	}
	// VoterId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.VoterId); err != nil {
			return err
		}
	}
	// VoterDirectoryId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteUUID(w, m.VoterDirectoryId); err != nil {
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

// Read reads a RemoveRaftVoterRequest from an io.Reader for the given version.
func (m *RemoveRaftVoterRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ClusterId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		}
	}
	// VoterId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.VoterId = val
	}
	// VoterDirectoryId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadUUID(r)
		if err != nil {
			return err
		}
		m.VoterDirectoryId = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for RemoveRaftVoterRequest.
func (m *RemoveRaftVoterRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for RemoveRaftVoterRequest.
func (m *RemoveRaftVoterRequest) readTaggedFields(r io.Reader, version int16) error {
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
