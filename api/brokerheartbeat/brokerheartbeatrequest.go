package brokerheartbeat

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	BrokerHeartbeatRequestApiKey        = 63
	BrokerHeartbeatRequestHeaderVersion = 1
)

// BrokerHeartbeatRequest represents a request message.
type BrokerHeartbeatRequest struct {
	// The broker ID.
	BrokerId int32 `json:"brokerid" versions:"0-999"`
	// The broker epoch.
	BrokerEpoch int64 `json:"brokerepoch" versions:"0-999"`
	// The highest metadata offset which the broker has reached.
	CurrentMetadataOffset int64 `json:"currentmetadataoffset" versions:"0-999"`
	// True if the broker wants to be fenced, false otherwise.
	WantFence bool `json:"wantfence" versions:"0-999"`
	// True if the broker wants to be shut down, false otherwise.
	WantShutDown bool `json:"wantshutdown" versions:"0-999"`
	// Log directories that failed and went offline.
	OfflineLogDirs []uuid.UUID `json:"offlinelogdirs" versions:"1-999" tag:"0"`
}

// Encode encodes a BrokerHeartbeatRequest to a byte slice for the given version.
func (m *BrokerHeartbeatRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a BrokerHeartbeatRequest from a byte slice for the given version.
func (m *BrokerHeartbeatRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a BrokerHeartbeatRequest to an io.Writer for the given version.
func (m *BrokerHeartbeatRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
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
	// CurrentMetadataOffset
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.CurrentMetadataOffset); err != nil {
			return err
		}
	}
	// WantFence
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.WantFence); err != nil {
			return err
		}
	}
	// WantShutDown
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.WantShutDown); err != nil {
			return err
		}
	}
	// OfflineLogDirs
	if version >= 1 && version <= 999 {
	}
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a BrokerHeartbeatRequest from an io.Reader for the given version.
func (m *BrokerHeartbeatRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
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
	// CurrentMetadataOffset
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.CurrentMetadataOffset = val
	}
	// WantFence
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.WantFence = val
	}
	// WantShutDown
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.WantShutDown = val
	}
	// OfflineLogDirs
	if version >= 1 && version <= 999 {
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// writeTaggedFields writes tagged fields for BrokerHeartbeatRequest.
func (m *BrokerHeartbeatRequest) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	isFlexible := version >= 0

	// OfflineLogDirs (tag 0)
	if version >= 1 {
		if m.OfflineLogDirs != nil && len(m.OfflineLogDirs) > 0 {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(0)); err != nil {
				return err
			}
			// Array in tagged field
			length := uint32(len(m.OfflineLogDirs) + 1)
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, length); err != nil {
				return err
			}
			for i := range m.OfflineLogDirs {
				if err := protocol.WriteUUID(&taggedFieldsBuf, m.OfflineLogDirs[i]); err != nil {
					return err
				}
			}
			taggedFieldsCount++
		}
	}

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

// readTaggedFields reads tagged fields for BrokerHeartbeatRequest.
func (m *BrokerHeartbeatRequest) readTaggedFields(r io.Reader, version int16) error {
	isFlexible := version >= 0

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
		case 0: // OfflineLogDirs
			if version >= 1 {
				// Array in tagged field
				length, err := protocol.ReadVaruint32(r)
				if err != nil {
					return err
				}
				if length == 0 {
					m.OfflineLogDirs = nil
				} else {
					if length < 1 {
						return errors.New("invalid compact array length")
					}
					m.OfflineLogDirs = make([]uuid.UUID, length-1)
					for i := uint32(0); i < length-1; i++ {
						val, err := protocol.ReadUUID(r)
						if err != nil {
							return err
						}
						m.OfflineLogDirs[i] = val
					}
				}
			}
		default:
			// Unknown tag, skip it
			// Read and discard the field data
			// For now, we'll need to know the type to skip properly
			// This is a simplified implementation
		}
	}

	return nil
}
