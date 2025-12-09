package updatefeatures

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	UpdateFeaturesRequestApiKey        = 57
	UpdateFeaturesRequestHeaderVersion = 1
)

// UpdateFeaturesRequest represents a request message.
type UpdateFeaturesRequest struct {
	// How long to wait in milliseconds before timing out the request.
	TimeoutMs int32 `json:"timeoutms" versions:"0-999"`
	// The list of updates to finalized features.
	FeatureUpdates []UpdateFeaturesRequestFeatureUpdateKey `json:"featureupdates" versions:"0-999"`
	// True if we should validate the request, but not perform the upgrade or downgrade.
	ValidateOnly bool `json:"validateonly" versions:"1-999"`
}

// Encode encodes a UpdateFeaturesRequest to a byte slice for the given version.
func (m *UpdateFeaturesRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a UpdateFeaturesRequest from a byte slice for the given version.
func (m *UpdateFeaturesRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a UpdateFeaturesRequest to an io.Writer for the given version.
func (m *UpdateFeaturesRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// timeoutMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.TimeoutMs); err != nil {
			return err
		}
	}
	// FeatureUpdates
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.FeatureUpdates) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.FeatureUpdates))); err != nil {
				return err
			}
		}
		for i := range m.FeatureUpdates {
			// Feature
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.FeatureUpdates[i].Feature); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.FeatureUpdates[i].Feature); err != nil {
						return err
					}
				}
			}
			// MaxVersionLevel
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.FeatureUpdates[i].MaxVersionLevel); err != nil {
					return err
				}
			}
			// AllowDowngrade
			if version >= 0 && version <= 0 {
				if err := protocol.WriteBool(w, m.FeatureUpdates[i].AllowDowngrade); err != nil {
					return err
				}
			}
			// UpgradeType
			if version >= 1 && version <= 999 {
				if err := protocol.WriteInt8(w, m.FeatureUpdates[i].UpgradeType); err != nil {
					return err
				}
			}
		}
	}
	// ValidateOnly
	if version >= 1 && version <= 999 {
		if err := protocol.WriteBool(w, m.ValidateOnly); err != nil {
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

// Read reads a UpdateFeaturesRequest from an io.Reader for the given version.
func (m *UpdateFeaturesRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// timeoutMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.TimeoutMs = val
	}
	// FeatureUpdates
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
			m.FeatureUpdates = make([]UpdateFeaturesRequestFeatureUpdateKey, length)
			for i := int32(0); i < length; i++ {
				// Feature
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.FeatureUpdates[i].Feature = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.FeatureUpdates[i].Feature = val
					}
				}
				// MaxVersionLevel
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.FeatureUpdates[i].MaxVersionLevel = val
				}
				// AllowDowngrade
				if version >= 0 && version <= 0 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					m.FeatureUpdates[i].AllowDowngrade = val
				}
				// UpgradeType
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.FeatureUpdates[i].UpgradeType = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.FeatureUpdates = make([]UpdateFeaturesRequestFeatureUpdateKey, length)
			for i := int32(0); i < length; i++ {
				// Feature
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.FeatureUpdates[i].Feature = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.FeatureUpdates[i].Feature = val
					}
				}
				// MaxVersionLevel
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.FeatureUpdates[i].MaxVersionLevel = val
				}
				// AllowDowngrade
				if version >= 0 && version <= 0 {
					val, err := protocol.ReadBool(r)
					if err != nil {
						return err
					}
					m.FeatureUpdates[i].AllowDowngrade = val
				}
				// UpgradeType
				if version >= 1 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.FeatureUpdates[i].UpgradeType = val
				}
			}
		}
	}
	// ValidateOnly
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.ValidateOnly = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// UpdateFeaturesRequestFeatureUpdateKey represents The list of updates to finalized features..
type UpdateFeaturesRequestFeatureUpdateKey struct {
	// The name of the finalized feature to be updated.
	Feature string `json:"feature" versions:"0-999"`
	// The new maximum version level for the finalized feature. A value >= 1 is valid. A value < 1, is special, and can be used to request the deletion of the finalized feature.
	MaxVersionLevel int16 `json:"maxversionlevel" versions:"0-999"`
	// DEPRECATED in version 1 (see DowngradeType). When set to true, the finalized feature version level is allowed to be downgraded/deleted. The downgrade request will fail if the new maximum version level is a value that's not lower than the existing maximum finalized version level.
	AllowDowngrade bool `json:"allowdowngrade" versions:"0"`
	// Determine which type of upgrade will be performed: 1 will perform an upgrade only (default), 2 is safe downgrades only (lossless), 3 is unsafe downgrades (lossy).
	UpgradeType int8 `json:"upgradetype" versions:"1-999"`
}

// writeTaggedFields writes tagged fields for UpdateFeaturesRequest.
func (m *UpdateFeaturesRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for UpdateFeaturesRequest.
func (m *UpdateFeaturesRequest) readTaggedFields(r io.Reader, version int16) error {
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
