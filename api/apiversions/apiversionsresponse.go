package apiversions

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ApiVersionsResponseApiKey        = 18
	ApiVersionsResponseHeaderVersion = 1
)

// ApiVersionsResponse represents a response message.
type ApiVersionsResponse struct {
	// The top-level error code.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The APIs supported by the broker.
	ApiKeys []ApiVersionsResponseApiVersion `json:"apikeys" versions:"0-999"`
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"1-999"`
	// Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
	SupportedFeatures []ApiVersionsResponseSupportedFeatureKey `json:"supportedfeatures" versions:"3-999" tag:"0"`
	// The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
	FinalizedFeaturesEpoch int64 `json:"finalizedfeaturesepoch" versions:"3-999" tag:"1"`
	// List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
	FinalizedFeatures []ApiVersionsResponseFinalizedFeatureKey `json:"finalizedfeatures" versions:"3-999" tag:"2"`
	// Set by a KRaft controller if the required configurations for ZK migration are present.
	ZkMigrationReady bool `json:"zkmigrationready" versions:"3-999" tag:"3"`
}

// Encode encodes a ApiVersionsResponse to a byte slice for the given version.
func (m *ApiVersionsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ApiVersionsResponse from a byte slice for the given version.
func (m *ApiVersionsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ApiVersionsResponse to an io.Writer for the given version.
func (m *ApiVersionsResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// ErrorCode
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// ApiKeys
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(ApiVersionsResponseApiVersion)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// ApiKey
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ApiKey); err != nil {
					return nil, err
				}
			}
			// MinVersion
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.MinVersion); err != nil {
					return nil, err
				}
			}
			// MaxVersion
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.MaxVersion); err != nil {
					return nil, err
				}
			}
			// Write tagged fields if flexible
			if isFlexible {
				if err := structItem.writeTaggedFields(elemW, version); err != nil {
					return nil, err
				}
			}
			return elemBuf.Bytes(), nil
		}
		items := make([]interface{}, len(m.ApiKeys))
		for i := range m.ApiKeys {
			items[i] = m.ApiKeys[i]
		}
		if isFlexible {
			if err := protocol.WriteCompactArray(w, items, encoder); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, items, encoder); err != nil {
				return err
			}
		}
	}
	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// SupportedFeatures
	if version >= 3 && version <= 999 {
	}
	// FinalizedFeaturesEpoch
	if version >= 3 && version <= 999 {
	}
	// FinalizedFeatures
	if version >= 3 && version <= 999 {
	}
	// ZkMigrationReady
	if version >= 3 && version <= 999 {
	}
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a ApiVersionsResponse from an io.Reader for the given version.
func (m *ApiVersionsResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 3 {
		isFlexible = true
	}

	// ErrorCode
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// ApiKeys
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem ApiVersionsResponseApiVersion
			elemR := bytes.NewReader(data)
			// ApiKey
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ApiKey = val
			}
			// MinVersion
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.MinVersion = val
			}
			// MaxVersion
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.MaxVersion = val
			}
			// Read tagged fields if flexible
			if isFlexible {
				if err := elem.readTaggedFields(elemR, version); err != nil {
					return nil, 0, err
				}
			}
			consumed := len(data) - elemR.Len()
			return elem, consumed, nil
		}
		if isFlexible {
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint < 1 {
				return errors.New("invalid compact array length")
			}
			length := int32(lengthUint - 1)
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem ApiVersionsResponseApiVersion
				// ApiKey
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ApiKey = val
				}
				// MinVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.MinVersion = val
				}
				// MaxVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.MaxVersion = val
				}
				// ApiKey
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ApiKey); err != nil {
						return err
					}
				}
				// MinVersion
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.MinVersion); err != nil {
						return err
					}
				}
				// MaxVersion
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.MaxVersion); err != nil {
						return err
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeCompactArray
			lengthBytes := protocol.EncodeVaruint32(lengthUint)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.ApiKeys = make([]ApiVersionsResponseApiVersion, len(decoded))
			for i, item := range decoded {
				m.ApiKeys[i] = item.(ApiVersionsResponseApiVersion)
			}
		} else {
			length, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem ApiVersionsResponseApiVersion
				// ApiKey
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ApiKey = val
				}
				// MinVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.MinVersion = val
				}
				// MaxVersion
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.MaxVersion = val
				}
				// ApiKey
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ApiKey); err != nil {
						return err
					}
				}
				// MinVersion
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.MinVersion); err != nil {
						return err
					}
				}
				// MaxVersion
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.MaxVersion); err != nil {
						return err
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeArray
			lengthBytes := protocol.EncodeInt32(length)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.ApiKeys = make([]ApiVersionsResponseApiVersion, len(decoded))
			for i, item := range decoded {
				m.ApiKeys[i] = item.(ApiVersionsResponseApiVersion)
			}
		}
	}
	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// SupportedFeatures
	if version >= 3 && version <= 999 {
	}
	// FinalizedFeaturesEpoch
	if version >= 3 && version <= 999 {
	}
	// FinalizedFeatures
	if version >= 3 && version <= 999 {
	}
	// ZkMigrationReady
	if version >= 3 && version <= 999 {
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// ApiVersionsResponseApiVersion represents The APIs supported by the broker..
type ApiVersionsResponseApiVersion struct {
	// The API index.
	ApiKey int16 `json:"apikey" versions:"0-999"`
	// The minimum supported version, inclusive.
	MinVersion int16 `json:"minversion" versions:"0-999"`
	// The maximum supported version, inclusive.
	MaxVersion int16 `json:"maxversion" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ApiVersionsResponseApiVersion.
func (m *ApiVersionsResponseApiVersion) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ApiVersionsResponseApiVersion.
func (m *ApiVersionsResponseApiVersion) readTaggedFields(r io.Reader, version int16) error {
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
		}
	}

	return nil
}

// ApiVersionsResponseSupportedFeatureKey represents Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted..
type ApiVersionsResponseSupportedFeatureKey struct {
	// The name of the feature.
	Name string `json:"name" versions:"3-999"`
	// The minimum supported version for the feature.
	MinVersion int16 `json:"minversion" versions:"3-999"`
	// The maximum supported version for the feature.
	MaxVersion int16 `json:"maxversion" versions:"3-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ApiVersionsResponseSupportedFeatureKey.
func (m *ApiVersionsResponseSupportedFeatureKey) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ApiVersionsResponseSupportedFeatureKey.
func (m *ApiVersionsResponseSupportedFeatureKey) readTaggedFields(r io.Reader, version int16) error {
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
		}
	}

	return nil
}

// ApiVersionsResponseFinalizedFeatureKey represents List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0..
type ApiVersionsResponseFinalizedFeatureKey struct {
	// The name of the feature.
	Name string `json:"name" versions:"3-999"`
	// The cluster-wide finalized max version level for the feature.
	MaxVersionLevel int16 `json:"maxversionlevel" versions:"3-999"`
	// The cluster-wide finalized min version level for the feature.
	MinVersionLevel int16 `json:"minversionlevel" versions:"3-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for ApiVersionsResponseFinalizedFeatureKey.
func (m *ApiVersionsResponseFinalizedFeatureKey) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ApiVersionsResponseFinalizedFeatureKey.
func (m *ApiVersionsResponseFinalizedFeatureKey) readTaggedFields(r io.Reader, version int16) error {
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
		}
	}

	return nil
}

// writeTaggedFields writes tagged fields for ApiVersionsResponse.
func (m *ApiVersionsResponse) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	isFlexible := version >= 3

	// SupportedFeatures (tag 0)
	if version >= 3 {
		if m.SupportedFeatures != nil && len(m.SupportedFeatures) > 0 {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(0)); err != nil {
				return err
			}
			// Array in tagged field
			length := uint32(len(m.SupportedFeatures) + 1)
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, length); err != nil {
				return err
			}
			for i := range m.SupportedFeatures {
				// Name
				if version >= 3 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(w, m.SupportedFeatures[i].Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(w, m.SupportedFeatures[i].Name); err != nil {
							return err
						}
					}
				}
				// MinVersion
				if version >= 3 && version <= 999 {
					if err := protocol.WriteInt16(w, m.SupportedFeatures[i].MinVersion); err != nil {
						return err
					}
				}
				// MaxVersion
				if version >= 3 && version <= 999 {
					if err := protocol.WriteInt16(w, m.SupportedFeatures[i].MaxVersion); err != nil {
						return err
					}
				}
			}
			taggedFieldsCount++
		}
	}

	// FinalizedFeaturesEpoch (tag 1)
	if version >= 3 {
		if m.FinalizedFeaturesEpoch != -1 {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(1)); err != nil {
				return err
			}
			if err := protocol.WriteInt64(&taggedFieldsBuf, m.FinalizedFeaturesEpoch); err != nil {
				return err
			}
			taggedFieldsCount++
		}
	}

	// FinalizedFeatures (tag 2)
	if version >= 3 {
		if m.FinalizedFeatures != nil && len(m.FinalizedFeatures) > 0 {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(2)); err != nil {
				return err
			}
			// Array in tagged field
			length := uint32(len(m.FinalizedFeatures) + 1)
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, length); err != nil {
				return err
			}
			for i := range m.FinalizedFeatures {
				// Name
				if version >= 3 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(w, m.FinalizedFeatures[i].Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(w, m.FinalizedFeatures[i].Name); err != nil {
							return err
						}
					}
				}
				// MaxVersionLevel
				if version >= 3 && version <= 999 {
					if err := protocol.WriteInt16(w, m.FinalizedFeatures[i].MaxVersionLevel); err != nil {
						return err
					}
				}
				// MinVersionLevel
				if version >= 3 && version <= 999 {
					if err := protocol.WriteInt16(w, m.FinalizedFeatures[i].MinVersionLevel); err != nil {
						return err
					}
				}
			}
			taggedFieldsCount++
		}
	}

	// ZkMigrationReady (tag 3)
	if version >= 3 {
		if m.ZkMigrationReady {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(3)); err != nil {
				return err
			}
			if err := protocol.WriteBool(&taggedFieldsBuf, m.ZkMigrationReady); err != nil {
				return err
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

// readTaggedFields reads tagged fields for ApiVersionsResponse.
func (m *ApiVersionsResponse) readTaggedFields(r io.Reader, version int16) error {
	isFlexible := version >= 3

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
		case 0: // SupportedFeatures
			if version >= 3 {
				// Array in tagged field
				length, err := protocol.ReadVaruint32(r)
				if err != nil {
					return err
				}
				if length == 0 {
					m.SupportedFeatures = nil
				} else {
					if length < 1 {
						return errors.New("invalid compact array length")
					}
					m.SupportedFeatures = make([]ApiVersionsResponseSupportedFeatureKey, length-1)
					for i := uint32(0); i < length-1; i++ {
						// Name
						if version >= 3 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(r)
								if err != nil {
									return err
								}
								m.SupportedFeatures[i].Name = val
							} else {
								val, err := protocol.ReadString(r)
								if err != nil {
									return err
								}
								m.SupportedFeatures[i].Name = val
							}
						}
						// MinVersion
						if version >= 3 && version <= 999 {
							val, err := protocol.ReadInt16(r)
							if err != nil {
								return err
							}
							m.SupportedFeatures[i].MinVersion = val
						}
						// MaxVersion
						if version >= 3 && version <= 999 {
							val, err := protocol.ReadInt16(r)
							if err != nil {
								return err
							}
							m.SupportedFeatures[i].MaxVersion = val
						}
					}
				}
			}
		case 1: // FinalizedFeaturesEpoch
			if version >= 3 {
				val, err := protocol.ReadInt64(r)
				if err != nil {
					return err
				}
				m.FinalizedFeaturesEpoch = val
			}
		case 2: // FinalizedFeatures
			if version >= 3 {
				// Array in tagged field
				length, err := protocol.ReadVaruint32(r)
				if err != nil {
					return err
				}
				if length == 0 {
					m.FinalizedFeatures = nil
				} else {
					if length < 1 {
						return errors.New("invalid compact array length")
					}
					m.FinalizedFeatures = make([]ApiVersionsResponseFinalizedFeatureKey, length-1)
					for i := uint32(0); i < length-1; i++ {
						// Name
						if version >= 3 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(r)
								if err != nil {
									return err
								}
								m.FinalizedFeatures[i].Name = val
							} else {
								val, err := protocol.ReadString(r)
								if err != nil {
									return err
								}
								m.FinalizedFeatures[i].Name = val
							}
						}
						// MaxVersionLevel
						if version >= 3 && version <= 999 {
							val, err := protocol.ReadInt16(r)
							if err != nil {
								return err
							}
							m.FinalizedFeatures[i].MaxVersionLevel = val
						}
						// MinVersionLevel
						if version >= 3 && version <= 999 {
							val, err := protocol.ReadInt16(r)
							if err != nil {
								return err
							}
							m.FinalizedFeatures[i].MinVersionLevel = val
						}
					}
				}
			}
		case 3: // ZkMigrationReady
			if version >= 3 {
				val, err := protocol.ReadBool(r)
				if err != nil {
					return err
				}
				m.ZkMigrationReady = val
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
