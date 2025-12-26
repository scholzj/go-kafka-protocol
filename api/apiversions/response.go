package apiversions

import (
	"bytes"
	"fmt"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

type ApiVersionsResponse struct {
	ApiVersion             int16
	ErrorCode              int16
	ApiKeys                *[]ApiVersionsResponseApiKey
	ThrottleTimeMs         int32
	SupportedFeatures      *[]ApiVersionsResponseSupportedFeature // tag 0
	FinalizedFeaturesEpoch int64                                  // tag 1
	FinalizedFeatures      *[]ApiVersionsResponseFinalizedFeature // tag 2
	ZkMigrationReady       bool                                   // tag 3
	rawTaggedFields        []protocol.TaggedField                 // For unknown tags
}

type ApiVersionsResponseApiKey struct {
	ApiKey          int16
	MinVersion      int16
	MaxVersion      int16
	rawTaggedFields []protocol.TaggedField
}

type ApiVersionsResponseSupportedFeature struct {
	Name            string
	MinVersion      int16
	MaxVersion      int16
	rawTaggedFields []protocol.TaggedField
}

type ApiVersionsResponseFinalizedFeature struct {
	Name            string
	MinVersionLevel int16
	MaxVersionLevel int16
	rawTaggedFields []protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (res *ApiVersionsResponse) Write(w io.Writer) error {
	// ErrorCode
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ApiKeys
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.apiKeysEncoder, res.ApiKeys); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.apiKeysEncoder, *res.ApiKeys); err != nil {
			return err
		}
	}

	// ThrottleTime
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		taggedFields, err := res.taggedFieldsEncoder()
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (res *ApiVersionsResponse) Read(response protocol.Response) error {
	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ErrorCode
	errorCode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorCode

	// ApiKeys
	if isResponseFlexible(res.ApiVersion) {
		apiKeys, err := protocol.ReadNullableCompactArray(r, res.apiKeysDecoder)
		if err != nil {
			return err
		}
		res.ApiKeys = apiKeys
	} else {
		apiKeys, err := protocol.ReadArray(r, res.apiKeysDecoder)
		if err != nil {
			return err
		}
		res.ApiKeys = &apiKeys
	}

	if response.ApiVersion >= 1 {
		// ThrottleTime
		throttleTimeMs, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttleTimeMs
	}

	if isResponseFlexible(response.ApiVersion) {
		// Decode tagged fields
		err = protocol.ReadTaggedFields(r, res.taggedFieldsDecoder)
		if err != nil {
			return err
		}
	}

	return nil
}

func (res *ApiVersionsResponse) apiKeysEncoder(w io.Writer, value ApiVersionsResponseApiKey) error {
	// Api Key
	if err := protocol.WriteInt16(w, value.ApiKey); err != nil {
		return err
	}

	// Min version
	if err := protocol.WriteInt16(w, value.MinVersion); err != nil {
		return err
	}

	// Max version
	if err := protocol.WriteInt16(w, value.MaxVersion); err != nil {
		return err
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteRawTaggedFields(w, value.rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *ApiVersionsResponse) apiKeysDecoder(r io.Reader) (ApiVersionsResponseApiKey, error) {
	apiKeys := ApiVersionsResponseApiKey{}

	// Api Key
	apiKey, err := protocol.ReadInt16(r)
	if err != nil {
		return apiKeys, err
	}
	apiKeys.ApiKey = apiKey

	// Min version
	minVersion, err := protocol.ReadInt16(r)
	if err != nil {
		return apiKeys, err
	}
	apiKeys.MinVersion = minVersion

	// Max version
	maxVersion, err := protocol.ReadInt16(r)
	if err != nil {
		return apiKeys, err
	}
	apiKeys.MaxVersion = maxVersion

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return apiKeys, err
		}
		apiKeys.rawTaggedFields = rawTaggedFields
	}

	return apiKeys, nil
}

// TODO: Do we care about the version here when decoding the tags? Or do we just decode them opportunistically? In a API version where the tag is not used, it should not be set either. But if it is, it should be still fine given it is "undefined"?
func (res *ApiVersionsResponse) taggedFieldsEncoder() ([]protocol.TaggedField, error) {
	taggedFields := make([]protocol.TaggedField, 0, 4+len(res.rawTaggedFields))

	// Tag 0
	buf := bytes.NewBuffer(make([]byte, 0))
	if err := protocol.WriteNullableCompactArray(buf, res.supportedFeaturesEncoder, res.SupportedFeatures); err != nil {
		return taggedFields, err
	}

	taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})

	// Tag 1
	buf = bytes.NewBuffer(make([]byte, 0))
	if err := protocol.WriteInt64(buf, res.FinalizedFeaturesEpoch); err != nil {
		return taggedFields, err
	}

	taggedFields = append(taggedFields, protocol.TaggedField{Tag: 1, Field: buf.Bytes()})

	// Tag 2
	buf = bytes.NewBuffer(make([]byte, 0))
	if err := protocol.WriteNullableCompactArray(buf, res.finalizedFeaturesEncoder, res.FinalizedFeatures); err != nil {
		return taggedFields, err
	}

	taggedFields = append(taggedFields, protocol.TaggedField{Tag: 2, Field: buf.Bytes()})

	// Tag 3
	buf = bytes.NewBuffer(make([]byte, 0))
	if err := protocol.WriteBool(buf, res.ZkMigrationReady); err != nil {
		return taggedFields, err
	}

	taggedFields = append(taggedFields, protocol.TaggedField{Tag: 3, Field: buf.Bytes()})

	// We append any raw tagged fields to the end of the array
	taggedFields = append(taggedFields, res.rawTaggedFields...)

	return taggedFields, nil
}

func (res *ApiVersionsResponse) taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {
	rawTaggedFields := make([]protocol.TaggedField, 0)

	switch tag {
	case 0:
		// SupportedFeatures
		supportedFeatures, err := protocol.ReadNullableCompactArray(r, res.supportedFeaturesDecoder)
		if err != nil {
			return err
		}
		res.SupportedFeatures = supportedFeatures
	case 1:
		// FinalizedFeaturesEpoch
		finalizedFeaturesEpoch, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		res.FinalizedFeaturesEpoch = finalizedFeaturesEpoch
	case 2:
		// FinalizedFeatures
		finalizedFeatures, err := protocol.ReadNullableCompactArray(r, res.finalizedFeaturesDecoder)
		if err != nil {
			return err
		}
		res.FinalizedFeatures = finalizedFeatures
	case 3:
		// ZkMigrationReady
		zkMigrationReady, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		res.ZkMigrationReady = zkMigrationReady
	default:
		// Decode as raw tags
		taggedField, err := protocol.ReadRawTaggedField(r)
		if err != nil {
			return err
		}
		rawTaggedFields = append(rawTaggedFields, taggedField)
	}

	// Set the raw tagged fields
	res.rawTaggedFields = rawTaggedFields

	return nil
}

func (res *ApiVersionsResponse) supportedFeaturesEncoder(w io.Writer, value ApiVersionsResponseSupportedFeature) error {
	// Name
	if err := protocol.WriteCompactString(w, value.Name); err != nil {
		return err
	}

	// MinVersion
	if err := protocol.WriteInt16(w, value.MinVersion); err != nil {
		return err
	}

	// MaxVersion
	if err := protocol.WriteInt16(w, value.MaxVersion); err != nil {
		return err
	}

	// Tagged fields
	if err := protocol.WriteRawTaggedFields(w, value.rawTaggedFields); err != nil {
		return err
	}

	return nil
}

func (res *ApiVersionsResponse) supportedFeaturesDecoder(r io.Reader) (ApiVersionsResponseSupportedFeature, error) {
	supportedFeatures := ApiVersionsResponseSupportedFeature{}

	// Name
	name, err := protocol.ReadCompactString(r)
	if err != nil {
		return supportedFeatures, err
	}
	supportedFeatures.Name = name

	// MinVersion
	minVersion, err := protocol.ReadInt16(r)
	if err != nil {
		return supportedFeatures, err
	}
	supportedFeatures.MinVersion = minVersion

	// MaxVersion
	maxVersion, err := protocol.ReadInt16(r)
	if err != nil {
		return supportedFeatures, err
	}
	supportedFeatures.MaxVersion = maxVersion

	// Tagged fields
	rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
	if err != nil {
		return supportedFeatures, err
	}
	supportedFeatures.rawTaggedFields = rawTaggedFields

	return supportedFeatures, nil
}

func (res *ApiVersionsResponse) finalizedFeaturesEncoder(w io.Writer, value ApiVersionsResponseFinalizedFeature) error {
	// Name
	if err := protocol.WriteCompactString(w, value.Name); err != nil {
		return err
	}

	// MinVersionLevel
	if err := protocol.WriteInt16(w, value.MinVersionLevel); err != nil {
		return err
	}

	// MaxVersionLevel
	if err := protocol.WriteInt16(w, value.MaxVersionLevel); err != nil {
		return err
	}

	// Tagged fields
	if err := protocol.WriteRawTaggedFields(w, value.rawTaggedFields); err != nil {
		return err
	}

	return nil
}

func (res *ApiVersionsResponse) finalizedFeaturesDecoder(r io.Reader) (ApiVersionsResponseFinalizedFeature, error) {
	finalizedFeatures := ApiVersionsResponseFinalizedFeature{}

	// Name
	name, err := protocol.ReadCompactString(r)
	if err != nil {
		return finalizedFeatures, err
	}
	finalizedFeatures.Name = name

	// MinVersionLevel
	minVersionLevel, err := protocol.ReadInt16(r)
	if err != nil {
		return finalizedFeatures, err
	}
	finalizedFeatures.MinVersionLevel = minVersionLevel

	// MaxVersionLevel
	maxVersionLevel, err := protocol.ReadInt16(r)
	if err != nil {
		return finalizedFeatures, err
	}
	finalizedFeatures.MaxVersionLevel = maxVersionLevel

	// Tagged fields
	rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
	if err != nil {
		return finalizedFeatures, err
	}
	finalizedFeatures.rawTaggedFields = rawTaggedFields

	return finalizedFeatures, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ApiVersionsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "<- ApiVersionsResponse:\n")
	fmt.Fprintf(w, "        ErrorCode: %d\n", res.ErrorCode)
	if res.ApiKeys != nil {
		fmt.Fprintf(w, "        ApiKeys:\n")
		for _, apiKey := range *res.ApiKeys {
			fmt.Fprintf(w, "                ApiKey: %d; MinVersion: %d; MaxVersion: %d\n", apiKey.ApiKey, apiKey.MinVersion, apiKey.MaxVersion)
		}
	} else {
		fmt.Fprintf(w, "        ApiKeys: nil\n")
	}
	fmt.Fprintf(w, "        ThrottleTimeMs: %d\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        SupportedFeatures: %v\n", *res.SupportedFeatures)
	fmt.Fprintf(w, "        FinalizedFeaturesEpoch: %d\n", res.FinalizedFeaturesEpoch)
	fmt.Fprintf(w, "        FinalizedFeatures: %v\n", *res.FinalizedFeatures)
	fmt.Fprintf(w, "        ZkMigrationReady: %t\n", res.ZkMigrationReady)

	return w.String()
}
