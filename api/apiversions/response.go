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
	ApiKeys                []ApiVersionsResponseApiKey
	ThrottleTimeMs         int32
	SupportedFeatures      []ApiVersionsResponseSupportedFeature // tag 0
	FinalizedFeaturesEpoch int64                                 // tag 1
	FinalizedFeatures      []ApiVersionsResponseFinalizedFeature // tag 2
	ZkMigrationReady       bool                                  // tag 3
	rawTaggedFields        []protocol.TaggedField                // For unknown tags
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

func ResponseHeaderVersion(apiVersion int16) int16 {
	// ApiVersions is always 0
	return 0
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
	if err := protocol.WriteCompactArray(w, res.apiKeysEncoder, res.ApiKeys); err != nil {
		return err
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
			fmt.Println("Failed to decode tagged fields", err)
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
	apiKeys, err := protocol.ReadCompactArray(r, res.apiKeysDecoder)
	if err != nil {
		return err
	}
	res.ApiKeys = apiKeys

	if response.ApiVersion >= 1 {
		// ThrottleTime
		throttleTimeMs, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttleTimeMs

		if isResponseFlexible(response.ApiVersion) {
			// Decode tagged fields
			err = protocol.ReadTaggedFields(r, res.taggedFieldsDecoder)
			if err != nil {
				fmt.Println("Failed to decode tagged fields", err)
				return err
			}
		}
	}

	return nil
}

//func (res *ApiVersionsResponse) Decode(response protocol.Response) error {
//	bytes := response.Body.Bytes()
//	offset := 0
//
//	// ErrorCode
//	errorCode, c, err := protocol.DecodeInt16(bytes[offset:])
//	if err != nil {
//		return err
//	}
//	offset += c
//	fmt.Printf("ErrorCode: %d, offset: %d, c: %d\n", errorCode, offset, c)
//	res.ErrorCode = errorCode
//
//	// ApiKeys
//	apiKeys, c, err := protocol.DecodeCompactArray(bytes[offset:], apiKeysDecoder)
//	if err != nil {
//		return err
//	}
//	offset += c
//	res.ApiKeys = apiKeys
//
//	if response.ApiVersion >= 1 {
//		// ThrottleTime
//		throttleTimeMs, c, err := protocol.DecodeInt32(bytes[offset:])
//		if err != nil {
//			return err
//		}
//		offset += c
//		fmt.Printf("ThrottleTimeMs: %d\n", throttleTimeMs)
//		res.ThrottleTimeMs = throttleTimeMs
//
//		if isResponseFlexible(response.ApiVersion) {
//			// Decode tagged fields
//			c, err = protocol.DecodeTaggedFields(bytes[offset:], taggedFieldsDecoder, res)
//			if err != nil {
//				fmt.Println("Failed to decode tagged fields", err)
//				return err
//			}
//			offset += c
//		}
//	}
//
//	return nil
//}

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

//func apiKeysDecoder(bytes []byte) (ApiVersionsResponseApiKey, int, error) {
//	offset := 0
//	apiKeys := ApiVersionsResponseApiKey{}
//
//	// Api Key
//	apiKey, c, err := protocol.DecodeInt16(bytes[offset:])
//	if err != nil {
//		return apiKeys, offset, err
//	}
//	offset += c
//	apiKeys.ApiKey = apiKey
//
//	// Min version
//	minVersion, c, err := protocol.DecodeInt16(bytes[offset:])
//	if err != nil {
//		return apiKeys, offset, err
//	}
//	offset += c
//	apiKeys.MinVersion = minVersion
//
//	// Max version
//	maxVersion, c, err := protocol.DecodeInt16(bytes[offset:])
//	if err != nil {
//		return apiKeys, offset, err
//	}
//	offset += c
//	apiKeys.MaxVersion = maxVersion
//
//	// Tagged fields
//	rawTaggedFields, c, err := protocol.DecodeRawTaggedFields(bytes[offset:])
//	if err != nil {
//		fmt.Println("Failed to decode tagged fields", err)
//		return apiKeys, offset, err
//	}
//	offset += c
//	apiKeys.rawTaggedFields = rawTaggedFields
//
//	return apiKeys, offset, nil
//}

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
			fmt.Println("Failed to decode tagged fields", err)
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
	if err := protocol.WriteCompactArray(buf, res.supportedFeaturesEncoder, res.SupportedFeatures); err != nil {
		return taggedFields, err
	}

	taggedFields[0] = protocol.TaggedField{
		Tag:   0,
		Field: buf.Bytes(),
	}

	// Tag 1
	buf = bytes.NewBuffer(make([]byte, 0))
	if err := protocol.WriteInt64(buf, res.FinalizedFeaturesEpoch); err != nil {
		return taggedFields, err
	}

	taggedFields[1] = protocol.TaggedField{
		Tag:   1,
		Field: buf.Bytes(),
	}

	// Tag 2
	buf = bytes.NewBuffer(make([]byte, 0))
	if err := protocol.WriteCompactArray(buf, res.finalizedFeaturesEncoder, res.FinalizedFeatures); err != nil {
		return taggedFields, err
	}

	taggedFields[2] = protocol.TaggedField{
		Tag:   2,
		Field: buf.Bytes(),
	}

	// Tag 3
	buf = bytes.NewBuffer(make([]byte, 0))
	if err := protocol.WriteBool(buf, res.ZkMigrationReady); err != nil {
		return taggedFields, err
	}

	taggedFields[3] = protocol.TaggedField{
		Tag:   3,
		Field: buf.Bytes(),
	}

	// We append any raw tagged fields to the end of the array
	taggedFields = append(taggedFields, res.rawTaggedFields...)

	return taggedFields, nil
}

//func taggedFieldsDecoder(bytes []byte, r *ApiVersionsResponse, tag uint64, tagLength uint64) (int, error) {
//	offset := 0
//
//	switch tag {
//	case 0:
//		// SupportedFeatures
//		supportedFeatures, c, err := protocol.DecodeCompactArray(bytes[offset:], supportedFeaturesDecoder)
//		if err != nil {
//			return offset, err
//		}
//		offset += c
//		r.SupportedFeatures = supportedFeatures
//	case 1:
//		// FinalizedFeaturesEpoch
//		finalizedFeaturesEpoch, c, err := protocol.DecodeInt64(bytes[offset:])
//		if err != nil {
//			fmt.Println("Failed to decode tag value", err)
//			return offset, err
//		}
//		offset += c
//		r.FinalizedFeaturesEpoch = finalizedFeaturesEpoch
//	case 2:
//		// FinalizedFeatures
//		finalizedFeatures, c, err := protocol.DecodeCompactArray(bytes[offset:], finalizedFeaturesDecoder)
//		if err != nil {
//			return offset, err
//		}
//		offset += c
//		r.FinalizedFeatures = finalizedFeatures
//	case 3:
//		// ZkMigrationReady
//		zkMigrationReady, c, err := protocol.DecodeBool(bytes[offset:])
//		if err != nil {
//			fmt.Println("Failed to decode tag value", err)
//			return offset, err
//		}
//		offset += c
//		r.ZkMigrationReady = zkMigrationReady
//	}
//
//	return offset, nil
//}

func (res *ApiVersionsResponse) taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {
	rawTaggedFields := make([]protocol.TaggedField, 0)

	switch tag {
	case 0:
		// SupportedFeatures
		supportedFeatures, err := protocol.ReadCompactArray(r, res.supportedFeaturesDecoder)
		if err != nil {
			return err
		}
		res.SupportedFeatures = supportedFeatures
	case 1:
		// FinalizedFeaturesEpoch
		finalizedFeaturesEpoch, err := protocol.ReadInt64(r)
		if err != nil {
			fmt.Println("Failed to decode tag value", err)
			return err
		}
		res.FinalizedFeaturesEpoch = finalizedFeaturesEpoch
	case 2:
		// FinalizedFeatures
		finalizedFeatures, err := protocol.ReadCompactArray(r, res.finalizedFeaturesDecoder)
		if err != nil {
			return err
		}
		res.FinalizedFeatures = finalizedFeatures
	case 3:
		// ZkMigrationReady
		zkMigrationReady, err := protocol.ReadBool(r)
		if err != nil {
			fmt.Println("Failed to decode tag value", err)
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

//func (res *ApiVersionsResponse) supportedFeaturesDecoder(bytes []byte) (ApiVersionsResponseSupportedFeature, int, error) {
//	offset := 0
//	supportedFeatures := ApiVersionsResponseSupportedFeature{}
//
//	name, c, err := protocol.DecodeCompactString(bytes[offset:])
//	if err != nil {
//		fmt.Println("Failed to decode tag value", err)
//	}
//	offset += c
//	supportedFeatures.Name = name
//
//	minVersion, c, err := protocol.DecodeInt16(bytes[offset:])
//	if err != nil {
//		fmt.Println("Failed to decode tag value", err)
//	}
//	offset += c
//	supportedFeatures.MinVersion = minVersion
//
//	maxVersion, c, err := protocol.DecodeInt16(bytes[offset:])
//	if err != nil {
//		fmt.Println("Failed to decode tag value", err)
//	}
//	offset += c
//	supportedFeatures.MaxVersion = maxVersion
//
//	// Tagged fields
//	rawTaggedFields, c, err := protocol.DecodeRawTaggedFields(bytes[offset:])
//	if err != nil {
//		fmt.Println("Failed to decode tagged fields", err)
//		return supportedFeatures, offset, err
//	}
//	offset += c
//	supportedFeatures.rawTaggedFields = rawTaggedFields
//
//	return supportedFeatures, offset, nil
//}

//func finalizedFeaturesDecoder(bytes []byte) (ApiVersionsResponseFinalizedFeature, int, error) {
//	offset := 0
//	finalizedFeatures := ApiVersionsResponseFinalizedFeature{}
//
//	name, c, err := protocol.DecodeCompactString(bytes[offset:])
//	if err != nil {
//		fmt.Println("Failed to decode tag value", err)
//	}
//	offset += c
//	finalizedFeatures.Name = name
//
//	minVersionLevel, c, err := protocol.DecodeInt16(bytes[offset:])
//	if err != nil {
//		fmt.Println("Failed to decode tag value", err)
//	}
//	offset += c
//	finalizedFeatures.MinVersionLevel = minVersionLevel
//
//	maxVersionLevel, c, err := protocol.DecodeInt16(bytes[offset:])
//	if err != nil {
//		fmt.Println("Failed to decode tag value", err)
//	}
//	offset += c
//	finalizedFeatures.MaxVersionLevel = maxVersionLevel
//
//	// Tagged fields
//	rawTaggedFields, c, err := protocol.DecodeRawTaggedFields(bytes[offset:])
//	if err != nil {
//		fmt.Println("Failed to decode tagged fields", err)
//		return finalizedFeatures, offset, err
//	}
//	offset += c
//	finalizedFeatures.rawTaggedFields = rawTaggedFields
//
//	return finalizedFeatures, offset, nil
//}

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
		fmt.Println("Failed to decode tag value", err)
	}
	supportedFeatures.Name = name

	// MinVersion
	minVersion, err := protocol.ReadInt16(r)
	if err != nil {
		fmt.Println("Failed to decode tag value", err)
	}
	supportedFeatures.MinVersion = minVersion

	// MaxVersion
	maxVersion, err := protocol.ReadInt16(r)
	if err != nil {
		fmt.Println("Failed to decode tag value", err)
	}
	supportedFeatures.MaxVersion = maxVersion

	// Tagged fields
	rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
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
		fmt.Println("Failed to decode tag value", err)
	}
	finalizedFeatures.Name = name

	// MinVersionLevel
	minVersionLevel, err := protocol.ReadInt16(r)
	if err != nil {
		fmt.Println("Failed to decode tag value", err)
	}
	finalizedFeatures.MinVersionLevel = minVersionLevel

	// MaxVersionLevel
	maxVersionLevel, err := protocol.ReadInt16(r)
	if err != nil {
		fmt.Println("Failed to decode tag value", err)
	}
	finalizedFeatures.MaxVersionLevel = maxVersionLevel

	// Tagged fields
	rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
		return finalizedFeatures, err
	}
	finalizedFeatures.rawTaggedFields = rawTaggedFields

	return finalizedFeatures, nil
}

func (res *ApiVersionsResponse) PrettyPrint() {
	fmt.Printf("<- ApiVersionsResponse:\n")
	fmt.Printf("        ErrorCode: %d\n", res.ErrorCode)
	fmt.Printf("        ApiKeys:\n")
	for _, apiKey := range res.ApiKeys {
		fmt.Printf("                ApiKey: %d; MinVersion: %d; MaxVersion: %d\n", apiKey.ApiKey, apiKey.MinVersion, apiKey.MaxVersion)
	}
	fmt.Printf("        ThrottleTimeMs: %d\n", res.ThrottleTimeMs)
	fmt.Printf("        SupportedFeatures: %v\n", res.SupportedFeatures)
	fmt.Printf("        FinalizedFeaturesEpoch: %d\n", res.FinalizedFeaturesEpoch)
	fmt.Printf("        FinalizedFeatures: %v\n", res.FinalizedFeatures)
	fmt.Printf("        ZkMigrationReady: %t\n", res.ZkMigrationReady)
	fmt.Printf("\n")
}
