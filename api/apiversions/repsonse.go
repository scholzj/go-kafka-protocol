package apiversions

import (
	"fmt"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

type ApiVersionsResponse struct {
	ErrorCode              int16
	ApiKeys                []ApiVersionsResponseApiKeys
	ThrottleTimeMs         int32
	SupportedFeatures      []ApiVersionsResponseSupportedFeatures // tag 0
	FinalizedFeaturesEpoch int64                                  // tag 1
	FinalizedFeatures      []ApiVersionsResponseFinalizedFeatures // tag 2
	ZkMigrationReady       bool                                   // tag 3
}

type ApiVersionsResponseApiKeys struct {
	ApiKey          int16
	MinVersion      int16
	MaxVersion      int16
	rawTaggedFields []protocol.TaggedField
}

type ApiVersionsResponseSupportedFeatures struct {
	Name            string
	MinVersion      int16
	MaxVersion      int16
	rawTaggedFields []protocol.TaggedField
}

type ApiVersionsResponseFinalizedFeatures struct {
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

// TODO: pass version and bytes only
func (r *ApiVersionsResponse) Decode(response protocol.Response) error {
	bytes := response.Body
	offset := 0

	// ErrorCode
	errorCode, c, err := protocol.DecodeInt16(bytes[offset:])
	if err != nil {
		return err
	}
	offset += c
	fmt.Printf("ErrorCode: %d, offset: %d, c: %d\n", errorCode, offset, c)
	r.ErrorCode = errorCode

	// ApiKeys
	apiKeys, c, err := protocol.DecodeCompactArray(bytes[offset:], apiKeysDecoder)
	if err != nil {
		return err
	}
	offset += c
	r.ApiKeys = apiKeys

	// ThrottleTime
	throttleTimeMs, c, err := protocol.DecodeInt32(bytes[offset:])
	if err != nil {
		return err
	}
	offset += c
	fmt.Printf("ThrottleTimeMs: %d\n", throttleTimeMs)
	r.ThrottleTimeMs = throttleTimeMs

	// Decode tagged fields
	c, err = protocol.DecodeTaggedFields(bytes[offset:], taggedFieldsDecoder, r)
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
		return err
	}
	offset += c

	return nil
}

func apiKeysDecoder(bytes []byte) (ApiVersionsResponseApiKeys, int, error) {
	offset := 0
	apiKeys := ApiVersionsResponseApiKeys{}

	// Api Key
	apiKey, c, err := protocol.DecodeInt16(bytes[offset:])
	if err != nil {
		return apiKeys, offset, err
	}
	offset += c
	apiKeys.ApiKey = apiKey

	// Min version
	minVersion, c, err := protocol.DecodeInt16(bytes[offset:])
	if err != nil {
		return apiKeys, offset, err
	}
	offset += c
	apiKeys.MinVersion = minVersion

	// Max version
	maxVersion, c, err := protocol.DecodeInt16(bytes[offset:])
	if err != nil {
		return apiKeys, offset, err
	}
	offset += c
	apiKeys.MaxVersion = maxVersion

	// Tagged fields
	rawTaggedFields, c, err := protocol.DecodeRawTaggedFields(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
		return apiKeys, offset, err
	}
	offset += c
	apiKeys.rawTaggedFields = rawTaggedFields

	fmt.Printf("ApiKey: %d, MinVersion: %d, MaxVersion: %d\n", apiKey, minVersion, maxVersion)
	return apiKeys, offset, nil
}

func apiKeysReader(r io.Reader) (ApiVersionsResponseApiKeys, error) {
	apiKeys := ApiVersionsResponseApiKeys{}

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
	rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
		return apiKeys, err
	}
	apiKeys.rawTaggedFields = rawTaggedFields

	fmt.Printf("ApiKey: %d, MinVersion: %d, MaxVersion: %d\n", apiKey, minVersion, maxVersion)
	return apiKeys, nil
}

// TODO: Can this be class method?
func taggedFieldsDecoder(bytes []byte, r *ApiVersionsResponse, tag uint64, tagLength uint64) (int, error) {
	offset := 0

	switch tag {
	case 0:
		// SupportedFeatures
		supportedFeatures, c, err := protocol.DecodeCompactArray(bytes[offset:], supportedFeaturesDecoder)
		if err != nil {
			return offset, err
		}
		offset += c
		r.SupportedFeatures = supportedFeatures
	case 1:
		// FinalizedFeaturesEpoch
		finalizedFeaturesEpoch, c, err := protocol.DecodeInt64(bytes[offset:])
		if err != nil {
			fmt.Println("Failed to decode tag value", err)
			return offset, err
		}
		offset += c
		r.FinalizedFeaturesEpoch = finalizedFeaturesEpoch
	case 2:
		// FinalizedFeatures
		finalizedFeatures, c, err := protocol.DecodeCompactArray(bytes[offset:], finalizedFeaturesDecoder)
		if err != nil {
			return offset, err
		}
		offset += c
		r.FinalizedFeatures = finalizedFeatures
	case 3:
		// ZkMigrationReady
		zkMigrationReady, c, err := protocol.DecodeBool(bytes[offset:])
		if err != nil {
			fmt.Println("Failed to decode tag value", err)
			return offset, err
		}
		offset += c
		r.ZkMigrationReady = zkMigrationReady
	}

	return offset, nil
}

func supportedFeaturesDecoder(bytes []byte) (ApiVersionsResponseSupportedFeatures, int, error) {
	offset := 0
	supportedFeatures := ApiVersionsResponseSupportedFeatures{}

	name, c, err := protocol.DecodeCompactString(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tag value", err)
	}
	offset += c
	supportedFeatures.Name = name

	minVersion, c, err := protocol.DecodeInt16(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tag value", err)
	}
	offset += c
	supportedFeatures.MinVersion = minVersion

	maxVersion, c, err := protocol.DecodeInt16(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tag value", err)
	}
	offset += c
	supportedFeatures.MaxVersion = maxVersion

	// Tagged fields
	rawTaggedFields, c, err := protocol.DecodeRawTaggedFields(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
		return supportedFeatures, offset, err
	}
	offset += c
	supportedFeatures.rawTaggedFields = rawTaggedFields

	return supportedFeatures, offset, nil
}

func finalizedFeaturesDecoder(bytes []byte) (ApiVersionsResponseFinalizedFeatures, int, error) {
	offset := 0
	finalizedFeatures := ApiVersionsResponseFinalizedFeatures{}

	name, c, err := protocol.DecodeCompactString(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tag value", err)
	}
	offset += c
	finalizedFeatures.Name = name

	minVersionLevel, c, err := protocol.DecodeInt16(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tag value", err)
	}
	offset += c
	finalizedFeatures.MinVersionLevel = minVersionLevel

	maxVersionLevel, c, err := protocol.DecodeInt16(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tag value", err)
	}
	offset += c
	finalizedFeatures.MaxVersionLevel = maxVersionLevel

	// Tagged fields
	rawTaggedFields, c, err := protocol.DecodeRawTaggedFields(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
		return finalizedFeatures, offset, err
	}
	offset += c
	finalizedFeatures.rawTaggedFields = rawTaggedFields

	return finalizedFeatures, offset, nil
}
