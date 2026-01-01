package apiversions

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ApiVersionsResponse struct {
	ApiVersion             int16
	ErrorCode              int16                                  // The top-level error code.
	ApiKeys                *[]ApiVersionsResponseApiKey           // The APIs supported by the broker.
	ThrottleTimeMs         int32                                  // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	SupportedFeatures      *[]ApiVersionsResponseSupportedFeature // tag 0: Features supported by the broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
	FinalizedFeaturesEpoch int64                                  // tag 1: The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.
	FinalizedFeatures      *[]ApiVersionsResponseFinalizedFeature // tag 2: List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.
	ZkMigrationReady       bool                                   // tag 3: Set by a KRaft controller if the required configurations for ZK migration are present.
	rawTaggedFields        *[]protocol.TaggedField
}

type ApiVersionsResponseApiKey struct {
	ApiKey          int16 // The API index.
	MinVersion      int16 // The minimum supported version, inclusive.
	MaxVersion      int16 // The maximum supported version, inclusive.
	rawTaggedFields *[]protocol.TaggedField
}

type ApiVersionsResponseSupportedFeature struct {
	Name            *string // The name of the feature.
	MinVersion      int16   // The minimum supported version for the feature.
	MaxVersion      int16   // The maximum supported version for the feature.
	rawTaggedFields *[]protocol.TaggedField
}

type ApiVersionsResponseFinalizedFeature struct {
	Name            *string // The name of the feature.
	MaxVersionLevel int16   // The cluster-wide finalized max version level for the feature.
	MinVersionLevel int16   // The cluster-wide finalized min version level for the feature.
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (res *ApiVersionsResponse) Write(w io.Writer) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ApiKeys (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.apiKeysEncoder, res.ApiKeys); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.apiKeysEncoder, *res.ApiKeys); err != nil {
			return err
		}
	}

	// ThrottleTimeMs (versions: 1+)
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

	var err error

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// ApiKeys (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		apikeys, err := protocol.ReadNullableCompactArray(r, res.apiKeysDecoder)
		if err != nil {
			return err
		}
		res.ApiKeys = apikeys
	} else {
		apikeys, err := protocol.ReadArray(r, res.apiKeysDecoder)
		if err != nil {
			return err
		}
		res.ApiKeys = &apikeys
	}

	// ThrottleTimeMs (versions: 1+)
	if response.ApiVersion >= 1 {
		throttletimems, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttletimems
	}

	if isResponseFlexible(res.ApiVersion) {
		// Decode tagged fields
		err = protocol.ReadTaggedFields(r, res.taggedFieldsDecoder)
		if err != nil {
			return err
		}
	}

	return nil
}

func (res *ApiVersionsResponse) apiKeysEncoder(w io.Writer, value ApiVersionsResponseApiKey) error {
	// ApiKey (versions: 0+)
	if err := protocol.WriteInt16(w, value.ApiKey); err != nil {
		return err
	}

	// MinVersion (versions: 0+)
	if err := protocol.WriteInt16(w, value.MinVersion); err != nil {
		return err
	}

	// MaxVersion (versions: 0+)
	if err := protocol.WriteInt16(w, value.MaxVersion); err != nil {
		return err
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields := []protocol.TaggedField{}
		if value.rawTaggedFields != nil {
			rawTaggedFields = *value.rawTaggedFields
		}
		if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *ApiVersionsResponse) apiKeysDecoder(r io.Reader) (ApiVersionsResponseApiKey, error) {
	apiversionsresponseapikey := ApiVersionsResponseApiKey{}
	var err error

	// ApiKey (versions: 0+)
	apikey, err := protocol.ReadInt16(r)
	if err != nil {
		return apiversionsresponseapikey, err
	}
	apiversionsresponseapikey.ApiKey = apikey

	// MinVersion (versions: 0+)
	minversion, err := protocol.ReadInt16(r)
	if err != nil {
		return apiversionsresponseapikey, err
	}
	apiversionsresponseapikey.MinVersion = minversion

	// MaxVersion (versions: 0+)
	maxversion, err := protocol.ReadInt16(r)
	if err != nil {
		return apiversionsresponseapikey, err
	}
	apiversionsresponseapikey.MaxVersion = maxversion

	// Tagged fields
	if isRequestFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return apiversionsresponseapikey, err
		}
		apiversionsresponseapikey.rawTaggedFields = &rawTaggedFields
	}

	return apiversionsresponseapikey, nil
}

func (res *ApiVersionsResponse) supportedFeaturesEncoder(w io.Writer, value ApiVersionsResponseSupportedFeature) error {
	// Name (versions: 3+)
	if res.ApiVersion >= 3 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.Name); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.Name); err != nil {
				return err
			}
		}
	}

	// MinVersion (versions: 3+)
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt16(w, value.MinVersion); err != nil {
			return err
		}
	}

	// MaxVersion (versions: 3+)
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt16(w, value.MaxVersion); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields := []protocol.TaggedField{}
		if value.rawTaggedFields != nil {
			rawTaggedFields = *value.rawTaggedFields
		}
		if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *ApiVersionsResponse) supportedFeaturesDecoder(r io.Reader) (ApiVersionsResponseSupportedFeature, error) {
	apiversionsresponsesupportedfeature := ApiVersionsResponseSupportedFeature{}
	var err error

	// Name (versions: 3+)
	if res.ApiVersion >= 3 {
		if isRequestFlexible(res.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return apiversionsresponsesupportedfeature, err
			}
			apiversionsresponsesupportedfeature.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return apiversionsresponsesupportedfeature, err
			}
			apiversionsresponsesupportedfeature.Name = &name
		}
	}

	// MinVersion (versions: 3+)
	if res.ApiVersion >= 3 {
		minversion, err := protocol.ReadInt16(r)
		if err != nil {
			return apiversionsresponsesupportedfeature, err
		}
		apiversionsresponsesupportedfeature.MinVersion = minversion
	}

	// MaxVersion (versions: 3+)
	if res.ApiVersion >= 3 {
		maxversion, err := protocol.ReadInt16(r)
		if err != nil {
			return apiversionsresponsesupportedfeature, err
		}
		apiversionsresponsesupportedfeature.MaxVersion = maxversion
	}

	// Tagged fields
	if isRequestFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return apiversionsresponsesupportedfeature, err
		}
		apiversionsresponsesupportedfeature.rawTaggedFields = &rawTaggedFields
	}

	return apiversionsresponsesupportedfeature, nil
}

func (res *ApiVersionsResponse) finalizedFeaturesEncoder(w io.Writer, value ApiVersionsResponseFinalizedFeature) error {
	// Name (versions: 3+)
	if res.ApiVersion >= 3 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.Name); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.Name); err != nil {
				return err
			}
		}
	}

	// MaxVersionLevel (versions: 3+)
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt16(w, value.MaxVersionLevel); err != nil {
			return err
		}
	}

	// MinVersionLevel (versions: 3+)
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt16(w, value.MinVersionLevel); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields := []protocol.TaggedField{}
		if value.rawTaggedFields != nil {
			rawTaggedFields = *value.rawTaggedFields
		}
		if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *ApiVersionsResponse) finalizedFeaturesDecoder(r io.Reader) (ApiVersionsResponseFinalizedFeature, error) {
	apiversionsresponsefinalizedfeature := ApiVersionsResponseFinalizedFeature{}
	var err error

	// Name (versions: 3+)
	if res.ApiVersion >= 3 {
		if isRequestFlexible(res.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return apiversionsresponsefinalizedfeature, err
			}
			apiversionsresponsefinalizedfeature.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return apiversionsresponsefinalizedfeature, err
			}
			apiversionsresponsefinalizedfeature.Name = &name
		}
	}

	// MaxVersionLevel (versions: 3+)
	if res.ApiVersion >= 3 {
		maxversionlevel, err := protocol.ReadInt16(r)
		if err != nil {
			return apiversionsresponsefinalizedfeature, err
		}
		apiversionsresponsefinalizedfeature.MaxVersionLevel = maxversionlevel
	}

	// MinVersionLevel (versions: 3+)
	if res.ApiVersion >= 3 {
		minversionlevel, err := protocol.ReadInt16(r)
		if err != nil {
			return apiversionsresponsefinalizedfeature, err
		}
		apiversionsresponsefinalizedfeature.MinVersionLevel = minversionlevel
	}

	// Tagged fields
	if isRequestFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return apiversionsresponsefinalizedfeature, err
		}
		apiversionsresponsefinalizedfeature.rawTaggedFields = &rawTaggedFields
	}

	return apiversionsresponsefinalizedfeature, nil
}

func (res *ApiVersionsResponse) taggedFieldsEncoder() ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if res.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*res.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 5+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	buf = bytes.NewBuffer(make([]byte, 0))
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
	if res.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *res.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (res *ApiVersionsResponse) taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {
	rawTaggedFields := make([]protocol.TaggedField, 0)

	switch tag {
	case 0:
		// SupportedFeatures
		supportedfeatures, err := protocol.ReadNullableCompactArray(r, res.supportedFeaturesDecoder)
		if err != nil {
			return err
		}
		res.SupportedFeatures = supportedfeatures
	case 1:
		// FinalizedFeaturesEpoch
		finalizedfeaturesepoch, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		res.FinalizedFeaturesEpoch = finalizedfeaturesepoch
	case 2:
		// FinalizedFeatures
		finalizedfeatures, err := protocol.ReadNullableCompactArray(r, res.finalizedFeaturesDecoder)
		if err != nil {
			return err
		}
		res.FinalizedFeatures = finalizedfeatures
	case 3:
		// ZkMigrationReady
		zkmigrationready, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		res.ZkMigrationReady = zkmigrationready
	default:
		// Decode as raw tags
		taggedField, err := protocol.ReadRawTaggedField(r)
		if err != nil {
			return err
		}
		rawTaggedFields = append(rawTaggedFields, taggedField)
	}

	// Set the raw tagged fields
	res.rawTaggedFields = &rawTaggedFields

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ApiVersionsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ApiVersionsResponse:\n")
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
	if res.ApiKeys != nil {
		fmt.Fprintf(w, "        ApiKeys:\n")
		for _, apikeys := range *res.ApiKeys {
			fmt.Fprintf(w, "%s", apikeys.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        ApiKeys: nil\n")
	}
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	if res.SupportedFeatures != nil {
		fmt.Fprintf(w, "        SupportedFeatures:\n")
		for _, supportedfeatures := range *res.SupportedFeatures {
			fmt.Fprintf(w, "%s", supportedfeatures.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        SupportedFeatures: nil\n")
	}
	fmt.Fprintf(w, "        FinalizedFeaturesEpoch: %v\n", res.FinalizedFeaturesEpoch)
	if res.FinalizedFeatures != nil {
		fmt.Fprintf(w, "        FinalizedFeatures:\n")
		for _, finalizedfeatures := range *res.FinalizedFeatures {
			fmt.Fprintf(w, "%s", finalizedfeatures.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        FinalizedFeatures: nil\n")
	}
	fmt.Fprintf(w, "        ZkMigrationReady: %v\n", res.ZkMigrationReady)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ApiVersionsResponseApiKey) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ApiKey: %v\n", value.ApiKey)
	fmt.Fprintf(w, "            MinVersion: %v\n", value.MinVersion)
	fmt.Fprintf(w, "            MaxVersion: %v\n", value.MaxVersion)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ApiVersionsResponseSupportedFeature) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}
	fmt.Fprintf(w, "            MinVersion: %v\n", value.MinVersion)
	fmt.Fprintf(w, "            MaxVersion: %v\n", value.MaxVersion)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ApiVersionsResponseFinalizedFeature) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}
	fmt.Fprintf(w, "            MaxVersionLevel: %v\n", value.MaxVersionLevel)
	fmt.Fprintf(w, "            MinVersionLevel: %v\n", value.MinVersionLevel)

	return w.String()
}
