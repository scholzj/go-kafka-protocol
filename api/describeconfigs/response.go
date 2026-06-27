package describeconfigs

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeConfigsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                            // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	Results         *[]DescribeConfigsResponseResult // The results for each resource. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeConfigsResponseResult struct {
	ErrorCode       int16                                  // The error code, or 0 if we were able to successfully describe the configurations. (versions: 0+)
	ErrorMessage    *string                                // The error message, or null if we were able to successfully describe the configurations. (versions: 0+, nullable: 0+)
	ResourceType    int8                                   // The resource type. (versions: 0+)
	ResourceName    *string                                // The resource name. (versions: 0+)
	Configs         *[]DescribeConfigsResponseResultConfig // Each listed configuration. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeConfigsResponseResultConfig struct {
	Name            *string                                       // The configuration name. (versions: 0+)
	Value           *string                                       // The configuration value. (versions: 0+, nullable: 0+)
	ReadOnly        bool                                          // True if the configuration is read-only. (versions: 0+)
	ConfigSource    int8                                          // The configuration source. (versions: 1+)
	IsSensitive     bool                                          // True if this configuration is sensitive. (versions: 0+)
	Synonyms        *[]DescribeConfigsResponseResultConfigSynonym // The synonyms for this configuration key. (versions: 1+)
	ConfigType      int8                                          // The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD. (versions: 3+)
	Documentation   *string                                       // The configuration documentation. (versions: 3+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeConfigsResponseResultConfigSynonym struct {
	Name            *string // The synonym name. (versions: 1+)
	Value           *string // The synonym value. (versions: 1+, nullable: 0+)
	Source          int8    // The synonym source. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 4
}

func (res *DescribeConfigsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// Results (versions: 0+)
	if res.Results == nil {
		return fmt.Errorf("DescribeConfigsResponse.Results must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.resultsEncoder, res.Results); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.resultsEncoder, *res.Results); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields := []protocol.TaggedField{}
		if res.rawTaggedFields != nil {
			rawTaggedFields = *res.rawTaggedFields
		}
		if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (res *DescribeConfigsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeConfigsResponse.Read: response or its body is nil")
	}

	*res = DescribeConfigsResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// Results (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		results, err := protocol.ReadCompactArray(r, res.resultsDecoder)
		if err != nil {
			return err
		}
		res.Results = &results
	} else {
		results, err := protocol.ReadArray(r, res.resultsDecoder)
		if err != nil {
			return err
		}
		res.Results = &results
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		res.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

func (res *DescribeConfigsResponse) resultsEncoder(w io.Writer, value DescribeConfigsResponseResult) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.ErrorMessage); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.ErrorMessage); err != nil {
			return err
		}
	}

	// ResourceType (versions: 0+)
	if err := protocol.WriteInt8(w, value.ResourceType); err != nil {
		return err
	}

	// ResourceName (versions: 0+)
	if value.ResourceName == nil {
		return fmt.Errorf("DescribeConfigsResponseResult.ResourceName must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.ResourceName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.ResourceName); err != nil {
			return err
		}
	}

	// Configs (versions: 0+)
	if value.Configs == nil {
		return fmt.Errorf("DescribeConfigsResponseResult.Configs must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.configsEncoder, value.Configs); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.configsEncoder, *value.Configs); err != nil {
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

func (res *DescribeConfigsResponse) resultsDecoder(r io.Reader) (DescribeConfigsResponseResult, error) {
	describeconfigsresponseresult := DescribeConfigsResponseResult{}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return describeconfigsresponseresult, err
	}
	describeconfigsresponseresult.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return describeconfigsresponseresult, err
		}
		describeconfigsresponseresult.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return describeconfigsresponseresult, err
		}
		describeconfigsresponseresult.ErrorMessage = errormessage
	}

	// ResourceType (versions: 0+)
	resourcetype, err := protocol.ReadInt8(r)
	if err != nil {
		return describeconfigsresponseresult, err
	}
	describeconfigsresponseresult.ResourceType = resourcetype

	// ResourceName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		resourcename, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeconfigsresponseresult, err
		}
		describeconfigsresponseresult.ResourceName = &resourcename
	} else {
		resourcename, err := protocol.ReadString(r)
		if err != nil {
			return describeconfigsresponseresult, err
		}
		describeconfigsresponseresult.ResourceName = &resourcename
	}

	// Configs (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		configs, err := protocol.ReadCompactArray(r, res.configsDecoder)
		if err != nil {
			return describeconfigsresponseresult, err
		}
		describeconfigsresponseresult.Configs = &configs
	} else {
		configs, err := protocol.ReadArray(r, res.configsDecoder)
		if err != nil {
			return describeconfigsresponseresult, err
		}
		describeconfigsresponseresult.Configs = &configs
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeconfigsresponseresult, err
		}
		describeconfigsresponseresult.rawTaggedFields = &rawTaggedFields
	}

	return describeconfigsresponseresult, nil
}

func (res *DescribeConfigsResponse) configsEncoder(w io.Writer, value DescribeConfigsResponseResultConfig) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("DescribeConfigsResponseResultConfig.Name must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// Value (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Value); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.Value); err != nil {
			return err
		}
	}

	// ReadOnly (versions: 0+)
	if err := protocol.WriteBool(w, value.ReadOnly); err != nil {
		return err
	}

	// ConfigSource (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, value.ConfigSource); err != nil {
			return err
		}
	}

	// IsSensitive (versions: 0+)
	if err := protocol.WriteBool(w, value.IsSensitive); err != nil {
		return err
	}

	// Synonyms (versions: 1+)
	if res.ApiVersion >= 1 {
		if value.Synonyms == nil {
			return fmt.Errorf("DescribeConfigsResponseResultConfig.Synonyms must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.synonymsEncoder, value.Synonyms); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, res.synonymsEncoder, *value.Synonyms); err != nil {
				return err
			}
		}
	}

	// ConfigType (versions: 3+)
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt8(w, value.ConfigType); err != nil {
			return err
		}
	}

	// Documentation (versions: 3+)
	if res.ApiVersion >= 3 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.Documentation); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.Documentation); err != nil {
				return err
			}
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

func (res *DescribeConfigsResponse) configsDecoder(r io.Reader) (DescribeConfigsResponseResultConfig, error) {
	describeconfigsresponseresultconfig := DescribeConfigsResponseResultConfig{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	describeconfigsresponseresultconfig.ConfigSource = -1

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeconfigsresponseresultconfig, err
		}
		describeconfigsresponseresultconfig.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return describeconfigsresponseresultconfig, err
		}
		describeconfigsresponseresultconfig.Name = &name
	}

	// Value (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		value, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return describeconfigsresponseresultconfig, err
		}
		describeconfigsresponseresultconfig.Value = value
	} else {
		value, err := protocol.ReadNullableString(r)
		if err != nil {
			return describeconfigsresponseresultconfig, err
		}
		describeconfigsresponseresultconfig.Value = value
	}

	// ReadOnly (versions: 0+)
	readonly, err := protocol.ReadBool(r)
	if err != nil {
		return describeconfigsresponseresultconfig, err
	}
	describeconfigsresponseresultconfig.ReadOnly = readonly

	// ConfigSource (versions: 1+)
	if res.ApiVersion >= 1 {
		configsource, err := protocol.ReadInt8(r)
		if err != nil {
			return describeconfigsresponseresultconfig, err
		}
		describeconfigsresponseresultconfig.ConfigSource = configsource
	}

	// IsSensitive (versions: 0+)
	issensitive, err := protocol.ReadBool(r)
	if err != nil {
		return describeconfigsresponseresultconfig, err
	}
	describeconfigsresponseresultconfig.IsSensitive = issensitive

	// Synonyms (versions: 1+)
	if res.ApiVersion >= 1 {
		if isResponseFlexible(res.ApiVersion) {
			synonyms, err := protocol.ReadCompactArray(r, res.synonymsDecoder)
			if err != nil {
				return describeconfigsresponseresultconfig, err
			}
			describeconfigsresponseresultconfig.Synonyms = &synonyms
		} else {
			synonyms, err := protocol.ReadArray(r, res.synonymsDecoder)
			if err != nil {
				return describeconfigsresponseresultconfig, err
			}
			describeconfigsresponseresultconfig.Synonyms = &synonyms
		}
	}

	// ConfigType (versions: 3+)
	if res.ApiVersion >= 3 {
		configtype, err := protocol.ReadInt8(r)
		if err != nil {
			return describeconfigsresponseresultconfig, err
		}
		describeconfigsresponseresultconfig.ConfigType = configtype
	}

	// Documentation (versions: 3+)
	if res.ApiVersion >= 3 {
		if isResponseFlexible(res.ApiVersion) {
			documentation, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return describeconfigsresponseresultconfig, err
			}
			describeconfigsresponseresultconfig.Documentation = documentation
		} else {
			documentation, err := protocol.ReadNullableString(r)
			if err != nil {
				return describeconfigsresponseresultconfig, err
			}
			describeconfigsresponseresultconfig.Documentation = documentation
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeconfigsresponseresultconfig, err
		}
		describeconfigsresponseresultconfig.rawTaggedFields = &rawTaggedFields
	}

	return describeconfigsresponseresultconfig, nil
}

func (res *DescribeConfigsResponse) synonymsEncoder(w io.Writer, value DescribeConfigsResponseResultConfigSynonym) error {
	// Name (versions: 1+)
	if res.ApiVersion >= 1 {
		if value.Name == nil {
			return fmt.Errorf("DescribeConfigsResponseResultConfigSynonym.Name must not be nil in version %d", res.ApiVersion)
		}
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

	// Value (versions: 1+)
	if res.ApiVersion >= 1 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.Value); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.Value); err != nil {
				return err
			}
		}
	}

	// Source (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, value.Source); err != nil {
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

func (res *DescribeConfigsResponse) synonymsDecoder(r io.Reader) (DescribeConfigsResponseResultConfigSynonym, error) {
	describeconfigsresponseresultconfigsynonym := DescribeConfigsResponseResultConfigSynonym{}

	// Name (versions: 1+)
	if res.ApiVersion >= 1 {
		if isResponseFlexible(res.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return describeconfigsresponseresultconfigsynonym, err
			}
			describeconfigsresponseresultconfigsynonym.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return describeconfigsresponseresultconfigsynonym, err
			}
			describeconfigsresponseresultconfigsynonym.Name = &name
		}
	}

	// Value (versions: 1+)
	if res.ApiVersion >= 1 {
		if isResponseFlexible(res.ApiVersion) {
			value, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return describeconfigsresponseresultconfigsynonym, err
			}
			describeconfigsresponseresultconfigsynonym.Value = value
		} else {
			value, err := protocol.ReadNullableString(r)
			if err != nil {
				return describeconfigsresponseresultconfigsynonym, err
			}
			describeconfigsresponseresultconfigsynonym.Value = value
		}
	}

	// Source (versions: 1+)
	if res.ApiVersion >= 1 {
		source, err := protocol.ReadInt8(r)
		if err != nil {
			return describeconfigsresponseresultconfigsynonym, err
		}
		describeconfigsresponseresultconfigsynonym.Source = source
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeconfigsresponseresultconfigsynonym, err
		}
		describeconfigsresponseresultconfigsynonym.rawTaggedFields = &rawTaggedFields
	}

	return describeconfigsresponseresultconfigsynonym, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeConfigsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeConfigsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	if res.Results != nil {
		fmt.Fprintf(w, "        Results:\n")
		for _, results := range *res.Results {
			fmt.Fprintf(w, "%s", results.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Results: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeConfigsResponseResult) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	fmt.Fprintf(w, "            ResourceType: %v\n", value.ResourceType)

	if value.ResourceName != nil {
		fmt.Fprintf(w, "            ResourceName: %v\n", *value.ResourceName)
	} else {
		fmt.Fprintf(w, "            ResourceName: nil\n")
	}

	if value.Configs != nil {
		fmt.Fprintf(w, "            Configs:\n")
		for _, configs := range *value.Configs {
			fmt.Fprintf(w, "%s", configs.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Configs: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeConfigsResponseResultConfig) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

	if value.Value != nil {
		fmt.Fprintf(w, "                Value: %v\n", *value.Value)
	} else {
		fmt.Fprintf(w, "                Value: nil\n")
	}

	fmt.Fprintf(w, "                ReadOnly: %v\n", value.ReadOnly)
	fmt.Fprintf(w, "                ConfigSource: %v\n", value.ConfigSource)
	fmt.Fprintf(w, "                IsSensitive: %v\n", value.IsSensitive)

	if value.Synonyms != nil {
		fmt.Fprintf(w, "                Synonyms:\n")
		for _, synonyms := range *value.Synonyms {
			fmt.Fprintf(w, "%s", synonyms.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                Synonyms: nil\n")
	}

	fmt.Fprintf(w, "                ConfigType: %v\n", value.ConfigType)

	if value.Documentation != nil {
		fmt.Fprintf(w, "                Documentation: %v\n", *value.Documentation)
	} else {
		fmt.Fprintf(w, "                Documentation: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeConfigsResponseResultConfigSynonym) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                    Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                    Name: nil\n")
	}

	if value.Value != nil {
		fmt.Fprintf(w, "                    Value: %v\n", *value.Value)
	} else {
		fmt.Fprintf(w, "                    Value: nil\n")
	}

	fmt.Fprintf(w, "                    Source: %v\n", value.Source)

	return w.String()
}
