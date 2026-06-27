package describeconfigs

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeConfigsRequest struct {
	ApiVersion           int16
	Resources            *[]DescribeConfigsRequestResource // The resources whose configurations we want to describe. (versions: 0+)
	IncludeSynonyms      bool                              // True if we should include all synonyms. (versions: 1+)
	IncludeDocumentation bool                              // True if we should include configuration documentation. (versions: 3+)
	rawTaggedFields      *[]protocol.TaggedField
}

type DescribeConfigsRequestResource struct {
	ResourceType      int8      // The resource type. (versions: 0+)
	ResourceName      *string   // The resource name. (versions: 0+)
	ConfigurationKeys *[]string // The configuration keys to list, or null to list all configuration keys. (versions: 0+, nullable: 0+)
	rawTaggedFields   *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 4
}

func (req *DescribeConfigsRequest) Write(w io.Writer) error {
	// Resources (versions: 0+)
	if req.Resources == nil {
		return fmt.Errorf("DescribeConfigsRequest.Resources must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.resourcesEncoder, req.Resources); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.resourcesEncoder, *req.Resources); err != nil {
			return err
		}
	}

	// IncludeSynonyms (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteBool(w, req.IncludeSynonyms); err != nil {
			return err
		}
	}

	// IncludeDocumentation (versions: 3+)
	if req.ApiVersion >= 3 {
		if err := protocol.WriteBool(w, req.IncludeDocumentation); err != nil {
			return err
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields := []protocol.TaggedField{}
		if req.rawTaggedFields != nil {
			rawTaggedFields = *req.rawTaggedFields
		}
		if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (req *DescribeConfigsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeConfigsRequest.Read: request or its body is nil")
	}

	*req = DescribeConfigsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Resources (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		resources, err := protocol.ReadCompactArray(r, req.resourcesDecoder)
		if err != nil {
			return err
		}
		req.Resources = &resources
	} else {
		resources, err := protocol.ReadArray(r, req.resourcesDecoder)
		if err != nil {
			return err
		}
		req.Resources = &resources
	}

	// IncludeSynonyms (versions: 1+)
	if req.ApiVersion >= 1 {
		includesynonyms, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.IncludeSynonyms = includesynonyms
	}

	// IncludeDocumentation (versions: 3+)
	if req.ApiVersion >= 3 {
		includedocumentation, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.IncludeDocumentation = includedocumentation
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		req.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

func (req *DescribeConfigsRequest) resourcesEncoder(w io.Writer, value DescribeConfigsRequestResource) error {
	// ResourceType (versions: 0+)
	if err := protocol.WriteInt8(w, value.ResourceType); err != nil {
		return err
	}

	// ResourceName (versions: 0+)
	if value.ResourceName == nil {
		return fmt.Errorf("DescribeConfigsRequestResource.ResourceName must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.ResourceName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.ResourceName); err != nil {
			return err
		}
	}

	// ConfigurationKeys (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, value.ConfigurationKeys); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, protocol.WriteString, value.ConfigurationKeys); err != nil {
			return err
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
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

func (req *DescribeConfigsRequest) resourcesDecoder(r io.Reader) (DescribeConfigsRequestResource, error) {
	describeconfigsrequestresource := DescribeConfigsRequestResource{}

	// ResourceType (versions: 0+)
	resourcetype, err := protocol.ReadInt8(r)
	if err != nil {
		return describeconfigsrequestresource, err
	}
	describeconfigsrequestresource.ResourceType = resourcetype

	// ResourceName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		resourcename, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeconfigsrequestresource, err
		}
		describeconfigsrequestresource.ResourceName = &resourcename
	} else {
		resourcename, err := protocol.ReadString(r)
		if err != nil {
			return describeconfigsrequestresource, err
		}
		describeconfigsrequestresource.ResourceName = &resourcename
	}

	// ConfigurationKeys (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		configurationkeys, err := protocol.ReadNullableCompactArray(r, protocol.ReadCompactString)
		if err != nil {
			return describeconfigsrequestresource, err
		}
		describeconfigsrequestresource.ConfigurationKeys = configurationkeys
	} else {
		configurationkeys, err := protocol.ReadNullableArray(r, protocol.ReadString)
		if err != nil {
			return describeconfigsrequestresource, err
		}
		describeconfigsrequestresource.ConfigurationKeys = configurationkeys
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeconfigsrequestresource, err
		}
		describeconfigsrequestresource.rawTaggedFields = &rawTaggedFields
	}

	return describeconfigsrequestresource, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DescribeConfigsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeConfigsRequest:\n")

	if req.Resources != nil {
		fmt.Fprintf(w, "        Resources:\n")
		for _, resources := range *req.Resources {
			fmt.Fprintf(w, "%s", resources.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Resources: nil\n")
	}

	fmt.Fprintf(w, "        IncludeSynonyms: %v\n", req.IncludeSynonyms)
	fmt.Fprintf(w, "        IncludeDocumentation: %v\n", req.IncludeDocumentation)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeConfigsRequestResource) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ResourceType: %v\n", value.ResourceType)

	if value.ResourceName != nil {
		fmt.Fprintf(w, "            ResourceName: %v\n", *value.ResourceName)
	} else {
		fmt.Fprintf(w, "            ResourceName: nil\n")
	}

	if value.ConfigurationKeys != nil {
		fmt.Fprintf(w, "            ConfigurationKeys: %v\n", *value.ConfigurationKeys)
	} else {
		fmt.Fprintf(w, "            ConfigurationKeys: nil\n")
	}

	return w.String()
}
