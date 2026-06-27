package alterconfigs

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AlterConfigsRequest struct {
	ApiVersion      int16
	Resources       *[]AlterConfigsRequestResource // The updates for each resource. (versions: 0+)
	ValidateOnly    bool                           // True if we should validate the request, but not change the configurations. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterConfigsRequestResource struct {
	ResourceType    int8                                 // The resource type. (versions: 0+)
	ResourceName    *string                              // The resource name. (versions: 0+)
	Configs         *[]AlterConfigsRequestResourceConfig // The configurations. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterConfigsRequestResourceConfig struct {
	Name            *string // The configuration key name. (versions: 0+)
	Value           *string // The value to set for the configuration key. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *AlterConfigsRequest) Write(w io.Writer) error {
	// Resources (versions: 0+)
	if req.Resources == nil {
		return fmt.Errorf("AlterConfigsRequest.Resources must not be nil in version %d", req.ApiVersion)
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

	// ValidateOnly (versions: 0+)
	if err := protocol.WriteBool(w, req.ValidateOnly); err != nil {
		return err
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
func (req *AlterConfigsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("AlterConfigsRequest.Read: request or its body is nil")
	}

	*req = AlterConfigsRequest{}

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

	// ValidateOnly (versions: 0+)
	validateonly, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	req.ValidateOnly = validateonly

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

func (req *AlterConfigsRequest) resourcesEncoder(w io.Writer, value AlterConfigsRequestResource) error {
	// ResourceType (versions: 0+)
	if err := protocol.WriteInt8(w, value.ResourceType); err != nil {
		return err
	}

	// ResourceName (versions: 0+)
	if value.ResourceName == nil {
		return fmt.Errorf("AlterConfigsRequestResource.ResourceName must not be nil in version %d", req.ApiVersion)
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

	// Configs (versions: 0+)
	if value.Configs == nil {
		return fmt.Errorf("AlterConfigsRequestResource.Configs must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.configsEncoder, value.Configs); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.configsEncoder, *value.Configs); err != nil {
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

func (req *AlterConfigsRequest) resourcesDecoder(r io.Reader) (AlterConfigsRequestResource, error) {
	alterconfigsrequestresource := AlterConfigsRequestResource{}

	// ResourceType (versions: 0+)
	resourcetype, err := protocol.ReadInt8(r)
	if err != nil {
		return alterconfigsrequestresource, err
	}
	alterconfigsrequestresource.ResourceType = resourcetype

	// ResourceName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		resourcename, err := protocol.ReadCompactString(r)
		if err != nil {
			return alterconfigsrequestresource, err
		}
		alterconfigsrequestresource.ResourceName = &resourcename
	} else {
		resourcename, err := protocol.ReadString(r)
		if err != nil {
			return alterconfigsrequestresource, err
		}
		alterconfigsrequestresource.ResourceName = &resourcename
	}

	// Configs (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		configs, err := protocol.ReadCompactArray(r, req.configsDecoder)
		if err != nil {
			return alterconfigsrequestresource, err
		}
		alterconfigsrequestresource.Configs = &configs
	} else {
		configs, err := protocol.ReadArray(r, req.configsDecoder)
		if err != nil {
			return alterconfigsrequestresource, err
		}
		alterconfigsrequestresource.Configs = &configs
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterconfigsrequestresource, err
		}
		alterconfigsrequestresource.rawTaggedFields = &rawTaggedFields
	}

	return alterconfigsrequestresource, nil
}

func (req *AlterConfigsRequest) configsEncoder(w io.Writer, value AlterConfigsRequestResourceConfig) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("AlterConfigsRequestResourceConfig.Name must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// Value (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Value); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.Value); err != nil {
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

func (req *AlterConfigsRequest) configsDecoder(r io.Reader) (AlterConfigsRequestResourceConfig, error) {
	alterconfigsrequestresourceconfig := AlterConfigsRequestResourceConfig{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return alterconfigsrequestresourceconfig, err
		}
		alterconfigsrequestresourceconfig.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return alterconfigsrequestresourceconfig, err
		}
		alterconfigsrequestresourceconfig.Name = &name
	}

	// Value (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		value, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return alterconfigsrequestresourceconfig, err
		}
		alterconfigsrequestresourceconfig.Value = value
	} else {
		value, err := protocol.ReadNullableString(r)
		if err != nil {
			return alterconfigsrequestresourceconfig, err
		}
		alterconfigsrequestresourceconfig.Value = value
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterconfigsrequestresourceconfig, err
		}
		alterconfigsrequestresourceconfig.rawTaggedFields = &rawTaggedFields
	}

	return alterconfigsrequestresourceconfig, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *AlterConfigsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> AlterConfigsRequest:\n")

	if req.Resources != nil {
		fmt.Fprintf(w, "        Resources:\n")
		for _, resources := range *req.Resources {
			fmt.Fprintf(w, "%s", resources.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Resources: nil\n")
	}

	fmt.Fprintf(w, "        ValidateOnly: %v\n", req.ValidateOnly)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterConfigsRequestResource) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

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
func (value *AlterConfigsRequestResourceConfig) PrettyPrint() string {
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

	return w.String()
}
