package incrementalalterconfigs

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type IncrementalAlterConfigsRequest struct {
	ApiVersion      int16
	Resources       *[]IncrementalAlterConfigsRequestResource // The incremental updates for each resource. (versions: 0+)
	ValidateOnly    bool                                      // True if we should validate the request, but not change the configurations. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type IncrementalAlterConfigsRequestResource struct {
	ResourceType    int8                                            // The resource type. (versions: 0+)
	ResourceName    *string                                         // The resource name. (versions: 0+)
	Configs         *[]IncrementalAlterConfigsRequestResourceConfig // The configurations. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type IncrementalAlterConfigsRequestResourceConfig struct {
	Name            *string // The configuration key name. (versions: 0+)
	ConfigOperation int8    // The type (Set, Delete, Append, Subtract) of operation. (versions: 0+)
	Value           *string // The value to set for the configuration key. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 1
}

func (req *IncrementalAlterConfigsRequest) Write(w io.Writer) error {
	// Resources (versions: 0+)
	if req.Resources == nil {
		return fmt.Errorf("IncrementalAlterConfigsRequest.Resources must not be nil in version %d", req.ApiVersion)
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
func (req *IncrementalAlterConfigsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("IncrementalAlterConfigsRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Resources (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		resources, err := protocol.ReadNullableCompactArray(r, req.resourcesDecoder)
		if err != nil {
			return err
		}
		req.Resources = resources
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

func (req *IncrementalAlterConfigsRequest) resourcesEncoder(w io.Writer, value IncrementalAlterConfigsRequestResource) error {
	// ResourceType (versions: 0+)
	if err := protocol.WriteInt8(w, value.ResourceType); err != nil {
		return err
	}

	// ResourceName (versions: 0+)
	if value.ResourceName == nil {
		return fmt.Errorf("IncrementalAlterConfigsRequestResource.ResourceName must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("IncrementalAlterConfigsRequestResource.Configs must not be nil in version %d", req.ApiVersion)
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

func (req *IncrementalAlterConfigsRequest) resourcesDecoder(r io.Reader) (IncrementalAlterConfigsRequestResource, error) {
	incrementalalterconfigsrequestresource := IncrementalAlterConfigsRequestResource{}

	// ResourceType (versions: 0+)
	resourcetype, err := protocol.ReadInt8(r)
	if err != nil {
		return incrementalalterconfigsrequestresource, err
	}
	incrementalalterconfigsrequestresource.ResourceType = resourcetype

	// ResourceName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		resourcename, err := protocol.ReadCompactString(r)
		if err != nil {
			return incrementalalterconfigsrequestresource, err
		}
		incrementalalterconfigsrequestresource.ResourceName = &resourcename
	} else {
		resourcename, err := protocol.ReadString(r)
		if err != nil {
			return incrementalalterconfigsrequestresource, err
		}
		incrementalalterconfigsrequestresource.ResourceName = &resourcename
	}

	// Configs (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		configs, err := protocol.ReadNullableCompactArray(r, req.configsDecoder)
		if err != nil {
			return incrementalalterconfigsrequestresource, err
		}
		incrementalalterconfigsrequestresource.Configs = configs
	} else {
		configs, err := protocol.ReadArray(r, req.configsDecoder)
		if err != nil {
			return incrementalalterconfigsrequestresource, err
		}
		incrementalalterconfigsrequestresource.Configs = &configs
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return incrementalalterconfigsrequestresource, err
		}
		incrementalalterconfigsrequestresource.rawTaggedFields = &rawTaggedFields
	}

	return incrementalalterconfigsrequestresource, nil
}

func (req *IncrementalAlterConfigsRequest) configsEncoder(w io.Writer, value IncrementalAlterConfigsRequestResourceConfig) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("IncrementalAlterConfigsRequestResourceConfig.Name must not be nil in version %d", req.ApiVersion)
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

	// ConfigOperation (versions: 0+)
	if err := protocol.WriteInt8(w, value.ConfigOperation); err != nil {
		return err
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

func (req *IncrementalAlterConfigsRequest) configsDecoder(r io.Reader) (IncrementalAlterConfigsRequestResourceConfig, error) {
	incrementalalterconfigsrequestresourceconfig := IncrementalAlterConfigsRequestResourceConfig{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return incrementalalterconfigsrequestresourceconfig, err
		}
		incrementalalterconfigsrequestresourceconfig.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return incrementalalterconfigsrequestresourceconfig, err
		}
		incrementalalterconfigsrequestresourceconfig.Name = &name
	}

	// ConfigOperation (versions: 0+)
	configoperation, err := protocol.ReadInt8(r)
	if err != nil {
		return incrementalalterconfigsrequestresourceconfig, err
	}
	incrementalalterconfigsrequestresourceconfig.ConfigOperation = configoperation

	// Value (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		value, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return incrementalalterconfigsrequestresourceconfig, err
		}
		incrementalalterconfigsrequestresourceconfig.Value = value
	} else {
		value, err := protocol.ReadNullableString(r)
		if err != nil {
			return incrementalalterconfigsrequestresourceconfig, err
		}
		incrementalalterconfigsrequestresourceconfig.Value = value
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return incrementalalterconfigsrequestresourceconfig, err
		}
		incrementalalterconfigsrequestresourceconfig.rawTaggedFields = &rawTaggedFields
	}

	return incrementalalterconfigsrequestresourceconfig, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *IncrementalAlterConfigsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> IncrementalAlterConfigsRequest:\n")

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
func (value *IncrementalAlterConfigsRequestResource) PrettyPrint() string {
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
func (value *IncrementalAlterConfigsRequestResourceConfig) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

	fmt.Fprintf(w, "                ConfigOperation: %v\n", value.ConfigOperation)

	if value.Value != nil {
		fmt.Fprintf(w, "                Value: %v\n", *value.Value)
	} else {
		fmt.Fprintf(w, "                Value: nil\n")
	}

	return w.String()
}
