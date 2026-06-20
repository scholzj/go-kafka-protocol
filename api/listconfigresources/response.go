package listconfigresources

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ListConfigResourcesResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                        // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                                        // The error code, or 0 if there was no error. (versions: 0+)
	ConfigResources *[]ListConfigResourcesResponseConfigResource // Each config resource in the response. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ListConfigResourcesResponseConfigResource struct {
	ResourceName    *string // The resource name. (versions: 0+)
	ResourceType    int8    // The resource type. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *ListConfigResourcesResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ConfigResources (versions: 0+)
	if res.ConfigResources == nil {
		return fmt.Errorf("ListConfigResourcesResponse.ConfigResources must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.configResourcesEncoder, res.ConfigResources); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.configResourcesEncoder, *res.ConfigResources); err != nil {
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
func (res *ListConfigResourcesResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("ListConfigResourcesResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// ConfigResources (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		configresources, err := protocol.ReadNullableCompactArray(r, res.configResourcesDecoder)
		if err != nil {
			return err
		}
		res.ConfigResources = configresources
	} else {
		configresources, err := protocol.ReadArray(r, res.configResourcesDecoder)
		if err != nil {
			return err
		}
		res.ConfigResources = &configresources
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

func (res *ListConfigResourcesResponse) configResourcesEncoder(w io.Writer, value ListConfigResourcesResponseConfigResource) error {
	// ResourceName (versions: 0+)
	if value.ResourceName == nil {
		return fmt.Errorf("ListConfigResourcesResponseConfigResource.ResourceName must not be nil in version %d", res.ApiVersion)
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

	// ResourceType (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, value.ResourceType); err != nil {
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

func (res *ListConfigResourcesResponse) configResourcesDecoder(r io.Reader) (ListConfigResourcesResponseConfigResource, error) {
	listconfigresourcesresponseconfigresource := ListConfigResourcesResponseConfigResource{}

	// ResourceName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		resourcename, err := protocol.ReadCompactString(r)
		if err != nil {
			return listconfigresourcesresponseconfigresource, err
		}
		listconfigresourcesresponseconfigresource.ResourceName = &resourcename
	} else {
		resourcename, err := protocol.ReadString(r)
		if err != nil {
			return listconfigresourcesresponseconfigresource, err
		}
		listconfigresourcesresponseconfigresource.ResourceName = &resourcename
	}

	// ResourceType (versions: 1+)
	if res.ApiVersion >= 1 {
		resourcetype, err := protocol.ReadInt8(r)
		if err != nil {
			return listconfigresourcesresponseconfigresource, err
		}
		listconfigresourcesresponseconfigresource.ResourceType = resourcetype
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return listconfigresourcesresponseconfigresource, err
		}
		listconfigresourcesresponseconfigresource.rawTaggedFields = &rawTaggedFields
	}

	return listconfigresourcesresponseconfigresource, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ListConfigResourcesResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ListConfigResourcesResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.ConfigResources != nil {
		fmt.Fprintf(w, "        ConfigResources:\n")
		for _, configresources := range *res.ConfigResources {
			fmt.Fprintf(w, "%s", configresources.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        ConfigResources: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ListConfigResourcesResponseConfigResource) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.ResourceName != nil {
		fmt.Fprintf(w, "            ResourceName: %v\n", *value.ResourceName)
	} else {
		fmt.Fprintf(w, "            ResourceName: nil\n")
	}

	fmt.Fprintf(w, "            ResourceType: %v\n", value.ResourceType)

	return w.String()
}
