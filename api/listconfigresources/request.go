package listconfigresources

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ListConfigResourcesRequest struct {
	ApiVersion      int16
	ResourceTypes   *[]int8 // The list of resource type. If the list is empty, it uses default supported config resource types. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *ListConfigResourcesRequest) Write(w io.Writer) error {
	// ResourceTypes (versions: 1+)
	if req.ApiVersion >= 1 {
		if req.ResourceTypes == nil {
			return fmt.Errorf("ListConfigResourcesRequest.ResourceTypes must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt8, req.ResourceTypes); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteInt8, *req.ResourceTypes); err != nil {
				return err
			}
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
func (req *ListConfigResourcesRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("ListConfigResourcesRequest.Read: request or its body is nil")
	}

	*req = ListConfigResourcesRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// ResourceTypes (versions: 1+)
	if req.ApiVersion >= 1 {
		if isRequestFlexible(req.ApiVersion) {
			resourcetypes, err := protocol.ReadCompactArray(r, protocol.ReadInt8)
			if err != nil {
				return err
			}
			req.ResourceTypes = &resourcetypes
		} else {
			resourcetypes, err := protocol.ReadArray(r, protocol.ReadInt8)
			if err != nil {
				return err
			}
			req.ResourceTypes = &resourcetypes
		}
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

//goland:noinspection GoUnhandledErrorResult
func (req *ListConfigResourcesRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ListConfigResourcesRequest:\n")

	if req.ResourceTypes != nil {
		fmt.Fprintf(w, "        ResourceTypes: %v\n", *req.ResourceTypes)
	} else {
		fmt.Fprintf(w, "        ResourceTypes: nil\n")
	}

	return w.String()
}
