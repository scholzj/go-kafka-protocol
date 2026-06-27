package describeacls

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeAclsRequest struct {
	ApiVersion         int16
	ResourceTypeFilter int8    // The resource type. (versions: 0+)
	ResourceNameFilter *string // The resource name, or null to match any resource name. (versions: 0+, nullable: 0+)
	PatternTypeFilter  int8    // The resource pattern to match. (versions: 1+)
	PrincipalFilter    *string // The principal to match, or null to match any principal. (versions: 0+, nullable: 0+)
	HostFilter         *string // The host to match, or null to match any host. (versions: 0+, nullable: 0+)
	Operation          int8    // The operation to match. (versions: 0+)
	PermissionType     int8    // The permission type to match. (versions: 0+)
	rawTaggedFields    *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *DescribeAclsRequest) Write(w io.Writer) error {
	// ResourceTypeFilter (versions: 0+)
	if err := protocol.WriteInt8(w, req.ResourceTypeFilter); err != nil {
		return err
	}

	// ResourceNameFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, req.ResourceNameFilter); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, req.ResourceNameFilter); err != nil {
			return err
		}
	}

	// PatternTypeFilter (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, req.PatternTypeFilter); err != nil {
			return err
		}
	}

	// PrincipalFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, req.PrincipalFilter); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, req.PrincipalFilter); err != nil {
			return err
		}
	}

	// HostFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, req.HostFilter); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, req.HostFilter); err != nil {
			return err
		}
	}

	// Operation (versions: 0+)
	if err := protocol.WriteInt8(w, req.Operation); err != nil {
		return err
	}

	// PermissionType (versions: 0+)
	if err := protocol.WriteInt8(w, req.PermissionType); err != nil {
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
func (req *DescribeAclsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeAclsRequest.Read: request or its body is nil")
	}

	*req = DescribeAclsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.PatternTypeFilter = 3

	// ResourceTypeFilter (versions: 0+)
	resourcetypefilter, err := protocol.ReadInt8(r)
	if err != nil {
		return err
	}
	req.ResourceTypeFilter = resourcetypefilter

	// ResourceNameFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		resourcenamefilter, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.ResourceNameFilter = resourcenamefilter
	} else {
		resourcenamefilter, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		req.ResourceNameFilter = resourcenamefilter
	}

	// PatternTypeFilter (versions: 1+)
	if req.ApiVersion >= 1 {
		patterntypefilter, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		req.PatternTypeFilter = patterntypefilter
	}

	// PrincipalFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		principalfilter, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.PrincipalFilter = principalfilter
	} else {
		principalfilter, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		req.PrincipalFilter = principalfilter
	}

	// HostFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		hostfilter, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.HostFilter = hostfilter
	} else {
		hostfilter, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		req.HostFilter = hostfilter
	}

	// Operation (versions: 0+)
	operation, err := protocol.ReadInt8(r)
	if err != nil {
		return err
	}
	req.Operation = operation

	// PermissionType (versions: 0+)
	permissiontype, err := protocol.ReadInt8(r)
	if err != nil {
		return err
	}
	req.PermissionType = permissiontype

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
func (req *DescribeAclsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeAclsRequest:\n")
	fmt.Fprintf(w, "        ResourceTypeFilter: %v\n", req.ResourceTypeFilter)

	if req.ResourceNameFilter != nil {
		fmt.Fprintf(w, "        ResourceNameFilter: %v\n", *req.ResourceNameFilter)
	} else {
		fmt.Fprintf(w, "        ResourceNameFilter: nil\n")
	}

	fmt.Fprintf(w, "        PatternTypeFilter: %v\n", req.PatternTypeFilter)

	if req.PrincipalFilter != nil {
		fmt.Fprintf(w, "        PrincipalFilter: %v\n", *req.PrincipalFilter)
	} else {
		fmt.Fprintf(w, "        PrincipalFilter: nil\n")
	}

	if req.HostFilter != nil {
		fmt.Fprintf(w, "        HostFilter: %v\n", *req.HostFilter)
	} else {
		fmt.Fprintf(w, "        HostFilter: nil\n")
	}

	fmt.Fprintf(w, "        Operation: %v\n", req.Operation)
	fmt.Fprintf(w, "        PermissionType: %v\n", req.PermissionType)

	return w.String()
}
