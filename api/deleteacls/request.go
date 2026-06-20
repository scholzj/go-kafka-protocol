package deleteacls

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DeleteAclsRequest struct {
	ApiVersion      int16
	Filters         *[]DeleteAclsRequestFilter // The filters to use when deleting ACLs. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DeleteAclsRequestFilter struct {
	ResourceTypeFilter int8    // The resource type. (versions: 0+)
	ResourceNameFilter *string // The resource name, or null to match any resource name. (versions: 0+, nullable: 0+)
	PatternTypeFilter  int8    // The pattern type. (versions: 1+)
	PrincipalFilter    *string // The principal filter, or null to accept all principals. (versions: 0+, nullable: 0+)
	HostFilter         *string // The host filter, or null to accept all hosts. (versions: 0+, nullable: 0+)
	Operation          int8    // The ACL operation. (versions: 0+)
	PermissionType     int8    // The permission type. (versions: 0+)
	rawTaggedFields    *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *DeleteAclsRequest) Write(w io.Writer) error {
	// Filters (versions: 0+)
	if req.Filters == nil {
		return fmt.Errorf("DeleteAclsRequest.Filters must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.filtersEncoder, req.Filters); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.filtersEncoder, *req.Filters); err != nil {
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
func (req *DeleteAclsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DeleteAclsRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Filters (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		filters, err := protocol.ReadNullableCompactArray(r, req.filtersDecoder)
		if err != nil {
			return err
		}
		req.Filters = filters
	} else {
		filters, err := protocol.ReadArray(r, req.filtersDecoder)
		if err != nil {
			return err
		}
		req.Filters = &filters
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

func (req *DeleteAclsRequest) filtersEncoder(w io.Writer, value DeleteAclsRequestFilter) error {
	// ResourceTypeFilter (versions: 0+)
	if err := protocol.WriteInt8(w, value.ResourceTypeFilter); err != nil {
		return err
	}

	// ResourceNameFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.ResourceNameFilter); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.ResourceNameFilter); err != nil {
			return err
		}
	}

	// PatternTypeFilter (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, value.PatternTypeFilter); err != nil {
			return err
		}
	}

	// PrincipalFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.PrincipalFilter); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.PrincipalFilter); err != nil {
			return err
		}
	}

	// HostFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.HostFilter); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.HostFilter); err != nil {
			return err
		}
	}

	// Operation (versions: 0+)
	if err := protocol.WriteInt8(w, value.Operation); err != nil {
		return err
	}

	// PermissionType (versions: 0+)
	if err := protocol.WriteInt8(w, value.PermissionType); err != nil {
		return err
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

func (req *DeleteAclsRequest) filtersDecoder(r io.Reader) (DeleteAclsRequestFilter, error) {
	deleteaclsrequestfilter := DeleteAclsRequestFilter{}

	// ResourceTypeFilter (versions: 0+)
	resourcetypefilter, err := protocol.ReadInt8(r)
	if err != nil {
		return deleteaclsrequestfilter, err
	}
	deleteaclsrequestfilter.ResourceTypeFilter = resourcetypefilter

	// ResourceNameFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		resourcenamefilter, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return deleteaclsrequestfilter, err
		}
		deleteaclsrequestfilter.ResourceNameFilter = resourcenamefilter
	} else {
		resourcenamefilter, err := protocol.ReadNullableString(r)
		if err != nil {
			return deleteaclsrequestfilter, err
		}
		deleteaclsrequestfilter.ResourceNameFilter = resourcenamefilter
	}

	// PatternTypeFilter (versions: 1+)
	if req.ApiVersion >= 1 {
		patterntypefilter, err := protocol.ReadInt8(r)
		if err != nil {
			return deleteaclsrequestfilter, err
		}
		deleteaclsrequestfilter.PatternTypeFilter = patterntypefilter
	}

	// PrincipalFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		principalfilter, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return deleteaclsrequestfilter, err
		}
		deleteaclsrequestfilter.PrincipalFilter = principalfilter
	} else {
		principalfilter, err := protocol.ReadNullableString(r)
		if err != nil {
			return deleteaclsrequestfilter, err
		}
		deleteaclsrequestfilter.PrincipalFilter = principalfilter
	}

	// HostFilter (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		hostfilter, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return deleteaclsrequestfilter, err
		}
		deleteaclsrequestfilter.HostFilter = hostfilter
	} else {
		hostfilter, err := protocol.ReadNullableString(r)
		if err != nil {
			return deleteaclsrequestfilter, err
		}
		deleteaclsrequestfilter.HostFilter = hostfilter
	}

	// Operation (versions: 0+)
	operation, err := protocol.ReadInt8(r)
	if err != nil {
		return deleteaclsrequestfilter, err
	}
	deleteaclsrequestfilter.Operation = operation

	// PermissionType (versions: 0+)
	permissiontype, err := protocol.ReadInt8(r)
	if err != nil {
		return deleteaclsrequestfilter, err
	}
	deleteaclsrequestfilter.PermissionType = permissiontype

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return deleteaclsrequestfilter, err
		}
		deleteaclsrequestfilter.rawTaggedFields = &rawTaggedFields
	}

	return deleteaclsrequestfilter, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DeleteAclsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DeleteAclsRequest:\n")

	if req.Filters != nil {
		fmt.Fprintf(w, "        Filters:\n")
		for _, filters := range *req.Filters {
			fmt.Fprintf(w, "%s", filters.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Filters: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DeleteAclsRequestFilter) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ResourceTypeFilter: %v\n", value.ResourceTypeFilter)

	if value.ResourceNameFilter != nil {
		fmt.Fprintf(w, "            ResourceNameFilter: %v\n", *value.ResourceNameFilter)
	} else {
		fmt.Fprintf(w, "            ResourceNameFilter: nil\n")
	}

	fmt.Fprintf(w, "            PatternTypeFilter: %v\n", value.PatternTypeFilter)

	if value.PrincipalFilter != nil {
		fmt.Fprintf(w, "            PrincipalFilter: %v\n", *value.PrincipalFilter)
	} else {
		fmt.Fprintf(w, "            PrincipalFilter: nil\n")
	}

	if value.HostFilter != nil {
		fmt.Fprintf(w, "            HostFilter: %v\n", *value.HostFilter)
	} else {
		fmt.Fprintf(w, "            HostFilter: nil\n")
	}

	fmt.Fprintf(w, "            Operation: %v\n", value.Operation)
	fmt.Fprintf(w, "            PermissionType: %v\n", value.PermissionType)

	return w.String()
}
