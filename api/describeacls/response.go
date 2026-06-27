package describeacls

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeAclsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                           // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                           // The error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage    *string                         // The error message, or null if there was no error. (versions: 0+, nullable: 0+)
	Resources       *[]DescribeAclsResponseResource // Each Resource that is referenced in an ACL. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeAclsResponseResource struct {
	ResourceType    int8                               // The resource type. (versions: 0+)
	ResourceName    *string                            // The resource name. (versions: 0+)
	PatternType     int8                               // The resource pattern type. (versions: 1+)
	Acls            *[]DescribeAclsResponseResourceAcl // The ACLs. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeAclsResponseResourceAcl struct {
	Principal       *string // The ACL principal. (versions: 0+)
	Host            *string // The ACL host. (versions: 0+)
	Operation       int8    // The ACL operation. (versions: 0+)
	PermissionType  int8    // The ACL permission type. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (res *DescribeAclsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, res.ErrorMessage); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, res.ErrorMessage); err != nil {
			return err
		}
	}

	// Resources (versions: 0+)
	if res.Resources == nil {
		return fmt.Errorf("DescribeAclsResponse.Resources must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.resourcesEncoder, res.Resources); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.resourcesEncoder, *res.Resources); err != nil {
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
func (res *DescribeAclsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeAclsResponse.Read: response or its body is nil")
	}

	*res = DescribeAclsResponse{}

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

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		res.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		res.ErrorMessage = errormessage
	}

	// Resources (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		resources, err := protocol.ReadCompactArray(r, res.resourcesDecoder)
		if err != nil {
			return err
		}
		res.Resources = &resources
	} else {
		resources, err := protocol.ReadArray(r, res.resourcesDecoder)
		if err != nil {
			return err
		}
		res.Resources = &resources
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

func (res *DescribeAclsResponse) resourcesEncoder(w io.Writer, value DescribeAclsResponseResource) error {
	// ResourceType (versions: 0+)
	if err := protocol.WriteInt8(w, value.ResourceType); err != nil {
		return err
	}

	// ResourceName (versions: 0+)
	if value.ResourceName == nil {
		return fmt.Errorf("DescribeAclsResponseResource.ResourceName must not be nil in version %d", res.ApiVersion)
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

	// PatternType (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, value.PatternType); err != nil {
			return err
		}
	}

	// Acls (versions: 0+)
	if value.Acls == nil {
		return fmt.Errorf("DescribeAclsResponseResource.Acls must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.aclsEncoder, value.Acls); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.aclsEncoder, *value.Acls); err != nil {
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

func (res *DescribeAclsResponse) resourcesDecoder(r io.Reader) (DescribeAclsResponseResource, error) {
	describeaclsresponseresource := DescribeAclsResponseResource{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	describeaclsresponseresource.PatternType = 3

	// ResourceType (versions: 0+)
	resourcetype, err := protocol.ReadInt8(r)
	if err != nil {
		return describeaclsresponseresource, err
	}
	describeaclsresponseresource.ResourceType = resourcetype

	// ResourceName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		resourcename, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeaclsresponseresource, err
		}
		describeaclsresponseresource.ResourceName = &resourcename
	} else {
		resourcename, err := protocol.ReadString(r)
		if err != nil {
			return describeaclsresponseresource, err
		}
		describeaclsresponseresource.ResourceName = &resourcename
	}

	// PatternType (versions: 1+)
	if res.ApiVersion >= 1 {
		patterntype, err := protocol.ReadInt8(r)
		if err != nil {
			return describeaclsresponseresource, err
		}
		describeaclsresponseresource.PatternType = patterntype
	}

	// Acls (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		acls, err := protocol.ReadCompactArray(r, res.aclsDecoder)
		if err != nil {
			return describeaclsresponseresource, err
		}
		describeaclsresponseresource.Acls = &acls
	} else {
		acls, err := protocol.ReadArray(r, res.aclsDecoder)
		if err != nil {
			return describeaclsresponseresource, err
		}
		describeaclsresponseresource.Acls = &acls
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeaclsresponseresource, err
		}
		describeaclsresponseresource.rawTaggedFields = &rawTaggedFields
	}

	return describeaclsresponseresource, nil
}

func (res *DescribeAclsResponse) aclsEncoder(w io.Writer, value DescribeAclsResponseResourceAcl) error {
	// Principal (versions: 0+)
	if value.Principal == nil {
		return fmt.Errorf("DescribeAclsResponseResourceAcl.Principal must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Principal); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Principal); err != nil {
			return err
		}
	}

	// Host (versions: 0+)
	if value.Host == nil {
		return fmt.Errorf("DescribeAclsResponseResourceAcl.Host must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Host); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Host); err != nil {
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

func (res *DescribeAclsResponse) aclsDecoder(r io.Reader) (DescribeAclsResponseResourceAcl, error) {
	describeaclsresponseresourceacl := DescribeAclsResponseResourceAcl{}

	// Principal (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		principal, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeaclsresponseresourceacl, err
		}
		describeaclsresponseresourceacl.Principal = &principal
	} else {
		principal, err := protocol.ReadString(r)
		if err != nil {
			return describeaclsresponseresourceacl, err
		}
		describeaclsresponseresourceacl.Principal = &principal
	}

	// Host (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeaclsresponseresourceacl, err
		}
		describeaclsresponseresourceacl.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return describeaclsresponseresourceacl, err
		}
		describeaclsresponseresourceacl.Host = &host
	}

	// Operation (versions: 0+)
	operation, err := protocol.ReadInt8(r)
	if err != nil {
		return describeaclsresponseresourceacl, err
	}
	describeaclsresponseresourceacl.Operation = operation

	// PermissionType (versions: 0+)
	permissiontype, err := protocol.ReadInt8(r)
	if err != nil {
		return describeaclsresponseresourceacl, err
	}
	describeaclsresponseresourceacl.PermissionType = permissiontype

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeaclsresponseresourceacl, err
		}
		describeaclsresponseresourceacl.rawTaggedFields = &rawTaggedFields
	}

	return describeaclsresponseresourceacl, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeAclsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeAclsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.ErrorMessage != nil {
		fmt.Fprintf(w, "        ErrorMessage: %v\n", *res.ErrorMessage)
	} else {
		fmt.Fprintf(w, "        ErrorMessage: nil\n")
	}

	if res.Resources != nil {
		fmt.Fprintf(w, "        Resources:\n")
		for _, resources := range *res.Resources {
			fmt.Fprintf(w, "%s", resources.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Resources: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeAclsResponseResource) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ResourceType: %v\n", value.ResourceType)

	if value.ResourceName != nil {
		fmt.Fprintf(w, "            ResourceName: %v\n", *value.ResourceName)
	} else {
		fmt.Fprintf(w, "            ResourceName: nil\n")
	}

	fmt.Fprintf(w, "            PatternType: %v\n", value.PatternType)

	if value.Acls != nil {
		fmt.Fprintf(w, "            Acls:\n")
		for _, acls := range *value.Acls {
			fmt.Fprintf(w, "%s", acls.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Acls: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeAclsResponseResourceAcl) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Principal != nil {
		fmt.Fprintf(w, "                Principal: %v\n", *value.Principal)
	} else {
		fmt.Fprintf(w, "                Principal: nil\n")
	}

	if value.Host != nil {
		fmt.Fprintf(w, "                Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "                Host: nil\n")
	}

	fmt.Fprintf(w, "                Operation: %v\n", value.Operation)
	fmt.Fprintf(w, "                PermissionType: %v\n", value.PermissionType)

	return w.String()
}
