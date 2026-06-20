package createacls

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type CreateAclsRequest struct {
	ApiVersion      int16
	Creations       *[]CreateAclsRequestCreation // The ACLs that we want to create. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type CreateAclsRequestCreation struct {
	ResourceType        int8    // The type of the resource. (versions: 0+)
	ResourceName        *string // The resource name for the ACL. (versions: 0+)
	ResourcePatternType int8    // The pattern type for the ACL. (versions: 1+)
	Principal           *string // The principal for the ACL. (versions: 0+)
	Host                *string // The host for the ACL. (versions: 0+)
	Operation           int8    // The operation type for the ACL (read, write, etc.). (versions: 0+)
	PermissionType      int8    // The permission type for the ACL (allow, deny, etc.). (versions: 0+)
	rawTaggedFields     *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *CreateAclsRequest) Write(w io.Writer) error {
	// Creations (versions: 0+)
	if req.Creations == nil {
		return fmt.Errorf("CreateAclsRequest.Creations must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.creationsEncoder, req.Creations); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.creationsEncoder, *req.Creations); err != nil {
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
func (req *CreateAclsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("CreateAclsRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Creations (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		creations, err := protocol.ReadNullableCompactArray(r, req.creationsDecoder)
		if err != nil {
			return err
		}
		req.Creations = creations
	} else {
		creations, err := protocol.ReadArray(r, req.creationsDecoder)
		if err != nil {
			return err
		}
		req.Creations = &creations
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

func (req *CreateAclsRequest) creationsEncoder(w io.Writer, value CreateAclsRequestCreation) error {
	// ResourceType (versions: 0+)
	if err := protocol.WriteInt8(w, value.ResourceType); err != nil {
		return err
	}

	// ResourceName (versions: 0+)
	if value.ResourceName == nil {
		return fmt.Errorf("CreateAclsRequestCreation.ResourceName must not be nil in version %d", req.ApiVersion)
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

	// ResourcePatternType (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, value.ResourcePatternType); err != nil {
			return err
		}
	}

	// Principal (versions: 0+)
	if value.Principal == nil {
		return fmt.Errorf("CreateAclsRequestCreation.Principal must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
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
		return fmt.Errorf("CreateAclsRequestCreation.Host must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
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

func (req *CreateAclsRequest) creationsDecoder(r io.Reader) (CreateAclsRequestCreation, error) {
	createaclsrequestcreation := CreateAclsRequestCreation{}

	// ResourceType (versions: 0+)
	resourcetype, err := protocol.ReadInt8(r)
	if err != nil {
		return createaclsrequestcreation, err
	}
	createaclsrequestcreation.ResourceType = resourcetype

	// ResourceName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		resourcename, err := protocol.ReadCompactString(r)
		if err != nil {
			return createaclsrequestcreation, err
		}
		createaclsrequestcreation.ResourceName = &resourcename
	} else {
		resourcename, err := protocol.ReadString(r)
		if err != nil {
			return createaclsrequestcreation, err
		}
		createaclsrequestcreation.ResourceName = &resourcename
	}

	// ResourcePatternType (versions: 1+)
	if req.ApiVersion >= 1 {
		resourcepatterntype, err := protocol.ReadInt8(r)
		if err != nil {
			return createaclsrequestcreation, err
		}
		createaclsrequestcreation.ResourcePatternType = resourcepatterntype
	}

	// Principal (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		principal, err := protocol.ReadCompactString(r)
		if err != nil {
			return createaclsrequestcreation, err
		}
		createaclsrequestcreation.Principal = &principal
	} else {
		principal, err := protocol.ReadString(r)
		if err != nil {
			return createaclsrequestcreation, err
		}
		createaclsrequestcreation.Principal = &principal
	}

	// Host (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return createaclsrequestcreation, err
		}
		createaclsrequestcreation.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return createaclsrequestcreation, err
		}
		createaclsrequestcreation.Host = &host
	}

	// Operation (versions: 0+)
	operation, err := protocol.ReadInt8(r)
	if err != nil {
		return createaclsrequestcreation, err
	}
	createaclsrequestcreation.Operation = operation

	// PermissionType (versions: 0+)
	permissiontype, err := protocol.ReadInt8(r)
	if err != nil {
		return createaclsrequestcreation, err
	}
	createaclsrequestcreation.PermissionType = permissiontype

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return createaclsrequestcreation, err
		}
		createaclsrequestcreation.rawTaggedFields = &rawTaggedFields
	}

	return createaclsrequestcreation, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *CreateAclsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> CreateAclsRequest:\n")

	if req.Creations != nil {
		fmt.Fprintf(w, "        Creations:\n")
		for _, creations := range *req.Creations {
			fmt.Fprintf(w, "%s", creations.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Creations: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *CreateAclsRequestCreation) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ResourceType: %v\n", value.ResourceType)

	if value.ResourceName != nil {
		fmt.Fprintf(w, "            ResourceName: %v\n", *value.ResourceName)
	} else {
		fmt.Fprintf(w, "            ResourceName: nil\n")
	}

	fmt.Fprintf(w, "            ResourcePatternType: %v\n", value.ResourcePatternType)

	if value.Principal != nil {
		fmt.Fprintf(w, "            Principal: %v\n", *value.Principal)
	} else {
		fmt.Fprintf(w, "            Principal: nil\n")
	}

	if value.Host != nil {
		fmt.Fprintf(w, "            Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "            Host: nil\n")
	}

	fmt.Fprintf(w, "            Operation: %v\n", value.Operation)
	fmt.Fprintf(w, "            PermissionType: %v\n", value.PermissionType)

	return w.String()
}
