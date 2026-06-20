package deleteacls

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DeleteAclsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                             // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	FilterResults   *[]DeleteAclsResponseFilterResult // The results for each filter. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DeleteAclsResponseFilterResult struct {
	ErrorCode       int16                                        // The error code, or 0 if the filter succeeded. (versions: 0+)
	ErrorMessage    *string                                      // The error message, or null if the filter succeeded. (versions: 0+, nullable: 0+)
	MatchingAcls    *[]DeleteAclsResponseFilterResultMatchingAcl // The ACLs which matched this filter. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DeleteAclsResponseFilterResultMatchingAcl struct {
	ErrorCode       int16   // The deletion error code, or 0 if the deletion succeeded. (versions: 0+)
	ErrorMessage    *string // The deletion error message, or null if the deletion succeeded. (versions: 0+, nullable: 0+)
	ResourceType    int8    // The ACL resource type. (versions: 0+)
	ResourceName    *string // The ACL resource name. (versions: 0+)
	PatternType     int8    // The ACL resource pattern type. (versions: 1+)
	Principal       *string // The ACL principal. (versions: 0+)
	Host            *string // The ACL host. (versions: 0+)
	Operation       int8    // The ACL operation. (versions: 0+)
	PermissionType  int8    // The ACL permission type. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (res *DeleteAclsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// FilterResults (versions: 0+)
	if res.FilterResults == nil {
		return fmt.Errorf("DeleteAclsResponse.FilterResults must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.filterResultsEncoder, res.FilterResults); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.filterResultsEncoder, *res.FilterResults); err != nil {
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
func (res *DeleteAclsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DeleteAclsResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// FilterResults (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		filterresults, err := protocol.ReadNullableCompactArray(r, res.filterResultsDecoder)
		if err != nil {
			return err
		}
		res.FilterResults = filterresults
	} else {
		filterresults, err := protocol.ReadArray(r, res.filterResultsDecoder)
		if err != nil {
			return err
		}
		res.FilterResults = &filterresults
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

func (res *DeleteAclsResponse) filterResultsEncoder(w io.Writer, value DeleteAclsResponseFilterResult) error {
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

	// MatchingAcls (versions: 0+)
	if value.MatchingAcls == nil {
		return fmt.Errorf("DeleteAclsResponseFilterResult.MatchingAcls must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.matchingAclsEncoder, value.MatchingAcls); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.matchingAclsEncoder, *value.MatchingAcls); err != nil {
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

func (res *DeleteAclsResponse) filterResultsDecoder(r io.Reader) (DeleteAclsResponseFilterResult, error) {
	deleteaclsresponsefilterresult := DeleteAclsResponseFilterResult{}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return deleteaclsresponsefilterresult, err
	}
	deleteaclsresponsefilterresult.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return deleteaclsresponsefilterresult, err
		}
		deleteaclsresponsefilterresult.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return deleteaclsresponsefilterresult, err
		}
		deleteaclsresponsefilterresult.ErrorMessage = errormessage
	}

	// MatchingAcls (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		matchingacls, err := protocol.ReadNullableCompactArray(r, res.matchingAclsDecoder)
		if err != nil {
			return deleteaclsresponsefilterresult, err
		}
		deleteaclsresponsefilterresult.MatchingAcls = matchingacls
	} else {
		matchingacls, err := protocol.ReadArray(r, res.matchingAclsDecoder)
		if err != nil {
			return deleteaclsresponsefilterresult, err
		}
		deleteaclsresponsefilterresult.MatchingAcls = &matchingacls
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return deleteaclsresponsefilterresult, err
		}
		deleteaclsresponsefilterresult.rawTaggedFields = &rawTaggedFields
	}

	return deleteaclsresponsefilterresult, nil
}

func (res *DeleteAclsResponse) matchingAclsEncoder(w io.Writer, value DeleteAclsResponseFilterResultMatchingAcl) error {
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
		return fmt.Errorf("DeleteAclsResponseFilterResultMatchingAcl.ResourceName must not be nil in version %d", res.ApiVersion)
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

	// Principal (versions: 0+)
	if value.Principal == nil {
		return fmt.Errorf("DeleteAclsResponseFilterResultMatchingAcl.Principal must not be nil in version %d", res.ApiVersion)
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
		return fmt.Errorf("DeleteAclsResponseFilterResultMatchingAcl.Host must not be nil in version %d", res.ApiVersion)
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

func (res *DeleteAclsResponse) matchingAclsDecoder(r io.Reader) (DeleteAclsResponseFilterResultMatchingAcl, error) {
	deleteaclsresponsefilterresultmatchingacl := DeleteAclsResponseFilterResultMatchingAcl{}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return deleteaclsresponsefilterresultmatchingacl, err
	}
	deleteaclsresponsefilterresultmatchingacl.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return deleteaclsresponsefilterresultmatchingacl, err
		}
		deleteaclsresponsefilterresultmatchingacl.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return deleteaclsresponsefilterresultmatchingacl, err
		}
		deleteaclsresponsefilterresultmatchingacl.ErrorMessage = errormessage
	}

	// ResourceType (versions: 0+)
	resourcetype, err := protocol.ReadInt8(r)
	if err != nil {
		return deleteaclsresponsefilterresultmatchingacl, err
	}
	deleteaclsresponsefilterresultmatchingacl.ResourceType = resourcetype

	// ResourceName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		resourcename, err := protocol.ReadCompactString(r)
		if err != nil {
			return deleteaclsresponsefilterresultmatchingacl, err
		}
		deleteaclsresponsefilterresultmatchingacl.ResourceName = &resourcename
	} else {
		resourcename, err := protocol.ReadString(r)
		if err != nil {
			return deleteaclsresponsefilterresultmatchingacl, err
		}
		deleteaclsresponsefilterresultmatchingacl.ResourceName = &resourcename
	}

	// PatternType (versions: 1+)
	if res.ApiVersion >= 1 {
		patterntype, err := protocol.ReadInt8(r)
		if err != nil {
			return deleteaclsresponsefilterresultmatchingacl, err
		}
		deleteaclsresponsefilterresultmatchingacl.PatternType = patterntype
	}

	// Principal (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		principal, err := protocol.ReadCompactString(r)
		if err != nil {
			return deleteaclsresponsefilterresultmatchingacl, err
		}
		deleteaclsresponsefilterresultmatchingacl.Principal = &principal
	} else {
		principal, err := protocol.ReadString(r)
		if err != nil {
			return deleteaclsresponsefilterresultmatchingacl, err
		}
		deleteaclsresponsefilterresultmatchingacl.Principal = &principal
	}

	// Host (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return deleteaclsresponsefilterresultmatchingacl, err
		}
		deleteaclsresponsefilterresultmatchingacl.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return deleteaclsresponsefilterresultmatchingacl, err
		}
		deleteaclsresponsefilterresultmatchingacl.Host = &host
	}

	// Operation (versions: 0+)
	operation, err := protocol.ReadInt8(r)
	if err != nil {
		return deleteaclsresponsefilterresultmatchingacl, err
	}
	deleteaclsresponsefilterresultmatchingacl.Operation = operation

	// PermissionType (versions: 0+)
	permissiontype, err := protocol.ReadInt8(r)
	if err != nil {
		return deleteaclsresponsefilterresultmatchingacl, err
	}
	deleteaclsresponsefilterresultmatchingacl.PermissionType = permissiontype

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return deleteaclsresponsefilterresultmatchingacl, err
		}
		deleteaclsresponsefilterresultmatchingacl.rawTaggedFields = &rawTaggedFields
	}

	return deleteaclsresponsefilterresultmatchingacl, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DeleteAclsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DeleteAclsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	if res.FilterResults != nil {
		fmt.Fprintf(w, "        FilterResults:\n")
		for _, filterresults := range *res.FilterResults {
			fmt.Fprintf(w, "%s", filterresults.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        FilterResults: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DeleteAclsResponseFilterResult) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	if value.MatchingAcls != nil {
		fmt.Fprintf(w, "            MatchingAcls:\n")
		for _, matchingacls := range *value.MatchingAcls {
			fmt.Fprintf(w, "%s", matchingacls.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            MatchingAcls: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DeleteAclsResponseFilterResultMatchingAcl) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                ErrorMessage: nil\n")
	}

	fmt.Fprintf(w, "                ResourceType: %v\n", value.ResourceType)

	if value.ResourceName != nil {
		fmt.Fprintf(w, "                ResourceName: %v\n", *value.ResourceName)
	} else {
		fmt.Fprintf(w, "                ResourceName: nil\n")
	}

	fmt.Fprintf(w, "                PatternType: %v\n", value.PatternType)

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
