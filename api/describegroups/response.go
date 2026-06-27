package describegroups

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeGroupsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                          // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 1+)
	Groups          *[]DescribeGroupsResponseGroup // Each described group. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeGroupsResponseGroup struct {
	ErrorCode            int16                                // The describe error, or 0 if there was no error. (versions: 0+)
	ErrorMessage         *string                              // The describe error message, or null if there was no error. (versions: 6+, nullable: 6+)
	GroupId              *string                              // The group ID string. (versions: 0+)
	GroupState           *string                              // The group state string, or the empty string. (versions: 0+)
	ProtocolType         *string                              // The group protocol type, or the empty string. (versions: 0+)
	ProtocolData         *string                              // The group protocol data, or the empty string. (versions: 0+)
	Members              *[]DescribeGroupsResponseGroupMember // The group members. (versions: 0+)
	AuthorizedOperations int32                                // 32-bit bitfield to represent authorized operations for this group. (versions: 3+)
	rawTaggedFields      *[]protocol.TaggedField
}

type DescribeGroupsResponseGroupMember struct {
	MemberId         *string // The member id. (versions: 0+)
	GroupInstanceId  *string // The unique identifier of the consumer instance provided by end user. (versions: 4+, nullable: 4+)
	ClientId         *string // The client ID used in the member's latest join group request. (versions: 0+)
	ClientHost       *string // The client host. (versions: 0+)
	MemberMetadata   *[]byte // The metadata corresponding to the current group protocol in use. (versions: 0+)
	MemberAssignment *[]byte // The current assignment provided by the group leader. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 5
}

func (res *DescribeGroupsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// Groups (versions: 0+)
	if res.Groups == nil {
		return fmt.Errorf("DescribeGroupsResponse.Groups must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.groupsEncoder, res.Groups); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.groupsEncoder, *res.Groups); err != nil {
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
func (res *DescribeGroupsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeGroupsResponse.Read: response or its body is nil")
	}

	*res = DescribeGroupsResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		throttletimems, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttletimems
	}

	// Groups (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		groups, err := protocol.ReadCompactArray(r, res.groupsDecoder)
		if err != nil {
			return err
		}
		res.Groups = &groups
	} else {
		groups, err := protocol.ReadArray(r, res.groupsDecoder)
		if err != nil {
			return err
		}
		res.Groups = &groups
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

func (res *DescribeGroupsResponse) groupsEncoder(w io.Writer, value DescribeGroupsResponseGroup) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 6+)
	if res.ApiVersion >= 6 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.ErrorMessage); err != nil {
				return err
			}
		}
	}

	// GroupId (versions: 0+)
	if value.GroupId == nil {
		return fmt.Errorf("DescribeGroupsResponseGroup.GroupId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.GroupId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.GroupId); err != nil {
			return err
		}
	}

	// GroupState (versions: 0+)
	if value.GroupState == nil {
		return fmt.Errorf("DescribeGroupsResponseGroup.GroupState must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.GroupState); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.GroupState); err != nil {
			return err
		}
	}

	// ProtocolType (versions: 0+)
	if value.ProtocolType == nil {
		return fmt.Errorf("DescribeGroupsResponseGroup.ProtocolType must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.ProtocolType); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.ProtocolType); err != nil {
			return err
		}
	}

	// ProtocolData (versions: 0+)
	if value.ProtocolData == nil {
		return fmt.Errorf("DescribeGroupsResponseGroup.ProtocolData must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.ProtocolData); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.ProtocolData); err != nil {
			return err
		}
	}

	// Members (versions: 0+)
	if value.Members == nil {
		return fmt.Errorf("DescribeGroupsResponseGroup.Members must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.membersEncoder, value.Members); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.membersEncoder, *value.Members); err != nil {
			return err
		}
	}

	// AuthorizedOperations (versions: 3+)
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt32(w, value.AuthorizedOperations); err != nil {
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

func (res *DescribeGroupsResponse) groupsDecoder(r io.Reader) (DescribeGroupsResponseGroup, error) {
	describegroupsresponsegroup := DescribeGroupsResponseGroup{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	describegroupsresponsegroup.AuthorizedOperations = -2147483648

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return describegroupsresponsegroup, err
	}
	describegroupsresponsegroup.ErrorCode = errorcode

	// ErrorMessage (versions: 6+)
	if res.ApiVersion >= 6 {
		if isResponseFlexible(res.ApiVersion) {
			errormessage, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return describegroupsresponsegroup, err
			}
			describegroupsresponsegroup.ErrorMessage = errormessage
		} else {
			errormessage, err := protocol.ReadNullableString(r)
			if err != nil {
				return describegroupsresponsegroup, err
			}
			describegroupsresponsegroup.ErrorMessage = errormessage
		}
	}

	// GroupId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		groupid, err := protocol.ReadCompactString(r)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.GroupId = &groupid
	} else {
		groupid, err := protocol.ReadString(r)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.GroupId = &groupid
	}

	// GroupState (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		groupstate, err := protocol.ReadCompactString(r)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.GroupState = &groupstate
	} else {
		groupstate, err := protocol.ReadString(r)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.GroupState = &groupstate
	}

	// ProtocolType (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		protocoltype, err := protocol.ReadCompactString(r)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.ProtocolType = &protocoltype
	} else {
		protocoltype, err := protocol.ReadString(r)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.ProtocolType = &protocoltype
	}

	// ProtocolData (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		protocoldata, err := protocol.ReadCompactString(r)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.ProtocolData = &protocoldata
	} else {
		protocoldata, err := protocol.ReadString(r)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.ProtocolData = &protocoldata
	}

	// Members (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		members, err := protocol.ReadCompactArray(r, res.membersDecoder)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.Members = &members
	} else {
		members, err := protocol.ReadArray(r, res.membersDecoder)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.Members = &members
	}

	// AuthorizedOperations (versions: 3+)
	if res.ApiVersion >= 3 {
		authorizedoperations, err := protocol.ReadInt32(r)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.AuthorizedOperations = authorizedoperations
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describegroupsresponsegroup, err
		}
		describegroupsresponsegroup.rawTaggedFields = &rawTaggedFields
	}

	return describegroupsresponsegroup, nil
}

func (res *DescribeGroupsResponse) membersEncoder(w io.Writer, value DescribeGroupsResponseGroupMember) error {
	// MemberId (versions: 0+)
	if value.MemberId == nil {
		return fmt.Errorf("DescribeGroupsResponseGroupMember.MemberId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.MemberId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.MemberId); err != nil {
			return err
		}
	}

	// GroupInstanceId (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.GroupInstanceId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.GroupInstanceId); err != nil {
				return err
			}
		}
	}

	// ClientId (versions: 0+)
	if value.ClientId == nil {
		return fmt.Errorf("DescribeGroupsResponseGroupMember.ClientId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.ClientId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.ClientId); err != nil {
			return err
		}
	}

	// ClientHost (versions: 0+)
	if value.ClientHost == nil {
		return fmt.Errorf("DescribeGroupsResponseGroupMember.ClientHost must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.ClientHost); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.ClientHost); err != nil {
			return err
		}
	}

	// MemberMetadata (versions: 0+)
	if value.MemberMetadata == nil {
		return fmt.Errorf("DescribeGroupsResponseGroupMember.MemberMetadata must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *value.MemberMetadata); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *value.MemberMetadata); err != nil {
			return err
		}
	}

	// MemberAssignment (versions: 0+)
	if value.MemberAssignment == nil {
		return fmt.Errorf("DescribeGroupsResponseGroupMember.MemberAssignment must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *value.MemberAssignment); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *value.MemberAssignment); err != nil {
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

func (res *DescribeGroupsResponse) membersDecoder(r io.Reader) (DescribeGroupsResponseGroupMember, error) {
	describegroupsresponsegroupmember := DescribeGroupsResponseGroupMember{}

	// MemberId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		memberid, err := protocol.ReadCompactString(r)
		if err != nil {
			return describegroupsresponsegroupmember, err
		}
		describegroupsresponsegroupmember.MemberId = &memberid
	} else {
		memberid, err := protocol.ReadString(r)
		if err != nil {
			return describegroupsresponsegroupmember, err
		}
		describegroupsresponsegroupmember.MemberId = &memberid
	}

	// GroupInstanceId (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			groupinstanceid, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return describegroupsresponsegroupmember, err
			}
			describegroupsresponsegroupmember.GroupInstanceId = groupinstanceid
		} else {
			groupinstanceid, err := protocol.ReadNullableString(r)
			if err != nil {
				return describegroupsresponsegroupmember, err
			}
			describegroupsresponsegroupmember.GroupInstanceId = groupinstanceid
		}
	}

	// ClientId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		clientid, err := protocol.ReadCompactString(r)
		if err != nil {
			return describegroupsresponsegroupmember, err
		}
		describegroupsresponsegroupmember.ClientId = &clientid
	} else {
		clientid, err := protocol.ReadString(r)
		if err != nil {
			return describegroupsresponsegroupmember, err
		}
		describegroupsresponsegroupmember.ClientId = &clientid
	}

	// ClientHost (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		clienthost, err := protocol.ReadCompactString(r)
		if err != nil {
			return describegroupsresponsegroupmember, err
		}
		describegroupsresponsegroupmember.ClientHost = &clienthost
	} else {
		clienthost, err := protocol.ReadString(r)
		if err != nil {
			return describegroupsresponsegroupmember, err
		}
		describegroupsresponsegroupmember.ClientHost = &clienthost
	}

	// MemberMetadata (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		membermetadata, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return describegroupsresponsegroupmember, err
		}
		describegroupsresponsegroupmember.MemberMetadata = &membermetadata
	} else {
		membermetadata, err := protocol.ReadBytes(r)
		if err != nil {
			return describegroupsresponsegroupmember, err
		}
		describegroupsresponsegroupmember.MemberMetadata = &membermetadata
	}

	// MemberAssignment (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		memberassignment, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return describegroupsresponsegroupmember, err
		}
		describegroupsresponsegroupmember.MemberAssignment = &memberassignment
	} else {
		memberassignment, err := protocol.ReadBytes(r)
		if err != nil {
			return describegroupsresponsegroupmember, err
		}
		describegroupsresponsegroupmember.MemberAssignment = &memberassignment
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describegroupsresponsegroupmember, err
		}
		describegroupsresponsegroupmember.rawTaggedFields = &rawTaggedFields
	}

	return describegroupsresponsegroupmember, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeGroupsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeGroupsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	if res.Groups != nil {
		fmt.Fprintf(w, "        Groups:\n")
		for _, groups := range *res.Groups {
			fmt.Fprintf(w, "%s", groups.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Groups: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeGroupsResponseGroup) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	if value.GroupId != nil {
		fmt.Fprintf(w, "            GroupId: %v\n", *value.GroupId)
	} else {
		fmt.Fprintf(w, "            GroupId: nil\n")
	}

	if value.GroupState != nil {
		fmt.Fprintf(w, "            GroupState: %v\n", *value.GroupState)
	} else {
		fmt.Fprintf(w, "            GroupState: nil\n")
	}

	if value.ProtocolType != nil {
		fmt.Fprintf(w, "            ProtocolType: %v\n", *value.ProtocolType)
	} else {
		fmt.Fprintf(w, "            ProtocolType: nil\n")
	}

	if value.ProtocolData != nil {
		fmt.Fprintf(w, "            ProtocolData: %v\n", *value.ProtocolData)
	} else {
		fmt.Fprintf(w, "            ProtocolData: nil\n")
	}

	if value.Members != nil {
		fmt.Fprintf(w, "            Members:\n")
		for _, members := range *value.Members {
			fmt.Fprintf(w, "%s", members.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Members: nil\n")
	}

	fmt.Fprintf(w, "            AuthorizedOperations: %v\n", value.AuthorizedOperations)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeGroupsResponseGroupMember) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.MemberId != nil {
		fmt.Fprintf(w, "                MemberId: %v\n", *value.MemberId)
	} else {
		fmt.Fprintf(w, "                MemberId: nil\n")
	}

	if value.GroupInstanceId != nil {
		fmt.Fprintf(w, "                GroupInstanceId: %v\n", *value.GroupInstanceId)
	} else {
		fmt.Fprintf(w, "                GroupInstanceId: nil\n")
	}

	if value.ClientId != nil {
		fmt.Fprintf(w, "                ClientId: %v\n", *value.ClientId)
	} else {
		fmt.Fprintf(w, "                ClientId: nil\n")
	}

	if value.ClientHost != nil {
		fmt.Fprintf(w, "                ClientHost: %v\n", *value.ClientHost)
	} else {
		fmt.Fprintf(w, "                ClientHost: nil\n")
	}

	if value.MemberMetadata != nil {
		fmt.Fprintf(w, "                MemberMetadata: <%d bytes>\n", len(*value.MemberMetadata))
	} else {
		fmt.Fprintf(w, "                MemberMetadata: nil\n")
	}

	if value.MemberAssignment != nil {
		fmt.Fprintf(w, "                MemberAssignment: <%d bytes>\n", len(*value.MemberAssignment))
	} else {
		fmt.Fprintf(w, "                MemberAssignment: nil\n")
	}

	return w.String()
}
