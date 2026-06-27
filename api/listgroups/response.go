package listgroups

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ListGroupsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                      // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 1+)
	ErrorCode       int16                      // The error code, or 0 if there was no error. (versions: 0+)
	Groups          *[]ListGroupsResponseGroup // Each group in the response. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ListGroupsResponseGroup struct {
	GroupId         *string // The group ID. (versions: 0+)
	ProtocolType    *string // The group protocol type. (versions: 0+)
	GroupState      *string // The group state name. (versions: 4+)
	GroupType       *string // The group type name. (versions: 5+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (res *ListGroupsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// Groups (versions: 0+)
	if res.Groups == nil {
		return fmt.Errorf("ListGroupsResponse.Groups must not be nil in version %d", res.ApiVersion)
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
func (res *ListGroupsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("ListGroupsResponse.Read: response or its body is nil")
	}

	*res = ListGroupsResponse{}

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

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

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

func (res *ListGroupsResponse) groupsEncoder(w io.Writer, value ListGroupsResponseGroup) error {
	// GroupId (versions: 0+)
	if value.GroupId == nil {
		return fmt.Errorf("ListGroupsResponseGroup.GroupId must not be nil in version %d", res.ApiVersion)
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

	// ProtocolType (versions: 0+)
	if value.ProtocolType == nil {
		return fmt.Errorf("ListGroupsResponseGroup.ProtocolType must not be nil in version %d", res.ApiVersion)
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

	// GroupState (versions: 4+)
	if res.ApiVersion >= 4 {
		if value.GroupState == nil {
			return fmt.Errorf("ListGroupsResponseGroup.GroupState must not be nil in version %d", res.ApiVersion)
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
	}

	// GroupType (versions: 5+)
	if res.ApiVersion >= 5 {
		if value.GroupType == nil {
			return fmt.Errorf("ListGroupsResponseGroup.GroupType must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.GroupType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.GroupType); err != nil {
				return err
			}
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

func (res *ListGroupsResponse) groupsDecoder(r io.Reader) (ListGroupsResponseGroup, error) {
	listgroupsresponsegroup := ListGroupsResponseGroup{}

	// GroupId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		groupid, err := protocol.ReadCompactString(r)
		if err != nil {
			return listgroupsresponsegroup, err
		}
		listgroupsresponsegroup.GroupId = &groupid
	} else {
		groupid, err := protocol.ReadString(r)
		if err != nil {
			return listgroupsresponsegroup, err
		}
		listgroupsresponsegroup.GroupId = &groupid
	}

	// ProtocolType (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		protocoltype, err := protocol.ReadCompactString(r)
		if err != nil {
			return listgroupsresponsegroup, err
		}
		listgroupsresponsegroup.ProtocolType = &protocoltype
	} else {
		protocoltype, err := protocol.ReadString(r)
		if err != nil {
			return listgroupsresponsegroup, err
		}
		listgroupsresponsegroup.ProtocolType = &protocoltype
	}

	// GroupState (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			groupstate, err := protocol.ReadCompactString(r)
			if err != nil {
				return listgroupsresponsegroup, err
			}
			listgroupsresponsegroup.GroupState = &groupstate
		} else {
			groupstate, err := protocol.ReadString(r)
			if err != nil {
				return listgroupsresponsegroup, err
			}
			listgroupsresponsegroup.GroupState = &groupstate
		}
	}

	// GroupType (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			grouptype, err := protocol.ReadCompactString(r)
			if err != nil {
				return listgroupsresponsegroup, err
			}
			listgroupsresponsegroup.GroupType = &grouptype
		} else {
			grouptype, err := protocol.ReadString(r)
			if err != nil {
				return listgroupsresponsegroup, err
			}
			listgroupsresponsegroup.GroupType = &grouptype
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return listgroupsresponsegroup, err
		}
		listgroupsresponsegroup.rawTaggedFields = &rawTaggedFields
	}

	return listgroupsresponsegroup, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ListGroupsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ListGroupsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

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
func (value *ListGroupsResponseGroup) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.GroupId != nil {
		fmt.Fprintf(w, "            GroupId: %v\n", *value.GroupId)
	} else {
		fmt.Fprintf(w, "            GroupId: nil\n")
	}

	if value.ProtocolType != nil {
		fmt.Fprintf(w, "            ProtocolType: %v\n", *value.ProtocolType)
	} else {
		fmt.Fprintf(w, "            ProtocolType: nil\n")
	}

	if value.GroupState != nil {
		fmt.Fprintf(w, "            GroupState: %v\n", *value.GroupState)
	} else {
		fmt.Fprintf(w, "            GroupState: nil\n")
	}

	if value.GroupType != nil {
		fmt.Fprintf(w, "            GroupType: %v\n", *value.GroupType)
	} else {
		fmt.Fprintf(w, "            GroupType: nil\n")
	}

	return w.String()
}
