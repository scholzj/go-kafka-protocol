package leavegroup

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type LeaveGroupRequest struct {
	ApiVersion      int16
	GroupId         *string                    // The ID of the group to leave. (versions: 0+)
	MemberId        *string                    // The member ID to remove from the group. (versions: 0-2)
	Members         *[]LeaveGroupRequestMember // List of leaving member identities. (versions: 3+)
	rawTaggedFields *[]protocol.TaggedField
}

type LeaveGroupRequestMember struct {
	MemberId        *string // The member ID to remove from the group. (versions: 3+)
	GroupInstanceId *string // The group instance ID to remove from the group. (versions: 3+, nullable: 3+)
	Reason          *string // The reason why the member left the group. (versions: 5+, nullable: 5+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 4
}

func (req *LeaveGroupRequest) Write(w io.Writer) error {
	// GroupId (versions: 0+)
	if req.GroupId == nil {
		return fmt.Errorf("LeaveGroupRequest.GroupId must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *req.GroupId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *req.GroupId); err != nil {
			return err
		}
	}

	// MemberId (versions: 0-2)
	if req.ApiVersion <= 2 {
		if req.MemberId == nil {
			return fmt.Errorf("LeaveGroupRequest.MemberId must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteCompactString(w, *req.MemberId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *req.MemberId); err != nil {
				return err
			}
		}
	}

	// Members (versions: 3+)
	if req.ApiVersion >= 3 {
		if req.Members == nil {
			return fmt.Errorf("LeaveGroupRequest.Members must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, req.membersEncoder, req.Members); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, req.membersEncoder, *req.Members); err != nil {
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
func (req *LeaveGroupRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("LeaveGroupRequest.Read: request or its body is nil")
	}

	*req = LeaveGroupRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// GroupId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		groupid, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		req.GroupId = &groupid
	} else {
		groupid, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		req.GroupId = &groupid
	}

	// MemberId (versions: 0-2)
	if req.ApiVersion <= 2 {
		if isRequestFlexible(req.ApiVersion) {
			memberid, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			req.MemberId = &memberid
		} else {
			memberid, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			req.MemberId = &memberid
		}
	}

	// Members (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			members, err := protocol.ReadCompactArray(r, req.membersDecoder)
			if err != nil {
				return err
			}
			req.Members = &members
		} else {
			members, err := protocol.ReadArray(r, req.membersDecoder)
			if err != nil {
				return err
			}
			req.Members = &members
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

func (req *LeaveGroupRequest) membersEncoder(w io.Writer, value LeaveGroupRequestMember) error {
	// MemberId (versions: 3+)
	if req.ApiVersion >= 3 {
		if value.MemberId == nil {
			return fmt.Errorf("LeaveGroupRequestMember.MemberId must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.MemberId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.MemberId); err != nil {
				return err
			}
		}
	}

	// GroupInstanceId (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.GroupInstanceId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.GroupInstanceId); err != nil {
				return err
			}
		}
	}

	// Reason (versions: 5+)
	if req.ApiVersion >= 5 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.Reason); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.Reason); err != nil {
				return err
			}
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

func (req *LeaveGroupRequest) membersDecoder(r io.Reader) (LeaveGroupRequestMember, error) {
	leavegrouprequestmember := LeaveGroupRequestMember{}

	// MemberId (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			memberid, err := protocol.ReadCompactString(r)
			if err != nil {
				return leavegrouprequestmember, err
			}
			leavegrouprequestmember.MemberId = &memberid
		} else {
			memberid, err := protocol.ReadString(r)
			if err != nil {
				return leavegrouprequestmember, err
			}
			leavegrouprequestmember.MemberId = &memberid
		}
	}

	// GroupInstanceId (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			groupinstanceid, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return leavegrouprequestmember, err
			}
			leavegrouprequestmember.GroupInstanceId = groupinstanceid
		} else {
			groupinstanceid, err := protocol.ReadNullableString(r)
			if err != nil {
				return leavegrouprequestmember, err
			}
			leavegrouprequestmember.GroupInstanceId = groupinstanceid
		}
	}

	// Reason (versions: 5+)
	if req.ApiVersion >= 5 {
		if isRequestFlexible(req.ApiVersion) {
			reason, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return leavegrouprequestmember, err
			}
			leavegrouprequestmember.Reason = reason
		} else {
			reason, err := protocol.ReadNullableString(r)
			if err != nil {
				return leavegrouprequestmember, err
			}
			leavegrouprequestmember.Reason = reason
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return leavegrouprequestmember, err
		}
		leavegrouprequestmember.rawTaggedFields = &rawTaggedFields
	}

	return leavegrouprequestmember, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *LeaveGroupRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> LeaveGroupRequest:\n")

	if req.GroupId != nil {
		fmt.Fprintf(w, "        GroupId: %v\n", *req.GroupId)
	} else {
		fmt.Fprintf(w, "        GroupId: nil\n")
	}

	if req.MemberId != nil {
		fmt.Fprintf(w, "        MemberId: %v\n", *req.MemberId)
	} else {
		fmt.Fprintf(w, "        MemberId: nil\n")
	}

	if req.Members != nil {
		fmt.Fprintf(w, "        Members:\n")
		for _, members := range *req.Members {
			fmt.Fprintf(w, "%s", members.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Members: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *LeaveGroupRequestMember) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.MemberId != nil {
		fmt.Fprintf(w, "            MemberId: %v\n", *value.MemberId)
	} else {
		fmt.Fprintf(w, "            MemberId: nil\n")
	}

	if value.GroupInstanceId != nil {
		fmt.Fprintf(w, "            GroupInstanceId: %v\n", *value.GroupInstanceId)
	} else {
		fmt.Fprintf(w, "            GroupInstanceId: nil\n")
	}

	if value.Reason != nil {
		fmt.Fprintf(w, "            Reason: %v\n", *value.Reason)
	} else {
		fmt.Fprintf(w, "            Reason: nil\n")
	}

	return w.String()
}
