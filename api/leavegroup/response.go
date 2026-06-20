package leavegroup

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type LeaveGroupResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                       // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 1+)
	ErrorCode       int16                       // The error code, or 0 if there was no error. (versions: 0+)
	Members         *[]LeaveGroupResponseMember // List of leaving member responses. (versions: 3+)
	rawTaggedFields *[]protocol.TaggedField
}

type LeaveGroupResponseMember struct {
	MemberId        *string // The member ID to remove from the group. (versions: 3+)
	GroupInstanceId *string // The group instance ID to remove from the group. (versions: 3+, nullable: 3+)
	ErrorCode       int16   // The error code, or 0 if there was no error. (versions: 3+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 4
}

func (res *LeaveGroupResponse) Write(w io.Writer) error {
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

	// Members (versions: 3+)
	if res.ApiVersion >= 3 {
		if res.Members == nil {
			return fmt.Errorf("LeaveGroupResponse.Members must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.membersEncoder, res.Members); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, res.membersEncoder, *res.Members); err != nil {
				return err
			}
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
func (res *LeaveGroupResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("LeaveGroupResponse.Read: response or its body is nil")
	}

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

	// Members (versions: 3+)
	if res.ApiVersion >= 3 {
		if isResponseFlexible(res.ApiVersion) {
			members, err := protocol.ReadNullableCompactArray(r, res.membersDecoder)
			if err != nil {
				return err
			}
			res.Members = members
		} else {
			members, err := protocol.ReadArray(r, res.membersDecoder)
			if err != nil {
				return err
			}
			res.Members = &members
		}
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

func (res *LeaveGroupResponse) membersEncoder(w io.Writer, value LeaveGroupResponseMember) error {
	// MemberId (versions: 3+)
	if res.ApiVersion >= 3 {
		if value.MemberId == nil {
			return fmt.Errorf("LeaveGroupResponseMember.MemberId must not be nil in version %d", res.ApiVersion)
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
	}

	// GroupInstanceId (versions: 3+)
	if res.ApiVersion >= 3 {
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

	// ErrorCode (versions: 3+)
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
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

func (res *LeaveGroupResponse) membersDecoder(r io.Reader) (LeaveGroupResponseMember, error) {
	leavegroupresponsemember := LeaveGroupResponseMember{}

	// MemberId (versions: 3+)
	if res.ApiVersion >= 3 {
		if isResponseFlexible(res.ApiVersion) {
			memberid, err := protocol.ReadCompactString(r)
			if err != nil {
				return leavegroupresponsemember, err
			}
			leavegroupresponsemember.MemberId = &memberid
		} else {
			memberid, err := protocol.ReadString(r)
			if err != nil {
				return leavegroupresponsemember, err
			}
			leavegroupresponsemember.MemberId = &memberid
		}
	}

	// GroupInstanceId (versions: 3+)
	if res.ApiVersion >= 3 {
		if isResponseFlexible(res.ApiVersion) {
			groupinstanceid, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return leavegroupresponsemember, err
			}
			leavegroupresponsemember.GroupInstanceId = groupinstanceid
		} else {
			groupinstanceid, err := protocol.ReadNullableString(r)
			if err != nil {
				return leavegroupresponsemember, err
			}
			leavegroupresponsemember.GroupInstanceId = groupinstanceid
		}
	}

	// ErrorCode (versions: 3+)
	if res.ApiVersion >= 3 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return leavegroupresponsemember, err
		}
		leavegroupresponsemember.ErrorCode = errorcode
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return leavegroupresponsemember, err
		}
		leavegroupresponsemember.rawTaggedFields = &rawTaggedFields
	}

	return leavegroupresponsemember, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *LeaveGroupResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- LeaveGroupResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.Members != nil {
		fmt.Fprintf(w, "        Members:\n")
		for _, members := range *res.Members {
			fmt.Fprintf(w, "%s", members.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Members: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *LeaveGroupResponseMember) PrettyPrint() string {
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

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	return w.String()
}
