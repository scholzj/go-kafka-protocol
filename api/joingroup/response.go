package joingroup

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type JoinGroupResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                      // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 2+)
	ErrorCode       int16                      // The error code, or 0 if there was no error. (versions: 0+)
	GenerationId    int32                      // The generation ID of the group. (versions: 0+)
	ProtocolType    *string                    // The group protocol name. (versions: 7+, nullable: 7+)
	ProtocolName    *string                    // The group protocol selected by the coordinator. (versions: 0+, nullable: 7+)
	Leader          *string                    // The leader of the group. (versions: 0+)
	SkipAssignment  bool                       // True if the leader must skip running the assignment. (versions: 9+)
	MemberId        *string                    // The member ID assigned by the group coordinator. (versions: 0+)
	Members         *[]JoinGroupResponseMember // The group members. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type JoinGroupResponseMember struct {
	MemberId        *string // The group member ID. (versions: 0+)
	GroupInstanceId *string // The unique identifier of the consumer instance provided by end user. (versions: 5+, nullable: 5+)
	Metadata        *[]byte // The group member metadata. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 6
}

func (res *JoinGroupResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// GenerationId (versions: 0+)
	if err := protocol.WriteInt32(w, res.GenerationId); err != nil {
		return err
	}

	// ProtocolType (versions: 7+)
	if res.ApiVersion >= 7 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, res.ProtocolType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, res.ProtocolType); err != nil {
				return err
			}
		}
	}

	// ProtocolName (versions: 0+)
	if res.ApiVersion < 7 && res.ProtocolName == nil {
		return fmt.Errorf("JoinGroupResponse.ProtocolName must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, res.ProtocolName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, res.ProtocolName); err != nil {
			return err
		}
	}

	// Leader (versions: 0+)
	if res.Leader == nil {
		return fmt.Errorf("JoinGroupResponse.Leader must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *res.Leader); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *res.Leader); err != nil {
			return err
		}
	}

	// SkipAssignment (versions: 9+)
	if res.ApiVersion >= 9 {
		if err := protocol.WriteBool(w, res.SkipAssignment); err != nil {
			return err
		}
	}

	// MemberId (versions: 0+)
	if res.MemberId == nil {
		return fmt.Errorf("JoinGroupResponse.MemberId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *res.MemberId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *res.MemberId); err != nil {
			return err
		}
	}

	// Members (versions: 0+)
	if res.Members == nil {
		return fmt.Errorf("JoinGroupResponse.Members must not be nil in version %d", res.ApiVersion)
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
func (res *JoinGroupResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("JoinGroupResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 2+)
	if res.ApiVersion >= 2 {
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

	// GenerationId (versions: 0+)
	generationid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.GenerationId = generationid

	// ProtocolType (versions: 7+)
	if res.ApiVersion >= 7 {
		if isResponseFlexible(res.ApiVersion) {
			protocoltype, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			res.ProtocolType = protocoltype
		} else {
			protocoltype, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			res.ProtocolType = protocoltype
		}
	}

	// ProtocolName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		protocolname, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		res.ProtocolName = protocolname
	} else {
		protocolname, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		res.ProtocolName = protocolname
	}

	// Leader (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		leader, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		res.Leader = &leader
	} else {
		leader, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		res.Leader = &leader
	}

	// SkipAssignment (versions: 9+)
	if res.ApiVersion >= 9 {
		skipassignment, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		res.SkipAssignment = skipassignment
	}

	// MemberId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		memberid, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		res.MemberId = &memberid
	} else {
		memberid, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		res.MemberId = &memberid
	}

	// Members (versions: 0+)
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

func (res *JoinGroupResponse) membersEncoder(w io.Writer, value JoinGroupResponseMember) error {
	// MemberId (versions: 0+)
	if value.MemberId == nil {
		return fmt.Errorf("JoinGroupResponseMember.MemberId must not be nil in version %d", res.ApiVersion)
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

	// GroupInstanceId (versions: 5+)
	if res.ApiVersion >= 5 {
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

	// Metadata (versions: 0+)
	if value.Metadata == nil {
		return fmt.Errorf("JoinGroupResponseMember.Metadata must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *value.Metadata); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *value.Metadata); err != nil {
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

func (res *JoinGroupResponse) membersDecoder(r io.Reader) (JoinGroupResponseMember, error) {
	joingroupresponsemember := JoinGroupResponseMember{}

	// MemberId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		memberid, err := protocol.ReadCompactString(r)
		if err != nil {
			return joingroupresponsemember, err
		}
		joingroupresponsemember.MemberId = &memberid
	} else {
		memberid, err := protocol.ReadString(r)
		if err != nil {
			return joingroupresponsemember, err
		}
		joingroupresponsemember.MemberId = &memberid
	}

	// GroupInstanceId (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			groupinstanceid, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return joingroupresponsemember, err
			}
			joingroupresponsemember.GroupInstanceId = groupinstanceid
		} else {
			groupinstanceid, err := protocol.ReadNullableString(r)
			if err != nil {
				return joingroupresponsemember, err
			}
			joingroupresponsemember.GroupInstanceId = groupinstanceid
		}
	}

	// Metadata (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		metadata, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return joingroupresponsemember, err
		}
		joingroupresponsemember.Metadata = &metadata
	} else {
		metadata, err := protocol.ReadBytes(r)
		if err != nil {
			return joingroupresponsemember, err
		}
		joingroupresponsemember.Metadata = &metadata
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return joingroupresponsemember, err
		}
		joingroupresponsemember.rawTaggedFields = &rawTaggedFields
	}

	return joingroupresponsemember, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *JoinGroupResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- JoinGroupResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
	fmt.Fprintf(w, "        GenerationId: %v\n", res.GenerationId)

	if res.ProtocolType != nil {
		fmt.Fprintf(w, "        ProtocolType: %v\n", *res.ProtocolType)
	} else {
		fmt.Fprintf(w, "        ProtocolType: nil\n")
	}

	if res.ProtocolName != nil {
		fmt.Fprintf(w, "        ProtocolName: %v\n", *res.ProtocolName)
	} else {
		fmt.Fprintf(w, "        ProtocolName: nil\n")
	}

	if res.Leader != nil {
		fmt.Fprintf(w, "        Leader: %v\n", *res.Leader)
	} else {
		fmt.Fprintf(w, "        Leader: nil\n")
	}

	fmt.Fprintf(w, "        SkipAssignment: %v\n", res.SkipAssignment)

	if res.MemberId != nil {
		fmt.Fprintf(w, "        MemberId: %v\n", *res.MemberId)
	} else {
		fmt.Fprintf(w, "        MemberId: nil\n")
	}

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
func (value *JoinGroupResponseMember) PrettyPrint() string {
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

	if value.Metadata != nil {
		fmt.Fprintf(w, "            Metadata: <%d bytes>\n", len(*value.Metadata))
	} else {
		fmt.Fprintf(w, "            Metadata: nil\n")
	}

	return w.String()
}
