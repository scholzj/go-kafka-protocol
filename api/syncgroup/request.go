package syncgroup

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type SyncGroupRequest struct {
	ApiVersion      int16
	GroupId         *string                       // The unique group identifier. (versions: 0+)
	GenerationId    int32                         // The generation of the group. (versions: 0+)
	MemberId        *string                       // The member ID assigned by the group. (versions: 0+)
	GroupInstanceId *string                       // The unique identifier of the consumer instance provided by end user. (versions: 3+, nullable: 3+)
	ProtocolType    *string                       // The group protocol type. (versions: 5+, nullable: 5+)
	ProtocolName    *string                       // The group protocol name. (versions: 5+, nullable: 5+)
	Assignments     *[]SyncGroupRequestAssignment // Each assignment. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type SyncGroupRequestAssignment struct {
	MemberId        *string // The ID of the member to assign. (versions: 0+)
	Assignment      *[]byte // The member assignment. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 4
}

func (req *SyncGroupRequest) Write(w io.Writer) error {
	// GroupId (versions: 0+)
	if req.GroupId == nil {
		return fmt.Errorf("SyncGroupRequest.GroupId must not be nil in version %d", req.ApiVersion)
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

	// GenerationId (versions: 0+)
	if err := protocol.WriteInt32(w, req.GenerationId); err != nil {
		return err
	}

	// MemberId (versions: 0+)
	if req.MemberId == nil {
		return fmt.Errorf("SyncGroupRequest.MemberId must not be nil in version %d", req.ApiVersion)
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

	// GroupInstanceId (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, req.GroupInstanceId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, req.GroupInstanceId); err != nil {
				return err
			}
		}
	}

	// ProtocolType (versions: 5+)
	if req.ApiVersion >= 5 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, req.ProtocolType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, req.ProtocolType); err != nil {
				return err
			}
		}
	}

	// ProtocolName (versions: 5+)
	if req.ApiVersion >= 5 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, req.ProtocolName); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, req.ProtocolName); err != nil {
				return err
			}
		}
	}

	// Assignments (versions: 0+)
	if req.Assignments == nil {
		return fmt.Errorf("SyncGroupRequest.Assignments must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.assignmentsEncoder, req.Assignments); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.assignmentsEncoder, *req.Assignments); err != nil {
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
func (req *SyncGroupRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("SyncGroupRequest.Read: request or its body is nil")
	}

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

	// GenerationId (versions: 0+)
	generationid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.GenerationId = generationid

	// MemberId (versions: 0+)
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

	// GroupInstanceId (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			groupinstanceid, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			req.GroupInstanceId = groupinstanceid
		} else {
			groupinstanceid, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			req.GroupInstanceId = groupinstanceid
		}
	}

	// ProtocolType (versions: 5+)
	if req.ApiVersion >= 5 {
		if isRequestFlexible(req.ApiVersion) {
			protocoltype, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			req.ProtocolType = protocoltype
		} else {
			protocoltype, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			req.ProtocolType = protocoltype
		}
	}

	// ProtocolName (versions: 5+)
	if req.ApiVersion >= 5 {
		if isRequestFlexible(req.ApiVersion) {
			protocolname, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			req.ProtocolName = protocolname
		} else {
			protocolname, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			req.ProtocolName = protocolname
		}
	}

	// Assignments (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		assignments, err := protocol.ReadNullableCompactArray(r, req.assignmentsDecoder)
		if err != nil {
			return err
		}
		req.Assignments = assignments
	} else {
		assignments, err := protocol.ReadArray(r, req.assignmentsDecoder)
		if err != nil {
			return err
		}
		req.Assignments = &assignments
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

func (req *SyncGroupRequest) assignmentsEncoder(w io.Writer, value SyncGroupRequestAssignment) error {
	// MemberId (versions: 0+)
	if value.MemberId == nil {
		return fmt.Errorf("SyncGroupRequestAssignment.MemberId must not be nil in version %d", req.ApiVersion)
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

	// Assignment (versions: 0+)
	if value.Assignment == nil {
		return fmt.Errorf("SyncGroupRequestAssignment.Assignment must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *value.Assignment); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *value.Assignment); err != nil {
			return err
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

func (req *SyncGroupRequest) assignmentsDecoder(r io.Reader) (SyncGroupRequestAssignment, error) {
	syncgrouprequestassignment := SyncGroupRequestAssignment{}

	// MemberId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		memberid, err := protocol.ReadCompactString(r)
		if err != nil {
			return syncgrouprequestassignment, err
		}
		syncgrouprequestassignment.MemberId = &memberid
	} else {
		memberid, err := protocol.ReadString(r)
		if err != nil {
			return syncgrouprequestassignment, err
		}
		syncgrouprequestassignment.MemberId = &memberid
	}

	// Assignment (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		assignment, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return syncgrouprequestassignment, err
		}
		syncgrouprequestassignment.Assignment = &assignment
	} else {
		assignment, err := protocol.ReadBytes(r)
		if err != nil {
			return syncgrouprequestassignment, err
		}
		syncgrouprequestassignment.Assignment = &assignment
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return syncgrouprequestassignment, err
		}
		syncgrouprequestassignment.rawTaggedFields = &rawTaggedFields
	}

	return syncgrouprequestassignment, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *SyncGroupRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> SyncGroupRequest:\n")

	if req.GroupId != nil {
		fmt.Fprintf(w, "        GroupId: %v\n", *req.GroupId)
	} else {
		fmt.Fprintf(w, "        GroupId: nil\n")
	}

	fmt.Fprintf(w, "        GenerationId: %v\n", req.GenerationId)

	if req.MemberId != nil {
		fmt.Fprintf(w, "        MemberId: %v\n", *req.MemberId)
	} else {
		fmt.Fprintf(w, "        MemberId: nil\n")
	}

	if req.GroupInstanceId != nil {
		fmt.Fprintf(w, "        GroupInstanceId: %v\n", *req.GroupInstanceId)
	} else {
		fmt.Fprintf(w, "        GroupInstanceId: nil\n")
	}

	if req.ProtocolType != nil {
		fmt.Fprintf(w, "        ProtocolType: %v\n", *req.ProtocolType)
	} else {
		fmt.Fprintf(w, "        ProtocolType: nil\n")
	}

	if req.ProtocolName != nil {
		fmt.Fprintf(w, "        ProtocolName: %v\n", *req.ProtocolName)
	} else {
		fmt.Fprintf(w, "        ProtocolName: nil\n")
	}

	if req.Assignments != nil {
		fmt.Fprintf(w, "        Assignments:\n")
		for _, assignments := range *req.Assignments {
			fmt.Fprintf(w, "%s", assignments.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Assignments: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *SyncGroupRequestAssignment) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.MemberId != nil {
		fmt.Fprintf(w, "            MemberId: %v\n", *value.MemberId)
	} else {
		fmt.Fprintf(w, "            MemberId: nil\n")
	}

	if value.Assignment != nil {
		fmt.Fprintf(w, "            Assignment: <%d bytes>\n", len(*value.Assignment))
	} else {
		fmt.Fprintf(w, "            Assignment: nil\n")
	}

	return w.String()
}
