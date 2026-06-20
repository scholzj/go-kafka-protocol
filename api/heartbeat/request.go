package heartbeat

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type HeartbeatRequest struct {
	ApiVersion      int16
	GroupId         *string // The group id. (versions: 0+)
	GenerationId    int32   // The generation of the group. (versions: 0+)
	MemberId        *string // The member ID. (versions: 0+)
	GroupInstanceId *string // The unique identifier of the consumer instance provided by end user. (versions: 3+, nullable: 3+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 4
}

func (req *HeartbeatRequest) Write(w io.Writer) error {
	// GroupId (versions: 0+)
	if req.GroupId == nil {
		return fmt.Errorf("HeartbeatRequest.GroupId must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("HeartbeatRequest.MemberId must not be nil in version %d", req.ApiVersion)
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
func (req *HeartbeatRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("HeartbeatRequest.Read: request or its body is nil")
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

//goland:noinspection GoUnhandledErrorResult
func (req *HeartbeatRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> HeartbeatRequest:\n")

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

	return w.String()
}
