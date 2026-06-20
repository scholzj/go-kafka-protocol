package joingroup

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type JoinGroupRequest struct {
	ApiVersion         int16
	GroupId            *string                     // The group identifier. (versions: 0+)
	SessionTimeoutMs   int32                       // The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds. (versions: 0+)
	RebalanceTimeoutMs int32                       // The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group. (versions: 1+)
	MemberId           *string                     // The member id assigned by the group coordinator. (versions: 0+)
	GroupInstanceId    *string                     // The unique identifier of the consumer instance provided by end user. (versions: 5+, nullable: 5+)
	ProtocolType       *string                     // The unique name the for class of protocols implemented by the group we want to join. (versions: 0+)
	Protocols          *[]JoinGroupRequestProtocol // The list of protocols that the member supports. (versions: 0+)
	Reason             *string                     // The reason why the member (re-)joins the group. (versions: 8+, nullable: 8+)
	rawTaggedFields    *[]protocol.TaggedField
}

type JoinGroupRequestProtocol struct {
	Name            *string // The protocol name. (versions: 0+)
	Metadata        *[]byte // The protocol metadata. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 6
}

func (req *JoinGroupRequest) Write(w io.Writer) error {
	// GroupId (versions: 0+)
	if req.GroupId == nil {
		return fmt.Errorf("JoinGroupRequest.GroupId must not be nil in version %d", req.ApiVersion)
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

	// SessionTimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.SessionTimeoutMs); err != nil {
		return err
	}

	// RebalanceTimeoutMs (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, req.RebalanceTimeoutMs); err != nil {
			return err
		}
	}

	// MemberId (versions: 0+)
	if req.MemberId == nil {
		return fmt.Errorf("JoinGroupRequest.MemberId must not be nil in version %d", req.ApiVersion)
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

	// GroupInstanceId (versions: 5+)
	if req.ApiVersion >= 5 {
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

	// ProtocolType (versions: 0+)
	if req.ProtocolType == nil {
		return fmt.Errorf("JoinGroupRequest.ProtocolType must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *req.ProtocolType); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *req.ProtocolType); err != nil {
			return err
		}
	}

	// Protocols (versions: 0+)
	if req.Protocols == nil {
		return fmt.Errorf("JoinGroupRequest.Protocols must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.protocolsEncoder, req.Protocols); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.protocolsEncoder, *req.Protocols); err != nil {
			return err
		}
	}

	// Reason (versions: 8+)
	if req.ApiVersion >= 8 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, req.Reason); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, req.Reason); err != nil {
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
func (req *JoinGroupRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("JoinGroupRequest.Read: request or its body is nil")
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

	// SessionTimeoutMs (versions: 0+)
	sessiontimeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.SessionTimeoutMs = sessiontimeoutms

	// RebalanceTimeoutMs (versions: 1+)
	if req.ApiVersion >= 1 {
		rebalancetimeoutms, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.RebalanceTimeoutMs = rebalancetimeoutms
	}

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

	// GroupInstanceId (versions: 5+)
	if req.ApiVersion >= 5 {
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

	// ProtocolType (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		protocoltype, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		req.ProtocolType = &protocoltype
	} else {
		protocoltype, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		req.ProtocolType = &protocoltype
	}

	// Protocols (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		protocols, err := protocol.ReadNullableCompactArray(r, req.protocolsDecoder)
		if err != nil {
			return err
		}
		req.Protocols = protocols
	} else {
		protocols, err := protocol.ReadArray(r, req.protocolsDecoder)
		if err != nil {
			return err
		}
		req.Protocols = &protocols
	}

	// Reason (versions: 8+)
	if req.ApiVersion >= 8 {
		if isRequestFlexible(req.ApiVersion) {
			reason, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			req.Reason = reason
		} else {
			reason, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			req.Reason = reason
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

func (req *JoinGroupRequest) protocolsEncoder(w io.Writer, value JoinGroupRequestProtocol) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("JoinGroupRequestProtocol.Name must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// Metadata (versions: 0+)
	if value.Metadata == nil {
		return fmt.Errorf("JoinGroupRequestProtocol.Metadata must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *value.Metadata); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *value.Metadata); err != nil {
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

func (req *JoinGroupRequest) protocolsDecoder(r io.Reader) (JoinGroupRequestProtocol, error) {
	joingrouprequestprotocol := JoinGroupRequestProtocol{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return joingrouprequestprotocol, err
		}
		joingrouprequestprotocol.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return joingrouprequestprotocol, err
		}
		joingrouprequestprotocol.Name = &name
	}

	// Metadata (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		metadata, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return joingrouprequestprotocol, err
		}
		joingrouprequestprotocol.Metadata = &metadata
	} else {
		metadata, err := protocol.ReadBytes(r)
		if err != nil {
			return joingrouprequestprotocol, err
		}
		joingrouprequestprotocol.Metadata = &metadata
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return joingrouprequestprotocol, err
		}
		joingrouprequestprotocol.rawTaggedFields = &rawTaggedFields
	}

	return joingrouprequestprotocol, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *JoinGroupRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> JoinGroupRequest:\n")

	if req.GroupId != nil {
		fmt.Fprintf(w, "        GroupId: %v\n", *req.GroupId)
	} else {
		fmt.Fprintf(w, "        GroupId: nil\n")
	}

	fmt.Fprintf(w, "        SessionTimeoutMs: %v\n", req.SessionTimeoutMs)
	fmt.Fprintf(w, "        RebalanceTimeoutMs: %v\n", req.RebalanceTimeoutMs)

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

	if req.Protocols != nil {
		fmt.Fprintf(w, "        Protocols:\n")
		for _, protocols := range *req.Protocols {
			fmt.Fprintf(w, "%s", protocols.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Protocols: nil\n")
	}

	if req.Reason != nil {
		fmt.Fprintf(w, "        Reason: %v\n", *req.Reason)
	} else {
		fmt.Fprintf(w, "        Reason: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *JoinGroupRequestProtocol) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	if value.Metadata != nil {
		fmt.Fprintf(w, "            Metadata: <%d bytes>\n", len(*value.Metadata))
	} else {
		fmt.Fprintf(w, "            Metadata: nil\n")
	}

	return w.String()
}
