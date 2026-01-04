package shareacknowledge

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ShareAcknowledgeResponse struct {
	ApiVersion               int16
	ThrottleTimeMs           int32                                   // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ErrorCode                int16                                   // The top level response error code.
	ErrorMessage             *string                                 // The top-level error message, or null if there was no error.
	AcquisitionLockTimeoutMs int32                                   // The time in milliseconds for which the acquired records are locked.
	Responses                *[]ShareAcknowledgeResponseResponse     // The response topics.
	NodeEndpoints            *[]ShareAcknowledgeResponseNodeEndpoint // Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
	rawTaggedFields          *[]protocol.TaggedField
}

type ShareAcknowledgeResponseResponse struct {
	TopicId         uuid.UUID                                    // The unique topic ID.
	Partitions      *[]ShareAcknowledgeResponseResponsePartition // The topic partitions.
	rawTaggedFields *[]protocol.TaggedField
}

type ShareAcknowledgeResponseResponsePartition struct {
	PartitionIndex  int32                                                   // The partition index.
	ErrorCode       int16                                                   // The error code, or 0 if there was no error.
	ErrorMessage    *string                                                 // The error message, or null if there was no error.
	CurrentLeader   *ShareAcknowledgeResponseResponsePartitionCurrentLeader // The current leader of the partition.
	rawTaggedFields *[]protocol.TaggedField
}

type ShareAcknowledgeResponseResponsePartitionCurrentLeader struct {
	LeaderId        int32 // The ID of the current leader or -1 if the leader is unknown.
	LeaderEpoch     int32 // The latest known leader epoch.
	rawTaggedFields *[]protocol.TaggedField
}

type ShareAcknowledgeResponseNodeEndpoint struct {
	NodeId          int32   // The ID of the associated node.
	Host            *string // The node's hostname.
	Port            int32   // The node's port.
	Rack            *string // The rack of the node, or null if it has not been assigned to a rack.
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *ShareAcknowledgeResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, res.ErrorMessage); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, res.ErrorMessage); err != nil {
			return err
		}
	}

	// AcquisitionLockTimeoutMs (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteInt32(w, res.AcquisitionLockTimeoutMs); err != nil {
			return err
		}
	}

	// Responses (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.responsesEncoder, res.Responses); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.responsesEncoder, *res.Responses); err != nil {
			return err
		}
	}

	// NodeEndpoints (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.nodeEndpointsEncoder, res.NodeEndpoints); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.nodeEndpointsEncoder, *res.NodeEndpoints); err != nil {
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
func (res *ShareAcknowledgeResponse) Read(response protocol.Response) error {
	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	var err error

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		res.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		res.ErrorMessage = errormessage
	}

	// AcquisitionLockTimeoutMs (versions: 2+)
	if response.ApiVersion >= 2 {
		acquisitionlocktimeoutms, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.AcquisitionLockTimeoutMs = acquisitionlocktimeoutms
	}

	// Responses (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		responses, err := protocol.ReadNullableCompactArray(r, res.responsesDecoder)
		if err != nil {
			return err
		}
		res.Responses = responses
	} else {
		responses, err := protocol.ReadArray(r, res.responsesDecoder)
		if err != nil {
			return err
		}
		res.Responses = &responses
	}

	// NodeEndpoints (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		nodeendpoints, err := protocol.ReadNullableCompactArray(r, res.nodeEndpointsDecoder)
		if err != nil {
			return err
		}
		res.NodeEndpoints = nodeendpoints
	} else {
		nodeendpoints, err := protocol.ReadArray(r, res.nodeEndpointsDecoder)
		if err != nil {
			return err
		}
		res.NodeEndpoints = &nodeendpoints
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		res.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

func (res *ShareAcknowledgeResponse) responsesEncoder(w io.Writer, value ShareAcknowledgeResponseResponse) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.partitionsEncoder, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.partitionsEncoder, *value.Partitions); err != nil {
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

func (res *ShareAcknowledgeResponse) responsesDecoder(r io.Reader) (ShareAcknowledgeResponseResponse, error) {
	shareacknowledgeresponseresponse := ShareAcknowledgeResponseResponse{}
	var err error

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return shareacknowledgeresponseresponse, err
	}
	shareacknowledgeresponseresponse.TopicId = topicid

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return shareacknowledgeresponseresponse, err
		}
		shareacknowledgeresponseresponse.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return shareacknowledgeresponseresponse, err
		}
		shareacknowledgeresponseresponse.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return shareacknowledgeresponseresponse, err
		}
		shareacknowledgeresponseresponse.rawTaggedFields = &rawTaggedFields
	}

	return shareacknowledgeresponseresponse, nil
}

func (res *ShareAcknowledgeResponse) partitionsEncoder(w io.Writer, value ShareAcknowledgeResponseResponsePartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

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

	// CurrentLeader (versions: 0+)
	if err := res.currentLeaderEncoder(w, *value.CurrentLeader); err != nil {
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

func (res *ShareAcknowledgeResponse) partitionsDecoder(r io.Reader) (ShareAcknowledgeResponseResponsePartition, error) {
	shareacknowledgeresponseresponsepartition := ShareAcknowledgeResponseResponsePartition{}
	var err error

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return shareacknowledgeresponseresponsepartition, err
	}
	shareacknowledgeresponseresponsepartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return shareacknowledgeresponseresponsepartition, err
	}
	shareacknowledgeresponseresponsepartition.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return shareacknowledgeresponseresponsepartition, err
		}
		shareacknowledgeresponseresponsepartition.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return shareacknowledgeresponseresponsepartition, err
		}
		shareacknowledgeresponseresponsepartition.ErrorMessage = errormessage
	}

	// CurrentLeader (versions: 0+)
	currentleader, err := res.currentLeaderDecoder(r)
	if err != nil {
		return shareacknowledgeresponseresponsepartition, err
	}
	shareacknowledgeresponseresponsepartition.CurrentLeader = &currentleader
	if err != nil {
		return shareacknowledgeresponseresponsepartition, err
	}
	shareacknowledgeresponseresponsepartition.CurrentLeader = &currentleader

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return shareacknowledgeresponseresponsepartition, err
		}
		shareacknowledgeresponseresponsepartition.rawTaggedFields = &rawTaggedFields
	}

	return shareacknowledgeresponseresponsepartition, nil
}

func (res *ShareAcknowledgeResponse) currentLeaderEncoder(w io.Writer, value ShareAcknowledgeResponseResponsePartitionCurrentLeader) error {
	// LeaderId (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderId); err != nil {
		return err
	}

	// LeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
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

func (res *ShareAcknowledgeResponse) currentLeaderDecoder(r io.Reader) (ShareAcknowledgeResponseResponsePartitionCurrentLeader, error) {
	shareacknowledgeresponseresponsepartitioncurrentleader := ShareAcknowledgeResponseResponsePartitionCurrentLeader{}
	var err error

	// LeaderId (versions: 0+)
	leaderid, err := protocol.ReadInt32(r)
	if err != nil {
		return shareacknowledgeresponseresponsepartitioncurrentleader, err
	}
	shareacknowledgeresponseresponsepartitioncurrentleader.LeaderId = leaderid

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return shareacknowledgeresponseresponsepartitioncurrentleader, err
	}
	shareacknowledgeresponseresponsepartitioncurrentleader.LeaderEpoch = leaderepoch

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return shareacknowledgeresponseresponsepartitioncurrentleader, err
		}
		shareacknowledgeresponseresponsepartitioncurrentleader.rawTaggedFields = &rawTaggedFields
	}

	return shareacknowledgeresponseresponsepartitioncurrentleader, nil
}

func (res *ShareAcknowledgeResponse) nodeEndpointsEncoder(w io.Writer, value ShareAcknowledgeResponseNodeEndpoint) error {
	// NodeId (versions: 0+)
	if err := protocol.WriteInt32(w, value.NodeId); err != nil {
		return err
	}

	// Host (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Host); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Host); err != nil {
			return err
		}
	}

	// Port (versions: 0+)
	if err := protocol.WriteInt32(w, value.Port); err != nil {
		return err
	}

	// Rack (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Rack); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.Rack); err != nil {
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

func (res *ShareAcknowledgeResponse) nodeEndpointsDecoder(r io.Reader) (ShareAcknowledgeResponseNodeEndpoint, error) {
	shareacknowledgeresponsenodeendpoint := ShareAcknowledgeResponseNodeEndpoint{}
	var err error

	// NodeId (versions: 0+)
	nodeid, err := protocol.ReadInt32(r)
	if err != nil {
		return shareacknowledgeresponsenodeendpoint, err
	}
	shareacknowledgeresponsenodeendpoint.NodeId = nodeid

	// Host (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return shareacknowledgeresponsenodeendpoint, err
		}
		shareacknowledgeresponsenodeendpoint.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return shareacknowledgeresponsenodeendpoint, err
		}
		shareacknowledgeresponsenodeendpoint.Host = &host
	}

	// Port (versions: 0+)
	port, err := protocol.ReadInt32(r)
	if err != nil {
		return shareacknowledgeresponsenodeendpoint, err
	}
	shareacknowledgeresponsenodeendpoint.Port = port

	// Rack (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		rack, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return shareacknowledgeresponsenodeendpoint, err
		}
		shareacknowledgeresponsenodeendpoint.Rack = rack
	} else {
		rack, err := protocol.ReadNullableString(r)
		if err != nil {
			return shareacknowledgeresponsenodeendpoint, err
		}
		shareacknowledgeresponsenodeendpoint.Rack = rack
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return shareacknowledgeresponsenodeendpoint, err
		}
		shareacknowledgeresponsenodeendpoint.rawTaggedFields = &rawTaggedFields
	}

	return shareacknowledgeresponsenodeendpoint, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ShareAcknowledgeResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ShareAcknowledgeResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
	if res.ErrorMessage != nil {
		fmt.Fprintf(w, "        ErrorMessage: %v\n", *res.ErrorMessage)
	} else {
		fmt.Fprintf(w, "        ErrorMessage: nil\n")
	}
	fmt.Fprintf(w, "        AcquisitionLockTimeoutMs: %v\n", res.AcquisitionLockTimeoutMs)
	if res.Responses != nil {
		fmt.Fprintf(w, "        Responses:\n")
		for _, responses := range *res.Responses {
			fmt.Fprintf(w, "%s", responses.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Responses: nil\n")
	}
	if res.NodeEndpoints != nil {
		fmt.Fprintf(w, "        NodeEndpoints:\n")
		for _, nodeendpoints := range *res.NodeEndpoints {
			fmt.Fprintf(w, "%s", nodeendpoints.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        NodeEndpoints: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ShareAcknowledgeResponseResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            TopicId: %v\n", value.TopicId)
	if value.Partitions != nil {
		fmt.Fprintf(w, "            Partitions:\n")
		for _, partitions := range *value.Partitions {
			fmt.Fprintf(w, "%s", partitions.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ShareAcknowledgeResponseResponsePartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)
	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                ErrorMessage: nil\n")
	}
	fmt.Fprintf(w, "                CurrentLeader:\n")
	if value.CurrentLeader != nil {
		fmt.Fprintf(w, "%s", value.CurrentLeader.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ShareAcknowledgeResponseResponsePartitionCurrentLeader) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                    LeaderEpoch: %v\n", value.LeaderEpoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ShareAcknowledgeResponseNodeEndpoint) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            NodeId: %v\n", value.NodeId)
	if value.Host != nil {
		fmt.Fprintf(w, "            Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "            Host: nil\n")
	}
	fmt.Fprintf(w, "            Port: %v\n", value.Port)
	if value.Rack != nil {
		fmt.Fprintf(w, "            Rack: %v\n", *value.Rack)
	} else {
		fmt.Fprintf(w, "            Rack: nil\n")
	}

	return w.String()
}
