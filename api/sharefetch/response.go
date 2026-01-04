package sharefetch

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ShareFetchResponse struct {
	ApiVersion               int16
	ThrottleTimeMs           int32                             // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ErrorCode                int16                             // The top-level response error code.
	ErrorMessage             *string                           // The top-level error message, or null if there was no error.
	AcquisitionLockTimeoutMs int32                             // The time in milliseconds for which the acquired records are locked.
	Responses                *[]ShareFetchResponseResponse     // The response topics.
	NodeEndpoints            *[]ShareFetchResponseNodeEndpoint // Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
	rawTaggedFields          *[]protocol.TaggedField
}

type ShareFetchResponseResponse struct {
	TopicId         uuid.UUID                              // The unique topic ID.
	Partitions      *[]ShareFetchResponseResponsePartition // The topic partitions.
	rawTaggedFields *[]protocol.TaggedField
}

type ShareFetchResponseResponsePartition struct {
	PartitionIndex          int32                                                // The partition index.
	ErrorCode               int16                                                // The fetch error code, or 0 if there was no fetch error.
	ErrorMessage            *string                                              // The fetch error message, or null if there was no fetch error.
	AcknowledgeErrorCode    int16                                                // The acknowledge error code, or 0 if there was no acknowledge error.
	AcknowledgeErrorMessage *string                                              // The acknowledge error message, or null if there was no acknowledge error.
	CurrentLeader           *ShareFetchResponseResponsePartitionCurrentLeader    // The current leader of the partition.
	Records                 *[]byte                                              // The record data.
	AcquiredRecords         *[]ShareFetchResponseResponsePartitionAcquiredRecord // The acquired records.
	rawTaggedFields         *[]protocol.TaggedField
}

type ShareFetchResponseResponsePartitionCurrentLeader struct {
	LeaderId        int32 // The ID of the current leader or -1 if the leader is unknown.
	LeaderEpoch     int32 // The latest known leader epoch.
	rawTaggedFields *[]protocol.TaggedField
}

type ShareFetchResponseResponsePartitionAcquiredRecord struct {
	FirstOffset     int64 // The earliest offset in this batch of acquired records.
	LastOffset      int64 // The last offset of this batch of acquired records.
	DeliveryCount   int16 // The delivery count of this batch of acquired records.
	rawTaggedFields *[]protocol.TaggedField
}

type ShareFetchResponseNodeEndpoint struct {
	NodeId          int32   // The ID of the associated node.
	Host            *string // The node's hostname.
	Port            int32   // The node's port.
	Rack            *string // The rack of the node, or null if it has not been assigned to a rack.
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *ShareFetchResponse) Write(w io.Writer) error {
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

	// AcquisitionLockTimeoutMs (versions: 1+)
	if res.ApiVersion >= 1 {
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
func (res *ShareFetchResponse) Read(response protocol.Response) error {
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

	// AcquisitionLockTimeoutMs (versions: 1+)
	if response.ApiVersion >= 1 {
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

func (res *ShareFetchResponse) responsesEncoder(w io.Writer, value ShareFetchResponseResponse) error {
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

func (res *ShareFetchResponse) responsesDecoder(r io.Reader) (ShareFetchResponseResponse, error) {
	sharefetchresponseresponse := ShareFetchResponseResponse{}
	var err error

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return sharefetchresponseresponse, err
	}
	sharefetchresponseresponse.TopicId = topicid

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return sharefetchresponseresponse, err
		}
		sharefetchresponseresponse.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return sharefetchresponseresponse, err
		}
		sharefetchresponseresponse.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return sharefetchresponseresponse, err
		}
		sharefetchresponseresponse.rawTaggedFields = &rawTaggedFields
	}

	return sharefetchresponseresponse, nil
}

func (res *ShareFetchResponse) partitionsEncoder(w io.Writer, value ShareFetchResponseResponsePartition) error {
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

	// AcknowledgeErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.AcknowledgeErrorCode); err != nil {
		return err
	}

	// AcknowledgeErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.AcknowledgeErrorMessage); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.AcknowledgeErrorMessage); err != nil {
			return err
		}
	}

	// CurrentLeader (versions: 0+)
	if err := res.currentLeaderEncoder(w, *value.CurrentLeader); err != nil {
		return err
	}

	// Records (versions: 0+)
	if err := protocol.WriteCompactRecords(w, value.Records); err != nil {
		return err
	}

	// AcquiredRecords (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.acquiredRecordsEncoder, value.AcquiredRecords); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.acquiredRecordsEncoder, *value.AcquiredRecords); err != nil {
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

func (res *ShareFetchResponse) partitionsDecoder(r io.Reader) (ShareFetchResponseResponsePartition, error) {
	sharefetchresponseresponsepartition := ShareFetchResponseResponsePartition{}
	var err error

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return sharefetchresponseresponsepartition, err
	}
	sharefetchresponseresponsepartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return sharefetchresponseresponsepartition, err
	}
	sharefetchresponseresponsepartition.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return sharefetchresponseresponsepartition, err
		}
		sharefetchresponseresponsepartition.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return sharefetchresponseresponsepartition, err
		}
		sharefetchresponseresponsepartition.ErrorMessage = errormessage
	}

	// AcknowledgeErrorCode (versions: 0+)
	acknowledgeerrorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return sharefetchresponseresponsepartition, err
	}
	sharefetchresponseresponsepartition.AcknowledgeErrorCode = acknowledgeerrorcode

	// AcknowledgeErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		acknowledgeerrormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return sharefetchresponseresponsepartition, err
		}
		sharefetchresponseresponsepartition.AcknowledgeErrorMessage = acknowledgeerrormessage
	} else {
		acknowledgeerrormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return sharefetchresponseresponsepartition, err
		}
		sharefetchresponseresponsepartition.AcknowledgeErrorMessage = acknowledgeerrormessage
	}

	// CurrentLeader (versions: 0+)
	currentleader, err := res.currentLeaderDecoder(r)
	if err != nil {
		return sharefetchresponseresponsepartition, err
	}
	sharefetchresponseresponsepartition.CurrentLeader = &currentleader
	if err != nil {
		return sharefetchresponseresponsepartition, err
	}
	sharefetchresponseresponsepartition.CurrentLeader = &currentleader

	// Records (versions: 0+)
	records, err := protocol.ReadCompactRecords(r)
	if err != nil {
		return sharefetchresponseresponsepartition, err
	}
	sharefetchresponseresponsepartition.Records = records

	// AcquiredRecords (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		acquiredrecords, err := protocol.ReadNullableCompactArray(r, res.acquiredRecordsDecoder)
		if err != nil {
			return sharefetchresponseresponsepartition, err
		}
		sharefetchresponseresponsepartition.AcquiredRecords = acquiredrecords
	} else {
		acquiredrecords, err := protocol.ReadArray(r, res.acquiredRecordsDecoder)
		if err != nil {
			return sharefetchresponseresponsepartition, err
		}
		sharefetchresponseresponsepartition.AcquiredRecords = &acquiredrecords
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return sharefetchresponseresponsepartition, err
		}
		sharefetchresponseresponsepartition.rawTaggedFields = &rawTaggedFields
	}

	return sharefetchresponseresponsepartition, nil
}

func (res *ShareFetchResponse) currentLeaderEncoder(w io.Writer, value ShareFetchResponseResponsePartitionCurrentLeader) error {
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

func (res *ShareFetchResponse) currentLeaderDecoder(r io.Reader) (ShareFetchResponseResponsePartitionCurrentLeader, error) {
	sharefetchresponseresponsepartitioncurrentleader := ShareFetchResponseResponsePartitionCurrentLeader{}
	var err error

	// LeaderId (versions: 0+)
	leaderid, err := protocol.ReadInt32(r)
	if err != nil {
		return sharefetchresponseresponsepartitioncurrentleader, err
	}
	sharefetchresponseresponsepartitioncurrentleader.LeaderId = leaderid

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return sharefetchresponseresponsepartitioncurrentleader, err
	}
	sharefetchresponseresponsepartitioncurrentleader.LeaderEpoch = leaderepoch

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return sharefetchresponseresponsepartitioncurrentleader, err
		}
		sharefetchresponseresponsepartitioncurrentleader.rawTaggedFields = &rawTaggedFields
	}

	return sharefetchresponseresponsepartitioncurrentleader, nil
}

func (res *ShareFetchResponse) acquiredRecordsEncoder(w io.Writer, value ShareFetchResponseResponsePartitionAcquiredRecord) error {
	// FirstOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.FirstOffset); err != nil {
		return err
	}

	// LastOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.LastOffset); err != nil {
		return err
	}

	// DeliveryCount (versions: 0+)
	if err := protocol.WriteInt16(w, value.DeliveryCount); err != nil {
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

func (res *ShareFetchResponse) acquiredRecordsDecoder(r io.Reader) (ShareFetchResponseResponsePartitionAcquiredRecord, error) {
	sharefetchresponseresponsepartitionacquiredrecord := ShareFetchResponseResponsePartitionAcquiredRecord{}
	var err error

	// FirstOffset (versions: 0+)
	firstoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return sharefetchresponseresponsepartitionacquiredrecord, err
	}
	sharefetchresponseresponsepartitionacquiredrecord.FirstOffset = firstoffset

	// LastOffset (versions: 0+)
	lastoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return sharefetchresponseresponsepartitionacquiredrecord, err
	}
	sharefetchresponseresponsepartitionacquiredrecord.LastOffset = lastoffset

	// DeliveryCount (versions: 0+)
	deliverycount, err := protocol.ReadInt16(r)
	if err != nil {
		return sharefetchresponseresponsepartitionacquiredrecord, err
	}
	sharefetchresponseresponsepartitionacquiredrecord.DeliveryCount = deliverycount

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return sharefetchresponseresponsepartitionacquiredrecord, err
		}
		sharefetchresponseresponsepartitionacquiredrecord.rawTaggedFields = &rawTaggedFields
	}

	return sharefetchresponseresponsepartitionacquiredrecord, nil
}

func (res *ShareFetchResponse) nodeEndpointsEncoder(w io.Writer, value ShareFetchResponseNodeEndpoint) error {
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

func (res *ShareFetchResponse) nodeEndpointsDecoder(r io.Reader) (ShareFetchResponseNodeEndpoint, error) {
	sharefetchresponsenodeendpoint := ShareFetchResponseNodeEndpoint{}
	var err error

	// NodeId (versions: 0+)
	nodeid, err := protocol.ReadInt32(r)
	if err != nil {
		return sharefetchresponsenodeendpoint, err
	}
	sharefetchresponsenodeendpoint.NodeId = nodeid

	// Host (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return sharefetchresponsenodeendpoint, err
		}
		sharefetchresponsenodeendpoint.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return sharefetchresponsenodeendpoint, err
		}
		sharefetchresponsenodeendpoint.Host = &host
	}

	// Port (versions: 0+)
	port, err := protocol.ReadInt32(r)
	if err != nil {
		return sharefetchresponsenodeendpoint, err
	}
	sharefetchresponsenodeendpoint.Port = port

	// Rack (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		rack, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return sharefetchresponsenodeendpoint, err
		}
		sharefetchresponsenodeendpoint.Rack = rack
	} else {
		rack, err := protocol.ReadNullableString(r)
		if err != nil {
			return sharefetchresponsenodeendpoint, err
		}
		sharefetchresponsenodeendpoint.Rack = rack
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return sharefetchresponsenodeendpoint, err
		}
		sharefetchresponsenodeendpoint.rawTaggedFields = &rawTaggedFields
	}

	return sharefetchresponsenodeendpoint, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ShareFetchResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ShareFetchResponse:\n")
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
func (value *ShareFetchResponseResponse) PrettyPrint() string {
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
func (value *ShareFetchResponseResponsePartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)
	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                ErrorMessage: nil\n")
	}
	fmt.Fprintf(w, "                AcknowledgeErrorCode: %v\n", value.AcknowledgeErrorCode)
	if value.AcknowledgeErrorMessage != nil {
		fmt.Fprintf(w, "                AcknowledgeErrorMessage: %v\n", *value.AcknowledgeErrorMessage)
	} else {
		fmt.Fprintf(w, "                AcknowledgeErrorMessage: nil\n")
	}
	fmt.Fprintf(w, "                CurrentLeader:\n")
	if value.CurrentLeader != nil {
		fmt.Fprintf(w, "%s", value.CurrentLeader.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}
	fmt.Fprintf(w, "                Records: %v\n", value.Records)
	if value.AcquiredRecords != nil {
		fmt.Fprintf(w, "                AcquiredRecords:\n")
		for _, acquiredrecords := range *value.AcquiredRecords {
			fmt.Fprintf(w, "%s", acquiredrecords.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                AcquiredRecords: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ShareFetchResponseResponsePartitionCurrentLeader) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                    LeaderEpoch: %v\n", value.LeaderEpoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ShareFetchResponseResponsePartitionAcquiredRecord) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    FirstOffset: %v\n", value.FirstOffset)
	fmt.Fprintf(w, "                    LastOffset: %v\n", value.LastOffset)
	fmt.Fprintf(w, "                    DeliveryCount: %v\n", value.DeliveryCount)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ShareFetchResponseNodeEndpoint) PrettyPrint() string {
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
