package fetch

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type FetchResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                        // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ErrorCode       int16                        // The top level response error code.
	SessionId       int32                        // The fetch session ID, or 0 if this is not part of a fetch session.
	Responses       *[]FetchResponseResponse     // The response topics.
	NodeEndpoints   *[]FetchResponseNodeEndpoint // tag 0: Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
	rawTaggedFields *[]protocol.TaggedField
}

type FetchResponseResponse struct {
	Topic           *string                           // The topic name.
	TopicId         uuid.UUID                         // The unique topic ID.
	Partitions      *[]FetchResponseResponsePartition // The topic partitions.
	rawTaggedFields *[]protocol.TaggedField
}

type FetchResponseResponsePartition struct {
	PartitionIndex       int32                                               // The partition index.
	ErrorCode            int16                                               // The error code, or 0 if there was no fetch error.
	HighWatermark        int64                                               // The current high water mark.
	LastStableOffset     int64                                               // The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
	LogStartOffset       int64                                               // The current log start offset.
	DivergingEpoch       *FetchResponseResponsePartitionDivergingEpoch       // tag 0: In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
	CurrentLeader        *FetchResponseResponsePartitionCurrentLeader        // tag 1: The current leader of the partition.
	SnapshotId           *FetchResponseResponsePartitionSnapshotId           // tag 2: In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
	AbortedTransactions  *[]FetchResponseResponsePartitionAbortedTransaction // The aborted transactions.
	PreferredReadReplica int32                                               // The preferred read replica for the consumer to use on its next fetch request.
	Records              *[]byte                                             // The record data.
	rawTaggedFields      *[]protocol.TaggedField
}

type FetchResponseResponsePartitionDivergingEpoch struct {
	Epoch           int32 // The largest epoch.
	EndOffset       int64 // The end offset of the epoch.
	rawTaggedFields *[]protocol.TaggedField
}

type FetchResponseResponsePartitionCurrentLeader struct {
	LeaderId        int32 // The ID of the current leader or -1 if the leader is unknown.
	LeaderEpoch     int32 // The latest known leader epoch.
	rawTaggedFields *[]protocol.TaggedField
}

type FetchResponseResponsePartitionSnapshotId struct {
	EndOffset       int64 // The end offset of the epoch.
	Epoch           int32 // The largest epoch.
	rawTaggedFields *[]protocol.TaggedField
}

type FetchResponseResponsePartitionAbortedTransaction struct {
	ProducerId      int64 // The producer id associated with the aborted transaction.
	FirstOffset     int64 // The first offset in the aborted transaction.
	rawTaggedFields *[]protocol.TaggedField
}

type FetchResponseNodeEndpoint struct {
	NodeId          int32   // The ID of the associated node.
	Host            *string // The node's hostname.
	Port            int32   // The node's port.
	Rack            *string // The rack of the node, or null if it has not been assigned to a rack.
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 12
}

func (res *FetchResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// ErrorCode (versions: 7+)
	if res.ApiVersion >= 7 {
		if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
			return err
		}
	}

	// SessionId (versions: 7+)
	if res.ApiVersion >= 7 {
		if err := protocol.WriteInt32(w, res.SessionId); err != nil {
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

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		taggedFields, err := res.taggedFieldsEncoder()
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (res *FetchResponse) Read(response protocol.Response) error {
	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	var err error

	// ThrottleTimeMs (versions: 1+)
	if response.ApiVersion >= 1 {
		throttletimems, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttletimems
	}

	// ErrorCode (versions: 7+)
	if response.ApiVersion >= 7 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		res.ErrorCode = errorcode
	}

	// SessionId (versions: 7+)
	if response.ApiVersion >= 7 {
		sessionid, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.SessionId = sessionid
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

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		err = protocol.ReadTaggedFields(r, res.taggedFieldsDecoder)
		if err != nil {
			return err
		}
	}

	return nil
}

func (res *FetchResponse) responsesEncoder(w io.Writer, value FetchResponseResponse) error {
	// Topic (versions: 0-12)
	if res.ApiVersion <= 12 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.Topic); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.Topic); err != nil {
				return err
			}
		}
	}

	// TopicId (versions: 13+)
	if res.ApiVersion >= 13 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
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

func (res *FetchResponse) responsesDecoder(r io.Reader) (FetchResponseResponse, error) {
	fetchresponseresponse := FetchResponseResponse{}
	var err error

	// Topic (versions: 0-12)
	if res.ApiVersion <= 12 {
		if isResponseFlexible(res.ApiVersion) {
			topic, err := protocol.ReadCompactString(r)
			if err != nil {
				return fetchresponseresponse, err
			}
			fetchresponseresponse.Topic = &topic
		} else {
			topic, err := protocol.ReadString(r)
			if err != nil {
				return fetchresponseresponse, err
			}
			fetchresponseresponse.Topic = &topic
		}
	}

	// TopicId (versions: 13+)
	if res.ApiVersion >= 13 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return fetchresponseresponse, err
		}
		fetchresponseresponse.TopicId = topicid
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return fetchresponseresponse, err
		}
		fetchresponseresponse.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return fetchresponseresponse, err
		}
		fetchresponseresponse.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchresponseresponse, err
		}
		fetchresponseresponse.rawTaggedFields = &rawTaggedFields
	}

	return fetchresponseresponse, nil
}

func (res *FetchResponse) partitionsEncoder(w io.Writer, value FetchResponseResponsePartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// HighWatermark (versions: 0+)
	if err := protocol.WriteInt64(w, value.HighWatermark); err != nil {
		return err
	}

	// LastStableOffset (versions: 4+)
	if res.ApiVersion >= 4 {
		if err := protocol.WriteInt64(w, value.LastStableOffset); err != nil {
			return err
		}
	}

	// LogStartOffset (versions: 5+)
	if res.ApiVersion >= 5 {
		if err := protocol.WriteInt64(w, value.LogStartOffset); err != nil {
			return err
		}
	}

	// DivergingEpoch (versions: 12+)
	if !isResponseFlexible(res.ApiVersion) {
		if res.ApiVersion >= 12 {
			if err := res.divergingEpochEncoder(w, *value.DivergingEpoch); err != nil {
				return err
			}
		}
	}

	// CurrentLeader (versions: 12+)
	if !isResponseFlexible(res.ApiVersion) {
		if res.ApiVersion >= 12 {
			if err := res.currentLeaderEncoder(w, *value.CurrentLeader); err != nil {
				return err
			}
		}
	}

	// SnapshotId (versions: 12+)
	if !isResponseFlexible(res.ApiVersion) {
		if res.ApiVersion >= 12 {
			if err := res.snapshotIdEncoder(w, *value.SnapshotId); err != nil {
				return err
			}
		}
	}

	// AbortedTransactions (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.abortedTransactionsEncoder, value.AbortedTransactions); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableArray(w, res.abortedTransactionsEncoder, value.AbortedTransactions); err != nil {
				return err
			}
		}
	}

	// PreferredReadReplica (versions: 11+)
	if res.ApiVersion >= 11 {
		if err := protocol.WriteInt32(w, value.PreferredReadReplica); err != nil {
			return err
		}
	}

	// Records (versions: 0+)
	if err := protocol.WriteCompactRecords(w, value.Records); err != nil {
		return err
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		taggedFields, err := res.taggedFieldsEncoderPartitions(value)
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *FetchResponse) partitionsDecoder(r io.Reader) (FetchResponseResponsePartition, error) {
	fetchresponseresponsepartition := FetchResponseResponsePartition{}
	var err error

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return fetchresponseresponsepartition, err
	}
	fetchresponseresponsepartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return fetchresponseresponsepartition, err
	}
	fetchresponseresponsepartition.ErrorCode = errorcode

	// HighWatermark (versions: 0+)
	highwatermark, err := protocol.ReadInt64(r)
	if err != nil {
		return fetchresponseresponsepartition, err
	}
	fetchresponseresponsepartition.HighWatermark = highwatermark

	// LastStableOffset (versions: 4+)
	if res.ApiVersion >= 4 {
		laststableoffset, err := protocol.ReadInt64(r)
		if err != nil {
			return fetchresponseresponsepartition, err
		}
		fetchresponseresponsepartition.LastStableOffset = laststableoffset
	}

	// LogStartOffset (versions: 5+)
	if res.ApiVersion >= 5 {
		logstartoffset, err := protocol.ReadInt64(r)
		if err != nil {
			return fetchresponseresponsepartition, err
		}
		fetchresponseresponsepartition.LogStartOffset = logstartoffset
	}

	// DivergingEpoch (versions: 12+)
	if !isResponseFlexible(res.ApiVersion) {
		if res.ApiVersion >= 12 {
			divergingepoch, err := res.divergingEpochDecoder(r)
			if err != nil {
				return fetchresponseresponsepartition, err
			}
			fetchresponseresponsepartition.DivergingEpoch = &divergingepoch
			if err != nil {
				return fetchresponseresponsepartition, err
			}
			fetchresponseresponsepartition.DivergingEpoch = &divergingepoch
		}
	}

	// CurrentLeader (versions: 12+)
	if !isResponseFlexible(res.ApiVersion) {
		if res.ApiVersion >= 12 {
			currentleader, err := res.currentLeaderDecoder(r)
			if err != nil {
				return fetchresponseresponsepartition, err
			}
			fetchresponseresponsepartition.CurrentLeader = &currentleader
			if err != nil {
				return fetchresponseresponsepartition, err
			}
			fetchresponseresponsepartition.CurrentLeader = &currentleader
		}
	}

	// SnapshotId (versions: 12+)
	if !isResponseFlexible(res.ApiVersion) {
		if res.ApiVersion >= 12 {
			snapshotid, err := res.snapshotIdDecoder(r)
			if err != nil {
				return fetchresponseresponsepartition, err
			}
			fetchresponseresponsepartition.SnapshotId = &snapshotid
			if err != nil {
				return fetchresponseresponsepartition, err
			}
			fetchresponseresponsepartition.SnapshotId = &snapshotid
		}
	}

	// AbortedTransactions (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			abortedtransactions, err := protocol.ReadNullableCompactArray(r, res.abortedTransactionsDecoder)
			if err != nil {
				return fetchresponseresponsepartition, err
			}
			fetchresponseresponsepartition.AbortedTransactions = abortedtransactions
		} else {
			abortedtransactions, err := protocol.ReadNullableArray(r, res.abortedTransactionsDecoder)
			if err != nil {
				return fetchresponseresponsepartition, err
			}
			fetchresponseresponsepartition.AbortedTransactions = abortedtransactions
		}
	}

	// PreferredReadReplica (versions: 11+)
	if res.ApiVersion >= 11 {
		preferredreadreplica, err := protocol.ReadInt32(r)
		if err != nil {
			return fetchresponseresponsepartition, err
		}
		fetchresponseresponsepartition.PreferredReadReplica = preferredreadreplica
	}

	// Records (versions: 0+)
	records, err := protocol.ReadCompactRecords(r)
	if err != nil {
		return fetchresponseresponsepartition, err
	}
	fetchresponseresponsepartition.Records = records

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		// Decode tagged fields
		err = protocol.ReadTaggedFields(r, func(r io.Reader, tag uint64, tagLength uint64) error {
			return res.taggedFieldsDecoderPartitions(r, tag, tagLength, &fetchresponseresponsepartition)
		})
		if err != nil {
			return fetchresponseresponsepartition, err
		}
	}

	return fetchresponseresponsepartition, nil
}

func (res *FetchResponse) taggedFieldsEncoderPartitions(value FetchResponseResponsePartition) ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if value.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*value.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 4+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	if value.DivergingEpoch != nil {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := res.divergingEpochEncoder(buf, *value.DivergingEpoch); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})
	}

	// Tag 1
	if value.CurrentLeader != nil {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := res.currentLeaderEncoder(buf, *value.CurrentLeader); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 1, Field: buf.Bytes()})
	}

	// Tag 2
	if value.SnapshotId != nil {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := res.snapshotIdEncoder(buf, *value.SnapshotId); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 2, Field: buf.Bytes()})
	}

	// We append any raw tagged fields to the end of the array
	if value.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *value.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (res *FetchResponse) taggedFieldsDecoderPartitions(r io.Reader, tag uint64, tagLength uint64, value *FetchResponseResponsePartition) error {
	rawTaggedFields := make([]protocol.TaggedField, 0)

	switch tag {
	case 0:
		// DivergingEpoch
		divergingepochVal, err := res.divergingEpochDecoder(r)
		if err != nil {
			return err
		}
		value.DivergingEpoch = &divergingepochVal
	case 1:
		// CurrentLeader
		currentleaderVal, err := res.currentLeaderDecoder(r)
		if err != nil {
			return err
		}
		value.CurrentLeader = &currentleaderVal
	case 2:
		// SnapshotId
		snapshotidVal, err := res.snapshotIdDecoder(r)
		if err != nil {
			return err
		}
		value.SnapshotId = &snapshotidVal
	default:
		// Decode as raw tags
		taggedField, err := protocol.ReadRawTaggedField(r)
		if err != nil {
			return err
		}
		rawTaggedFields = append(rawTaggedFields, taggedField)
	}

	// Set the raw tagged fields
	value.rawTaggedFields = &rawTaggedFields

	return nil
}

func (res *FetchResponse) divergingEpochEncoder(w io.Writer, value FetchResponseResponsePartitionDivergingEpoch) error {
	// Epoch (versions: 12+)
	if res.ApiVersion >= 12 {
		if err := protocol.WriteInt32(w, value.Epoch); err != nil {
			return err
		}
	}

	// EndOffset (versions: 12+)
	if res.ApiVersion >= 12 {
		if err := protocol.WriteInt64(w, value.EndOffset); err != nil {
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

func (res *FetchResponse) divergingEpochDecoder(r io.Reader) (FetchResponseResponsePartitionDivergingEpoch, error) {
	fetchresponseresponsepartitiondivergingepoch := FetchResponseResponsePartitionDivergingEpoch{}
	var err error

	// Epoch (versions: 12+)
	if res.ApiVersion >= 12 {
		epoch, err := protocol.ReadInt32(r)
		if err != nil {
			return fetchresponseresponsepartitiondivergingepoch, err
		}
		fetchresponseresponsepartitiondivergingepoch.Epoch = epoch
	}

	// EndOffset (versions: 12+)
	if res.ApiVersion >= 12 {
		endoffset, err := protocol.ReadInt64(r)
		if err != nil {
			return fetchresponseresponsepartitiondivergingepoch, err
		}
		fetchresponseresponsepartitiondivergingepoch.EndOffset = endoffset
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchresponseresponsepartitiondivergingepoch, err
		}
		fetchresponseresponsepartitiondivergingepoch.rawTaggedFields = &rawTaggedFields
	}

	return fetchresponseresponsepartitiondivergingepoch, nil
}

func (res *FetchResponse) currentLeaderEncoder(w io.Writer, value FetchResponseResponsePartitionCurrentLeader) error {
	// LeaderId (versions: 12+)
	if res.ApiVersion >= 12 {
		if err := protocol.WriteInt32(w, value.LeaderId); err != nil {
			return err
		}
	}

	// LeaderEpoch (versions: 12+)
	if res.ApiVersion >= 12 {
		if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
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

func (res *FetchResponse) currentLeaderDecoder(r io.Reader) (FetchResponseResponsePartitionCurrentLeader, error) {
	fetchresponseresponsepartitioncurrentleader := FetchResponseResponsePartitionCurrentLeader{}
	var err error

	// LeaderId (versions: 12+)
	if res.ApiVersion >= 12 {
		leaderid, err := protocol.ReadInt32(r)
		if err != nil {
			return fetchresponseresponsepartitioncurrentleader, err
		}
		fetchresponseresponsepartitioncurrentleader.LeaderId = leaderid
	}

	// LeaderEpoch (versions: 12+)
	if res.ApiVersion >= 12 {
		leaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return fetchresponseresponsepartitioncurrentleader, err
		}
		fetchresponseresponsepartitioncurrentleader.LeaderEpoch = leaderepoch
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchresponseresponsepartitioncurrentleader, err
		}
		fetchresponseresponsepartitioncurrentleader.rawTaggedFields = &rawTaggedFields
	}

	return fetchresponseresponsepartitioncurrentleader, nil
}

func (res *FetchResponse) snapshotIdEncoder(w io.Writer, value FetchResponseResponsePartitionSnapshotId) error {
	// EndOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.EndOffset); err != nil {
		return err
	}

	// Epoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.Epoch); err != nil {
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

func (res *FetchResponse) snapshotIdDecoder(r io.Reader) (FetchResponseResponsePartitionSnapshotId, error) {
	fetchresponseresponsepartitionsnapshotid := FetchResponseResponsePartitionSnapshotId{}
	var err error

	// EndOffset (versions: 0+)
	endoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return fetchresponseresponsepartitionsnapshotid, err
	}
	fetchresponseresponsepartitionsnapshotid.EndOffset = endoffset

	// Epoch (versions: 0+)
	epoch, err := protocol.ReadInt32(r)
	if err != nil {
		return fetchresponseresponsepartitionsnapshotid, err
	}
	fetchresponseresponsepartitionsnapshotid.Epoch = epoch

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchresponseresponsepartitionsnapshotid, err
		}
		fetchresponseresponsepartitionsnapshotid.rawTaggedFields = &rawTaggedFields
	}

	return fetchresponseresponsepartitionsnapshotid, nil
}

func (res *FetchResponse) abortedTransactionsEncoder(w io.Writer, value FetchResponseResponsePartitionAbortedTransaction) error {
	// ProducerId (versions: 4+)
	if res.ApiVersion >= 4 {
		if err := protocol.WriteInt64(w, value.ProducerId); err != nil {
			return err
		}
	}

	// FirstOffset (versions: 4+)
	if res.ApiVersion >= 4 {
		if err := protocol.WriteInt64(w, value.FirstOffset); err != nil {
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

func (res *FetchResponse) abortedTransactionsDecoder(r io.Reader) (FetchResponseResponsePartitionAbortedTransaction, error) {
	fetchresponseresponsepartitionabortedtransaction := FetchResponseResponsePartitionAbortedTransaction{}
	var err error

	// ProducerId (versions: 4+)
	if res.ApiVersion >= 4 {
		producerid, err := protocol.ReadInt64(r)
		if err != nil {
			return fetchresponseresponsepartitionabortedtransaction, err
		}
		fetchresponseresponsepartitionabortedtransaction.ProducerId = producerid
	}

	// FirstOffset (versions: 4+)
	if res.ApiVersion >= 4 {
		firstoffset, err := protocol.ReadInt64(r)
		if err != nil {
			return fetchresponseresponsepartitionabortedtransaction, err
		}
		fetchresponseresponsepartitionabortedtransaction.FirstOffset = firstoffset
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchresponseresponsepartitionabortedtransaction, err
		}
		fetchresponseresponsepartitionabortedtransaction.rawTaggedFields = &rawTaggedFields
	}

	return fetchresponseresponsepartitionabortedtransaction, nil
}

func (res *FetchResponse) nodeEndpointsEncoder(w io.Writer, value FetchResponseNodeEndpoint) error {
	// NodeId (versions: 16+)
	if res.ApiVersion >= 16 {
		if err := protocol.WriteInt32(w, value.NodeId); err != nil {
			return err
		}
	}

	// Host (versions: 16+)
	if res.ApiVersion >= 16 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.Host); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.Host); err != nil {
				return err
			}
		}
	}

	// Port (versions: 16+)
	if res.ApiVersion >= 16 {
		if err := protocol.WriteInt32(w, value.Port); err != nil {
			return err
		}
	}

	// Rack (versions: 16+)
	if res.ApiVersion >= 16 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.Rack); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.Rack); err != nil {
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

func (res *FetchResponse) nodeEndpointsDecoder(r io.Reader) (FetchResponseNodeEndpoint, error) {
	fetchresponsenodeendpoint := FetchResponseNodeEndpoint{}
	var err error

	// NodeId (versions: 16+)
	if res.ApiVersion >= 16 {
		nodeid, err := protocol.ReadInt32(r)
		if err != nil {
			return fetchresponsenodeendpoint, err
		}
		fetchresponsenodeendpoint.NodeId = nodeid
	}

	// Host (versions: 16+)
	if res.ApiVersion >= 16 {
		if isResponseFlexible(res.ApiVersion) {
			host, err := protocol.ReadCompactString(r)
			if err != nil {
				return fetchresponsenodeendpoint, err
			}
			fetchresponsenodeendpoint.Host = &host
		} else {
			host, err := protocol.ReadString(r)
			if err != nil {
				return fetchresponsenodeendpoint, err
			}
			fetchresponsenodeendpoint.Host = &host
		}
	}

	// Port (versions: 16+)
	if res.ApiVersion >= 16 {
		port, err := protocol.ReadInt32(r)
		if err != nil {
			return fetchresponsenodeendpoint, err
		}
		fetchresponsenodeendpoint.Port = port
	}

	// Rack (versions: 16+)
	if res.ApiVersion >= 16 {
		if isResponseFlexible(res.ApiVersion) {
			rack, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return fetchresponsenodeendpoint, err
			}
			fetchresponsenodeendpoint.Rack = rack
		} else {
			rack, err := protocol.ReadNullableString(r)
			if err != nil {
				return fetchresponsenodeendpoint, err
			}
			fetchresponsenodeendpoint.Rack = rack
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchresponsenodeendpoint, err
		}
		fetchresponsenodeendpoint.rawTaggedFields = &rawTaggedFields
	}

	return fetchresponsenodeendpoint, nil
}

func (res *FetchResponse) taggedFieldsEncoder() ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if res.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*res.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 2+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	if res.NodeEndpoints != nil {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := protocol.WriteNullableCompactArray(buf, res.nodeEndpointsEncoder, res.NodeEndpoints); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})
	}

	// We append any raw tagged fields to the end of the array
	if res.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *res.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (res *FetchResponse) taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {
	rawTaggedFields := make([]protocol.TaggedField, 0)

	switch tag {
	case 0:
		// NodeEndpoints
		nodeendpoints, err := protocol.ReadNullableCompactArray(r, res.nodeEndpointsDecoder)
		if err != nil {
			return err
		}
		res.NodeEndpoints = nodeendpoints
	default:
		// Decode as raw tags
		taggedField, err := protocol.ReadRawTaggedField(r)
		if err != nil {
			return err
		}
		rawTaggedFields = append(rawTaggedFields, taggedField)
	}

	// Set the raw tagged fields
	res.rawTaggedFields = &rawTaggedFields

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *FetchResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- FetchResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
	fmt.Fprintf(w, "        SessionId: %v\n", res.SessionId)
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
func (value *FetchResponseResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Topic != nil {
		fmt.Fprintf(w, "            Topic: %v\n", *value.Topic)
	} else {
		fmt.Fprintf(w, "            Topic: nil\n")
	}
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
func (value *FetchResponseResponsePartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)
	fmt.Fprintf(w, "                HighWatermark: %v\n", value.HighWatermark)
	fmt.Fprintf(w, "                LastStableOffset: %v\n", value.LastStableOffset)
	fmt.Fprintf(w, "                LogStartOffset: %v\n", value.LogStartOffset)
	fmt.Fprintf(w, "                DivergingEpoch:\n")
	if value.DivergingEpoch != nil {
		fmt.Fprintf(w, "%s", value.DivergingEpoch.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}
	fmt.Fprintf(w, "                CurrentLeader:\n")
	if value.CurrentLeader != nil {
		fmt.Fprintf(w, "%s", value.CurrentLeader.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}
	fmt.Fprintf(w, "                SnapshotId:\n")
	if value.SnapshotId != nil {
		fmt.Fprintf(w, "%s", value.SnapshotId.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}
	if value.AbortedTransactions != nil {
		fmt.Fprintf(w, "                AbortedTransactions:\n")
		for _, abortedtransactions := range *value.AbortedTransactions {
			fmt.Fprintf(w, "%s", abortedtransactions.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                AbortedTransactions: nil\n")
	}
	fmt.Fprintf(w, "                PreferredReadReplica: %v\n", value.PreferredReadReplica)
	fmt.Fprintf(w, "                Records: %v\n", value.Records)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchResponseResponsePartitionDivergingEpoch) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    Epoch: %v\n", value.Epoch)
	fmt.Fprintf(w, "                    EndOffset: %v\n", value.EndOffset)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchResponseResponsePartitionCurrentLeader) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                    LeaderEpoch: %v\n", value.LeaderEpoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchResponseResponsePartitionSnapshotId) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    EndOffset: %v\n", value.EndOffset)
	fmt.Fprintf(w, "                    Epoch: %v\n", value.Epoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchResponseResponsePartitionAbortedTransaction) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    ProducerId: %v\n", value.ProducerId)
	fmt.Fprintf(w, "                    FirstOffset: %v\n", value.FirstOffset)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchResponseNodeEndpoint) PrettyPrint() string {
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
