package fetch

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type FetchRequest struct {
	ApiVersion          int16
	ClusterId           *string                            // tag 0: The clusterId if known. This is used to validate metadata fetches prior to broker registration.
	ReplicaId           int32                              // The broker ID of the follower, of -1 if this request is from a consumer.
	ReplicaState        *FetchRequestReplicaState          // tag 1: The state of the replica in the follower.
	MaxWaitMs           int32                              // The maximum time in milliseconds to wait for the response.
	MinBytes            int32                              // The minimum bytes to accumulate in the response.
	MaxBytes            int32                              // The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
	IsolationLevel      int8                               // This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
	SessionId           int32                              // The fetch session ID.
	SessionEpoch        int32                              // The fetch session epoch, which is used for ordering requests in a session.
	Topics              *[]FetchRequestTopic               // The topics to fetch.
	ForgottenTopicsData *[]FetchRequestForgottenTopicsData // In an incremental fetch request, the partitions to remove.
	RackId              *string                            // Rack ID of the consumer making this request.
	rawTaggedFields     *[]protocol.TaggedField
}

type FetchRequestReplicaState struct {
	ReplicaId       int32 // The replica ID of the follower, or -1 if this request is from a consumer.
	ReplicaEpoch    int64 // The epoch of this follower, or -1 if not available.
	rawTaggedFields *[]protocol.TaggedField
}

type FetchRequestTopic struct {
	Topic           *string                       // The name of the topic to fetch.
	TopicId         uuid.UUID                     // The unique topic ID.
	Partitions      *[]FetchRequestTopicPartition // The partitions to fetch.
	rawTaggedFields *[]protocol.TaggedField
}

type FetchRequestTopicPartition struct {
	Partition          int32     // The partition index.
	CurrentLeaderEpoch int32     // The current leader epoch of the partition.
	FetchOffset        int64     // The message offset.
	LastFetchedEpoch   int32     // The epoch of the last fetched record or -1 if there is none.
	LogStartOffset     int64     // The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
	PartitionMaxBytes  int32     // The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
	ReplicaDirectoryId uuid.UUID // tag 0: The directory id of the follower fetching.
	HighWatermark      int64     // tag 1: The high-watermark known by the replica. -1 if the high-watermark is not known and 9223372036854775807 if the feature is not supported.
	rawTaggedFields    *[]protocol.TaggedField
}

type FetchRequestForgottenTopicsData struct {
	Topic           *string   // The topic name.
	TopicId         uuid.UUID // The unique topic ID.
	Partitions      *[]int32  // The partitions indexes to forget.
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 12
}

func (req *FetchRequest) Write(w io.Writer) error {
	// ReplicaId (versions: 0-14)
	if req.ApiVersion <= 14 {
		if err := protocol.WriteInt32(w, req.ReplicaId); err != nil {
			return err
		}
	}

	// MaxWaitMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.MaxWaitMs); err != nil {
		return err
	}

	// MinBytes (versions: 0+)
	if err := protocol.WriteInt32(w, req.MinBytes); err != nil {
		return err
	}

	// MaxBytes (versions: 3+)
	if req.ApiVersion >= 3 {
		if err := protocol.WriteInt32(w, req.MaxBytes); err != nil {
			return err
		}
	}

	// IsolationLevel (versions: 4+)
	if req.ApiVersion >= 4 {
		if err := protocol.WriteInt8(w, req.IsolationLevel); err != nil {
			return err
		}
	}

	// SessionId (versions: 7+)
	if req.ApiVersion >= 7 {
		if err := protocol.WriteInt32(w, req.SessionId); err != nil {
			return err
		}
	}

	// SessionEpoch (versions: 7+)
	if req.ApiVersion >= 7 {
		if err := protocol.WriteInt32(w, req.SessionEpoch); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, req.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.topicsEncoder, *req.Topics); err != nil {
			return err
		}
	}

	// ForgottenTopicsData (versions: 7+)
	if req.ApiVersion >= 7 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, req.forgottenTopicsDataEncoder, req.ForgottenTopicsData); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, req.forgottenTopicsDataEncoder, *req.ForgottenTopicsData); err != nil {
				return err
			}
		}
	}

	// RackId (versions: 11+)
	if req.ApiVersion >= 11 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteCompactString(w, *req.RackId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *req.RackId); err != nil {
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
func (req *FetchRequest) Read(request protocol.Request) error {
	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	var err error

	// ReplicaId (versions: 0-14)
	if request.ApiVersion <= 14 {
		replicaid, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.ReplicaId = replicaid
	}

	// MaxWaitMs (versions: 0+)
	maxwaitms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.MaxWaitMs = maxwaitms

	// MinBytes (versions: 0+)
	minbytes, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.MinBytes = minbytes

	// MaxBytes (versions: 3+)
	if request.ApiVersion >= 3 {
		maxbytes, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.MaxBytes = maxbytes
	}

	// IsolationLevel (versions: 4+)
	if request.ApiVersion >= 4 {
		isolationlevel, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		req.IsolationLevel = isolationlevel
	}

	// SessionId (versions: 7+)
	if request.ApiVersion >= 7 {
		sessionid, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.SessionId = sessionid
	}

	// SessionEpoch (versions: 7+)
	if request.ApiVersion >= 7 {
		sessionepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.SessionEpoch = sessionepoch
	}

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = topics
	} else {
		topics, err := protocol.ReadArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = &topics
	}

	// ForgottenTopicsData (versions: 7+)
	if request.ApiVersion >= 7 {
		if isRequestFlexible(req.ApiVersion) {
			forgottentopicsdata, err := protocol.ReadNullableCompactArray(r, req.forgottenTopicsDataDecoder)
			if err != nil {
				return err
			}
			req.ForgottenTopicsData = forgottentopicsdata
		} else {
			forgottentopicsdata, err := protocol.ReadArray(r, req.forgottenTopicsDataDecoder)
			if err != nil {
				return err
			}
			req.ForgottenTopicsData = &forgottentopicsdata
		}
	}

	// RackId (versions: 11+)
	if request.ApiVersion >= 11 {
		if isRequestFlexible(req.ApiVersion) {
			rackid, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			req.RackId = &rackid
		} else {
			rackid, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			req.RackId = &rackid
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		err = protocol.ReadTaggedFields(r, req.taggedFieldsDecoder)
		if err != nil {
			return err
		}
	}

	return nil
}

func (req *FetchRequest) replicaStateEncoder(w io.Writer, value FetchRequestReplicaState) error {
	// ReplicaId (versions: 15+)
	if req.ApiVersion >= 15 {
		if err := protocol.WriteInt32(w, value.ReplicaId); err != nil {
			return err
		}
	}

	// ReplicaEpoch (versions: 15+)
	if req.ApiVersion >= 15 {
		if err := protocol.WriteInt64(w, value.ReplicaEpoch); err != nil {
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

func (req *FetchRequest) replicaStateDecoder(r io.Reader) (FetchRequestReplicaState, error) {
	fetchrequestreplicastate := FetchRequestReplicaState{}
	var err error

	// ReplicaId (versions: 15+)
	if req.ApiVersion >= 15 {
		replicaid, err := protocol.ReadInt32(r)
		if err != nil {
			return fetchrequestreplicastate, err
		}
		fetchrequestreplicastate.ReplicaId = replicaid
	}

	// ReplicaEpoch (versions: 15+)
	if req.ApiVersion >= 15 {
		replicaepoch, err := protocol.ReadInt64(r)
		if err != nil {
			return fetchrequestreplicastate, err
		}
		fetchrequestreplicastate.ReplicaEpoch = replicaepoch
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchrequestreplicastate, err
		}
		fetchrequestreplicastate.rawTaggedFields = &rawTaggedFields
	}

	return fetchrequestreplicastate, nil
}

func (req *FetchRequest) topicsEncoder(w io.Writer, value FetchRequestTopic) error {
	// Topic (versions: 0-12)
	if req.ApiVersion <= 12 {
		if isRequestFlexible(req.ApiVersion) {
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
	if req.ApiVersion >= 13 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.partitionsEncoder, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.partitionsEncoder, *value.Partitions); err != nil {
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

func (req *FetchRequest) topicsDecoder(r io.Reader) (FetchRequestTopic, error) {
	fetchrequesttopic := FetchRequestTopic{}
	var err error

	// Topic (versions: 0-12)
	if req.ApiVersion <= 12 {
		if isRequestFlexible(req.ApiVersion) {
			topic, err := protocol.ReadCompactString(r)
			if err != nil {
				return fetchrequesttopic, err
			}
			fetchrequesttopic.Topic = &topic
		} else {
			topic, err := protocol.ReadString(r)
			if err != nil {
				return fetchrequesttopic, err
			}
			fetchrequesttopic.Topic = &topic
		}
	}

	// TopicId (versions: 13+)
	if req.ApiVersion >= 13 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return fetchrequesttopic, err
		}
		fetchrequesttopic.TopicId = topicid
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return fetchrequesttopic, err
		}
		fetchrequesttopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return fetchrequesttopic, err
		}
		fetchrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchrequesttopic, err
		}
		fetchrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return fetchrequesttopic, nil
}

func (req *FetchRequest) partitionsEncoder(w io.Writer, value FetchRequestTopicPartition) error {
	// Partition (versions: 0+)
	if err := protocol.WriteInt32(w, value.Partition); err != nil {
		return err
	}

	// CurrentLeaderEpoch (versions: 9+)
	if req.ApiVersion >= 9 {
		if err := protocol.WriteInt32(w, value.CurrentLeaderEpoch); err != nil {
			return err
		}
	}

	// FetchOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.FetchOffset); err != nil {
		return err
	}

	// LastFetchedEpoch (versions: 12+)
	if req.ApiVersion >= 12 {
		if err := protocol.WriteInt32(w, value.LastFetchedEpoch); err != nil {
			return err
		}
	}

	// LogStartOffset (versions: 5+)
	if req.ApiVersion >= 5 {
		if err := protocol.WriteInt64(w, value.LogStartOffset); err != nil {
			return err
		}
	}

	// PartitionMaxBytes (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionMaxBytes); err != nil {
		return err
	}

	// ReplicaDirectoryId (versions: 17+)
	if !isRequestFlexible(req.ApiVersion) {
		if req.ApiVersion >= 17 {
			if err := protocol.WriteUUID(w, value.ReplicaDirectoryId); err != nil {
				return err
			}
		}
	}

	// HighWatermark (versions: 18+)
	if !isRequestFlexible(req.ApiVersion) {
		if req.ApiVersion >= 18 {
			if err := protocol.WriteInt64(w, value.HighWatermark); err != nil {
				return err
			}
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		taggedFields, err := req.taggedFieldsEncoderPartitions(value)
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (req *FetchRequest) partitionsDecoder(r io.Reader) (FetchRequestTopicPartition, error) {
	fetchrequesttopicpartition := FetchRequestTopicPartition{}
	var err error

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return fetchrequesttopicpartition, err
	}
	fetchrequesttopicpartition.Partition = partition

	// CurrentLeaderEpoch (versions: 9+)
	if req.ApiVersion >= 9 {
		currentleaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return fetchrequesttopicpartition, err
		}
		fetchrequesttopicpartition.CurrentLeaderEpoch = currentleaderepoch
	}

	// FetchOffset (versions: 0+)
	fetchoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return fetchrequesttopicpartition, err
	}
	fetchrequesttopicpartition.FetchOffset = fetchoffset

	// LastFetchedEpoch (versions: 12+)
	if req.ApiVersion >= 12 {
		lastfetchedepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return fetchrequesttopicpartition, err
		}
		fetchrequesttopicpartition.LastFetchedEpoch = lastfetchedepoch
	}

	// LogStartOffset (versions: 5+)
	if req.ApiVersion >= 5 {
		logstartoffset, err := protocol.ReadInt64(r)
		if err != nil {
			return fetchrequesttopicpartition, err
		}
		fetchrequesttopicpartition.LogStartOffset = logstartoffset
	}

	// PartitionMaxBytes (versions: 0+)
	partitionmaxbytes, err := protocol.ReadInt32(r)
	if err != nil {
		return fetchrequesttopicpartition, err
	}
	fetchrequesttopicpartition.PartitionMaxBytes = partitionmaxbytes

	// ReplicaDirectoryId (versions: 17+)
	if !isRequestFlexible(req.ApiVersion) {
		if req.ApiVersion >= 17 {
			replicadirectoryid, err := protocol.ReadUUID(r)
			if err != nil {
				return fetchrequesttopicpartition, err
			}
			fetchrequesttopicpartition.ReplicaDirectoryId = replicadirectoryid
		}
	}

	// HighWatermark (versions: 18+)
	if !isRequestFlexible(req.ApiVersion) {
		if req.ApiVersion >= 18 {
			highwatermark, err := protocol.ReadInt64(r)
			if err != nil {
				return fetchrequesttopicpartition, err
			}
			fetchrequesttopicpartition.HighWatermark = highwatermark
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		// Decode tagged fields
		err = protocol.ReadTaggedFields(r, func(r io.Reader, tag uint64, tagLength uint64) error {
			return req.taggedFieldsDecoderPartitions(r, tag, tagLength, &fetchrequesttopicpartition)
		})
		if err != nil {
			return fetchrequesttopicpartition, err
		}
	}

	return fetchrequesttopicpartition, nil
}

func (req *FetchRequest) taggedFieldsEncoderPartitions(value FetchRequestTopicPartition) ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if value.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*value.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 3+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	buf = bytes.NewBuffer(make([]byte, 0))
	if err := protocol.WriteUUID(buf, value.ReplicaDirectoryId); err != nil {
		return taggedFields, err
	}

	taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})

	// Tag 1
	buf = bytes.NewBuffer(make([]byte, 0))
	if err := protocol.WriteInt64(buf, value.HighWatermark); err != nil {
		return taggedFields, err
	}

	taggedFields = append(taggedFields, protocol.TaggedField{Tag: 1, Field: buf.Bytes()})

	// We append any raw tagged fields to the end of the array
	if value.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *value.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (req *FetchRequest) taggedFieldsDecoderPartitions(r io.Reader, tag uint64, tagLength uint64, value *FetchRequestTopicPartition) error {
	rawTaggedFields := make([]protocol.TaggedField, 0)

	switch tag {
	case 0:
		// ReplicaDirectoryId
		replicadirectoryid, err := protocol.ReadUUID(r)
		if err != nil {
			return err
		}
		value.ReplicaDirectoryId = replicadirectoryid
	case 1:
		// HighWatermark
		highwatermark, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		value.HighWatermark = highwatermark
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

func (req *FetchRequest) forgottenTopicsDataEncoder(w io.Writer, value FetchRequestForgottenTopicsData) error {
	// Topic (versions: 7-12)
	if req.ApiVersion >= 7 && req.ApiVersion <= 12 {
		if isRequestFlexible(req.ApiVersion) {
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
	if req.ApiVersion >= 13 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// Partitions (versions: 7+)
	if req.ApiVersion >= 7 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
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

func (req *FetchRequest) forgottenTopicsDataDecoder(r io.Reader) (FetchRequestForgottenTopicsData, error) {
	fetchrequestforgottentopicsdata := FetchRequestForgottenTopicsData{}
	var err error

	// Topic (versions: 7-12)
	if req.ApiVersion >= 7 && req.ApiVersion <= 12 {
		if isRequestFlexible(req.ApiVersion) {
			topic, err := protocol.ReadCompactString(r)
			if err != nil {
				return fetchrequestforgottentopicsdata, err
			}
			fetchrequestforgottentopicsdata.Topic = &topic
		} else {
			topic, err := protocol.ReadString(r)
			if err != nil {
				return fetchrequestforgottentopicsdata, err
			}
			fetchrequestforgottentopicsdata.Topic = &topic
		}
	}

	// TopicId (versions: 13+)
	if req.ApiVersion >= 13 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return fetchrequestforgottentopicsdata, err
		}
		fetchrequestforgottentopicsdata.TopicId = topicid
	}

	// Partitions (versions: 7+)
	if req.ApiVersion >= 7 {
		if isRequestFlexible(req.ApiVersion) {
			partitions, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
			if err != nil {
				return fetchrequestforgottentopicsdata, err
			}
			fetchrequestforgottentopicsdata.Partitions = partitions
		} else {
			partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
			if err != nil {
				return fetchrequestforgottentopicsdata, err
			}
			fetchrequestforgottentopicsdata.Partitions = &partitions
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchrequestforgottentopicsdata, err
		}
		fetchrequestforgottentopicsdata.rawTaggedFields = &rawTaggedFields
	}

	return fetchrequestforgottentopicsdata, nil
}

func (req *FetchRequest) taggedFieldsEncoder() ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if req.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*req.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 3+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	if req.ClusterId != nil {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := protocol.WriteNullableCompactString(buf, req.ClusterId); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})
	}

	// Tag 1
	if req.ReplicaState != nil {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := req.replicaStateEncoder(buf, *req.ReplicaState); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 1, Field: buf.Bytes()})
	}

	// We append any raw tagged fields to the end of the array
	if req.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *req.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (req *FetchRequest) taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {
	rawTaggedFields := make([]protocol.TaggedField, 0)

	switch tag {
	case 0:
		// ClusterId
		clusterid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.ClusterId = clusterid
	case 1:
		// ReplicaState
		replicastateVal, err := req.replicaStateDecoder(r)
		if err != nil {
			return err
		}
		req.ReplicaState = &replicastateVal
	default:
		// Decode as raw tags
		taggedField, err := protocol.ReadRawTaggedField(r)
		if err != nil {
			return err
		}
		rawTaggedFields = append(rawTaggedFields, taggedField)
	}

	// Set the raw tagged fields
	req.rawTaggedFields = &rawTaggedFields

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *FetchRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> FetchRequest:\n")
	if req.ClusterId != nil {
		fmt.Fprintf(w, "        ClusterId: %v\n", *req.ClusterId)
	} else {
		fmt.Fprintf(w, "        ClusterId: nil\n")
	}
	fmt.Fprintf(w, "        ReplicaId: %v\n", req.ReplicaId)
	fmt.Fprintf(w, "        ReplicaState:\n")
	if req.ReplicaState != nil {
		fmt.Fprintf(w, "%s", req.ReplicaState.PrettyPrint())
	} else {
		fmt.Fprintf(w, "            nil\n")
	}
	fmt.Fprintf(w, "        MaxWaitMs: %v\n", req.MaxWaitMs)
	fmt.Fprintf(w, "        MinBytes: %v\n", req.MinBytes)
	fmt.Fprintf(w, "        MaxBytes: %v\n", req.MaxBytes)
	fmt.Fprintf(w, "        IsolationLevel: %v\n", req.IsolationLevel)
	fmt.Fprintf(w, "        SessionId: %v\n", req.SessionId)
	fmt.Fprintf(w, "        SessionEpoch: %v\n", req.SessionEpoch)
	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}
	if req.ForgottenTopicsData != nil {
		fmt.Fprintf(w, "        ForgottenTopicsData:\n")
		for _, forgottentopicsdata := range *req.ForgottenTopicsData {
			fmt.Fprintf(w, "%s", forgottentopicsdata.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        ForgottenTopicsData: nil\n")
	}
	if req.RackId != nil {
		fmt.Fprintf(w, "        RackId: %v\n", *req.RackId)
	} else {
		fmt.Fprintf(w, "        RackId: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchRequestReplicaState) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ReplicaId: %v\n", value.ReplicaId)
	fmt.Fprintf(w, "            ReplicaEpoch: %v\n", value.ReplicaEpoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchRequestTopic) PrettyPrint() string {
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
func (value *FetchRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Partition: %v\n", value.Partition)
	fmt.Fprintf(w, "                CurrentLeaderEpoch: %v\n", value.CurrentLeaderEpoch)
	fmt.Fprintf(w, "                FetchOffset: %v\n", value.FetchOffset)
	fmt.Fprintf(w, "                LastFetchedEpoch: %v\n", value.LastFetchedEpoch)
	fmt.Fprintf(w, "                LogStartOffset: %v\n", value.LogStartOffset)
	fmt.Fprintf(w, "                PartitionMaxBytes: %v\n", value.PartitionMaxBytes)
	fmt.Fprintf(w, "                ReplicaDirectoryId: %v\n", value.ReplicaDirectoryId)
	fmt.Fprintf(w, "                HighWatermark: %v\n", value.HighWatermark)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchRequestForgottenTopicsData) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Topic != nil {
		fmt.Fprintf(w, "            Topic: %v\n", *value.Topic)
	} else {
		fmt.Fprintf(w, "            Topic: nil\n")
	}
	fmt.Fprintf(w, "            TopicId: %v\n", value.TopicId)
	if value.Partitions != nil {
		fmt.Fprintf(w, "            Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "            Partitions: nil\n")
	}

	return w.String()
}
