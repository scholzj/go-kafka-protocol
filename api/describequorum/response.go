package describequorum

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeQuorumResponse struct {
	ApiVersion      int16
	ErrorCode       int16                          // The top level error code. (versions: 0+)
	ErrorMessage    *string                        // The error message, or null if there was no error. (versions: 2+, nullable: 2+)
	Topics          *[]DescribeQuorumResponseTopic // The response from the describe quorum API. (versions: 0+)
	Nodes           *[]DescribeQuorumResponseNode  // The nodes in the quorum. (versions: 2+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeQuorumResponseTopic struct {
	TopicName       *string                                 // The topic name. (versions: 0+)
	Partitions      *[]DescribeQuorumResponseTopicPartition // The partition data. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeQuorumResponseTopicPartition struct {
	PartitionIndex  int32                                               // The partition index. (versions: 0+)
	ErrorCode       int16                                               // The partition error code. (versions: 0+)
	ErrorMessage    *string                                             // The error message, or null if there was no error. (versions: 2+, nullable: 2+)
	LeaderId        int32                                               // The ID of the current leader or -1 if the leader is unknown. (versions: 0+)
	LeaderEpoch     int32                                               // The latest known leader epoch. (versions: 0+)
	HighWatermark   int64                                               // The high water mark. (versions: 0+)
	CurrentVoters   *[]DescribeQuorumResponseTopicPartitionCurrentVoter // The current voters of the partition. (versions: 0+)
	Observers       *[]DescribeQuorumResponseTopicPartitionObserver     // The observers of the partition. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeQuorumResponseTopicPartitionCurrentVoter struct {
	ReplicaId             int32     // The ID of the replica. (versions: 0+)
	ReplicaDirectoryId    uuid.UUID // The replica directory ID of the replica. (versions: 2+)
	LogEndOffset          int64     // The last known log end offset of the follower or -1 if it is unknown. (versions: 0+)
	LastFetchTimestamp    int64     // The last known leader wall clock time time when a follower fetched from the leader. This is reported as -1 both for the current leader or if it is unknown for a voter. (versions: 1+)
	LastCaughtUpTimestamp int64     // The leader wall clock append time of the offset for which the follower made the most recent fetch request. This is reported as the current time for the leader and -1 if unknown for a voter. (versions: 1+)
	rawTaggedFields       *[]protocol.TaggedField
}

type DescribeQuorumResponseTopicPartitionObserver struct {
	ReplicaId             int32     // The ID of the replica. (versions: 0+)
	ReplicaDirectoryId    uuid.UUID // The replica directory ID of the replica. (versions: 2+)
	LogEndOffset          int64     // The last known log end offset of the follower or -1 if it is unknown. (versions: 0+)
	LastFetchTimestamp    int64     // The last known leader wall clock time time when a follower fetched from the leader. This is reported as -1 both for the current leader or if it is unknown for a voter. (versions: 1+)
	LastCaughtUpTimestamp int64     // The leader wall clock append time of the offset for which the follower made the most recent fetch request. This is reported as the current time for the leader and -1 if unknown for a voter. (versions: 1+)
	rawTaggedFields       *[]protocol.TaggedField
}

type DescribeQuorumResponseNode struct {
	NodeId          int32                                 // The ID of the associated node. (versions: 2+)
	Listeners       *[]DescribeQuorumResponseNodeListener // The listeners of this controller. (versions: 2+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeQuorumResponseNodeListener struct {
	Name            *string // The name of the endpoint. (versions: 2+)
	Host            *string // The hostname. (versions: 2+)
	Port            uint16  // The port. (versions: 2+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *DescribeQuorumResponse) Write(w io.Writer) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 2+)
	if res.ApiVersion >= 2 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, res.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, res.ErrorMessage); err != nil {
				return err
			}
		}
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("DescribeQuorumResponse.Topics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.topicsEncoder, res.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicsEncoder, *res.Topics); err != nil {
			return err
		}
	}

	// Nodes (versions: 2+)
	if res.ApiVersion >= 2 {
		if res.Nodes == nil {
			return fmt.Errorf("DescribeQuorumResponse.Nodes must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.nodesEncoder, res.Nodes); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, res.nodesEncoder, *res.Nodes); err != nil {
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
func (res *DescribeQuorumResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeQuorumResponse.Read: response or its body is nil")
	}

	*res = DescribeQuorumResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// ErrorMessage (versions: 2+)
	if res.ApiVersion >= 2 {
		if isResponseFlexible(res.ApiVersion) {
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
	}

	// Topics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topics, err := protocol.ReadCompactArray(r, res.topicsDecoder)
		if err != nil {
			return err
		}
		res.Topics = &topics
	} else {
		topics, err := protocol.ReadArray(r, res.topicsDecoder)
		if err != nil {
			return err
		}
		res.Topics = &topics
	}

	// Nodes (versions: 2+)
	if res.ApiVersion >= 2 {
		if isResponseFlexible(res.ApiVersion) {
			nodes, err := protocol.ReadCompactArray(r, res.nodesDecoder)
			if err != nil {
				return err
			}
			res.Nodes = &nodes
		} else {
			nodes, err := protocol.ReadArray(r, res.nodesDecoder)
			if err != nil {
				return err
			}
			res.Nodes = &nodes
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

func (res *DescribeQuorumResponse) topicsEncoder(w io.Writer, value DescribeQuorumResponseTopic) error {
	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("DescribeQuorumResponseTopic.TopicName must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.TopicName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.TopicName); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("DescribeQuorumResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
	}
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

func (res *DescribeQuorumResponse) topicsDecoder(r io.Reader) (DescribeQuorumResponseTopic, error) {
	describequorumresponsetopic := DescribeQuorumResponseTopic{}

	// TopicName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return describequorumresponsetopic, err
		}
		describequorumresponsetopic.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return describequorumresponsetopic, err
		}
		describequorumresponsetopic.TopicName = &topicname
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return describequorumresponsetopic, err
		}
		describequorumresponsetopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return describequorumresponsetopic, err
		}
		describequorumresponsetopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describequorumresponsetopic, err
		}
		describequorumresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return describequorumresponsetopic, nil
}

func (res *DescribeQuorumResponse) partitionsEncoder(w io.Writer, value DescribeQuorumResponseTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 2+)
	if res.ApiVersion >= 2 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.ErrorMessage); err != nil {
				return err
			}
		}
	}

	// LeaderId (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderId); err != nil {
		return err
	}

	// LeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
		return err
	}

	// HighWatermark (versions: 0+)
	if err := protocol.WriteInt64(w, value.HighWatermark); err != nil {
		return err
	}

	// CurrentVoters (versions: 0+)
	if value.CurrentVoters == nil {
		return fmt.Errorf("DescribeQuorumResponseTopicPartition.CurrentVoters must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.currentVotersEncoder, value.CurrentVoters); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.currentVotersEncoder, *value.CurrentVoters); err != nil {
			return err
		}
	}

	// Observers (versions: 0+)
	if value.Observers == nil {
		return fmt.Errorf("DescribeQuorumResponseTopicPartition.Observers must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.observersEncoder, value.Observers); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.observersEncoder, *value.Observers); err != nil {
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

func (res *DescribeQuorumResponse) partitionsDecoder(r io.Reader) (DescribeQuorumResponseTopicPartition, error) {
	describequorumresponsetopicpartition := DescribeQuorumResponseTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return describequorumresponsetopicpartition, err
	}
	describequorumresponsetopicpartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return describequorumresponsetopicpartition, err
	}
	describequorumresponsetopicpartition.ErrorCode = errorcode

	// ErrorMessage (versions: 2+)
	if res.ApiVersion >= 2 {
		if isResponseFlexible(res.ApiVersion) {
			errormessage, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return describequorumresponsetopicpartition, err
			}
			describequorumresponsetopicpartition.ErrorMessage = errormessage
		} else {
			errormessage, err := protocol.ReadNullableString(r)
			if err != nil {
				return describequorumresponsetopicpartition, err
			}
			describequorumresponsetopicpartition.ErrorMessage = errormessage
		}
	}

	// LeaderId (versions: 0+)
	leaderid, err := protocol.ReadInt32(r)
	if err != nil {
		return describequorumresponsetopicpartition, err
	}
	describequorumresponsetopicpartition.LeaderId = leaderid

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return describequorumresponsetopicpartition, err
	}
	describequorumresponsetopicpartition.LeaderEpoch = leaderepoch

	// HighWatermark (versions: 0+)
	highwatermark, err := protocol.ReadInt64(r)
	if err != nil {
		return describequorumresponsetopicpartition, err
	}
	describequorumresponsetopicpartition.HighWatermark = highwatermark

	// CurrentVoters (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		currentvoters, err := protocol.ReadCompactArray(r, res.currentVotersDecoder)
		if err != nil {
			return describequorumresponsetopicpartition, err
		}
		describequorumresponsetopicpartition.CurrentVoters = &currentvoters
	} else {
		currentvoters, err := protocol.ReadArray(r, res.currentVotersDecoder)
		if err != nil {
			return describequorumresponsetopicpartition, err
		}
		describequorumresponsetopicpartition.CurrentVoters = &currentvoters
	}

	// Observers (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		observers, err := protocol.ReadCompactArray(r, res.observersDecoder)
		if err != nil {
			return describequorumresponsetopicpartition, err
		}
		describequorumresponsetopicpartition.Observers = &observers
	} else {
		observers, err := protocol.ReadArray(r, res.observersDecoder)
		if err != nil {
			return describequorumresponsetopicpartition, err
		}
		describequorumresponsetopicpartition.Observers = &observers
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describequorumresponsetopicpartition, err
		}
		describequorumresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return describequorumresponsetopicpartition, nil
}

func (res *DescribeQuorumResponse) currentVotersEncoder(w io.Writer, value DescribeQuorumResponseTopicPartitionCurrentVoter) error {
	// ReplicaId (versions: 0+)
	if err := protocol.WriteInt32(w, value.ReplicaId); err != nil {
		return err
	}

	// ReplicaDirectoryId (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteUUID(w, value.ReplicaDirectoryId); err != nil {
			return err
		}
	}

	// LogEndOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.LogEndOffset); err != nil {
		return err
	}

	// LastFetchTimestamp (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt64(w, value.LastFetchTimestamp); err != nil {
			return err
		}
	}

	// LastCaughtUpTimestamp (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt64(w, value.LastCaughtUpTimestamp); err != nil {
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

func (res *DescribeQuorumResponse) currentVotersDecoder(r io.Reader) (DescribeQuorumResponseTopicPartitionCurrentVoter, error) {
	describequorumresponsetopicpartitioncurrentvoter := DescribeQuorumResponseTopicPartitionCurrentVoter{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	describequorumresponsetopicpartitioncurrentvoter.LastFetchTimestamp = -1
	describequorumresponsetopicpartitioncurrentvoter.LastCaughtUpTimestamp = -1

	// ReplicaId (versions: 0+)
	replicaid, err := protocol.ReadInt32(r)
	if err != nil {
		return describequorumresponsetopicpartitioncurrentvoter, err
	}
	describequorumresponsetopicpartitioncurrentvoter.ReplicaId = replicaid

	// ReplicaDirectoryId (versions: 2+)
	if res.ApiVersion >= 2 {
		replicadirectoryid, err := protocol.ReadUUID(r)
		if err != nil {
			return describequorumresponsetopicpartitioncurrentvoter, err
		}
		describequorumresponsetopicpartitioncurrentvoter.ReplicaDirectoryId = replicadirectoryid
	}

	// LogEndOffset (versions: 0+)
	logendoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return describequorumresponsetopicpartitioncurrentvoter, err
	}
	describequorumresponsetopicpartitioncurrentvoter.LogEndOffset = logendoffset

	// LastFetchTimestamp (versions: 1+)
	if res.ApiVersion >= 1 {
		lastfetchtimestamp, err := protocol.ReadInt64(r)
		if err != nil {
			return describequorumresponsetopicpartitioncurrentvoter, err
		}
		describequorumresponsetopicpartitioncurrentvoter.LastFetchTimestamp = lastfetchtimestamp
	}

	// LastCaughtUpTimestamp (versions: 1+)
	if res.ApiVersion >= 1 {
		lastcaughtuptimestamp, err := protocol.ReadInt64(r)
		if err != nil {
			return describequorumresponsetopicpartitioncurrentvoter, err
		}
		describequorumresponsetopicpartitioncurrentvoter.LastCaughtUpTimestamp = lastcaughtuptimestamp
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describequorumresponsetopicpartitioncurrentvoter, err
		}
		describequorumresponsetopicpartitioncurrentvoter.rawTaggedFields = &rawTaggedFields
	}

	return describequorumresponsetopicpartitioncurrentvoter, nil
}

func (res *DescribeQuorumResponse) observersEncoder(w io.Writer, value DescribeQuorumResponseTopicPartitionObserver) error {
	// ReplicaId (versions: 0+)
	if err := protocol.WriteInt32(w, value.ReplicaId); err != nil {
		return err
	}

	// ReplicaDirectoryId (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteUUID(w, value.ReplicaDirectoryId); err != nil {
			return err
		}
	}

	// LogEndOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.LogEndOffset); err != nil {
		return err
	}

	// LastFetchTimestamp (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt64(w, value.LastFetchTimestamp); err != nil {
			return err
		}
	}

	// LastCaughtUpTimestamp (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt64(w, value.LastCaughtUpTimestamp); err != nil {
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

func (res *DescribeQuorumResponse) observersDecoder(r io.Reader) (DescribeQuorumResponseTopicPartitionObserver, error) {
	describequorumresponsetopicpartitionobserver := DescribeQuorumResponseTopicPartitionObserver{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	describequorumresponsetopicpartitionobserver.LastFetchTimestamp = -1
	describequorumresponsetopicpartitionobserver.LastCaughtUpTimestamp = -1

	// ReplicaId (versions: 0+)
	replicaid, err := protocol.ReadInt32(r)
	if err != nil {
		return describequorumresponsetopicpartitionobserver, err
	}
	describequorumresponsetopicpartitionobserver.ReplicaId = replicaid

	// ReplicaDirectoryId (versions: 2+)
	if res.ApiVersion >= 2 {
		replicadirectoryid, err := protocol.ReadUUID(r)
		if err != nil {
			return describequorumresponsetopicpartitionobserver, err
		}
		describequorumresponsetopicpartitionobserver.ReplicaDirectoryId = replicadirectoryid
	}

	// LogEndOffset (versions: 0+)
	logendoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return describequorumresponsetopicpartitionobserver, err
	}
	describequorumresponsetopicpartitionobserver.LogEndOffset = logendoffset

	// LastFetchTimestamp (versions: 1+)
	if res.ApiVersion >= 1 {
		lastfetchtimestamp, err := protocol.ReadInt64(r)
		if err != nil {
			return describequorumresponsetopicpartitionobserver, err
		}
		describequorumresponsetopicpartitionobserver.LastFetchTimestamp = lastfetchtimestamp
	}

	// LastCaughtUpTimestamp (versions: 1+)
	if res.ApiVersion >= 1 {
		lastcaughtuptimestamp, err := protocol.ReadInt64(r)
		if err != nil {
			return describequorumresponsetopicpartitionobserver, err
		}
		describequorumresponsetopicpartitionobserver.LastCaughtUpTimestamp = lastcaughtuptimestamp
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describequorumresponsetopicpartitionobserver, err
		}
		describequorumresponsetopicpartitionobserver.rawTaggedFields = &rawTaggedFields
	}

	return describequorumresponsetopicpartitionobserver, nil
}

func (res *DescribeQuorumResponse) nodesEncoder(w io.Writer, value DescribeQuorumResponseNode) error {
	// NodeId (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteInt32(w, value.NodeId); err != nil {
			return err
		}
	}

	// Listeners (versions: 2+)
	if res.ApiVersion >= 2 {
		if value.Listeners == nil {
			return fmt.Errorf("DescribeQuorumResponseNode.Listeners must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.listenersEncoder, value.Listeners); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, res.listenersEncoder, *value.Listeners); err != nil {
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

func (res *DescribeQuorumResponse) nodesDecoder(r io.Reader) (DescribeQuorumResponseNode, error) {
	describequorumresponsenode := DescribeQuorumResponseNode{}

	// NodeId (versions: 2+)
	if res.ApiVersion >= 2 {
		nodeid, err := protocol.ReadInt32(r)
		if err != nil {
			return describequorumresponsenode, err
		}
		describequorumresponsenode.NodeId = nodeid
	}

	// Listeners (versions: 2+)
	if res.ApiVersion >= 2 {
		if isResponseFlexible(res.ApiVersion) {
			listeners, err := protocol.ReadCompactArray(r, res.listenersDecoder)
			if err != nil {
				return describequorumresponsenode, err
			}
			describequorumresponsenode.Listeners = &listeners
		} else {
			listeners, err := protocol.ReadArray(r, res.listenersDecoder)
			if err != nil {
				return describequorumresponsenode, err
			}
			describequorumresponsenode.Listeners = &listeners
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describequorumresponsenode, err
		}
		describequorumresponsenode.rawTaggedFields = &rawTaggedFields
	}

	return describequorumresponsenode, nil
}

func (res *DescribeQuorumResponse) listenersEncoder(w io.Writer, value DescribeQuorumResponseNodeListener) error {
	// Name (versions: 2+)
	if res.ApiVersion >= 2 {
		if value.Name == nil {
			return fmt.Errorf("DescribeQuorumResponseNodeListener.Name must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.Name); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.Name); err != nil {
				return err
			}
		}
	}

	// Host (versions: 2+)
	if res.ApiVersion >= 2 {
		if value.Host == nil {
			return fmt.Errorf("DescribeQuorumResponseNodeListener.Host must not be nil in version %d", res.ApiVersion)
		}
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

	// Port (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteUint16(w, value.Port); err != nil {
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

func (res *DescribeQuorumResponse) listenersDecoder(r io.Reader) (DescribeQuorumResponseNodeListener, error) {
	describequorumresponsenodelistener := DescribeQuorumResponseNodeListener{}

	// Name (versions: 2+)
	if res.ApiVersion >= 2 {
		if isResponseFlexible(res.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return describequorumresponsenodelistener, err
			}
			describequorumresponsenodelistener.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return describequorumresponsenodelistener, err
			}
			describequorumresponsenodelistener.Name = &name
		}
	}

	// Host (versions: 2+)
	if res.ApiVersion >= 2 {
		if isResponseFlexible(res.ApiVersion) {
			host, err := protocol.ReadCompactString(r)
			if err != nil {
				return describequorumresponsenodelistener, err
			}
			describequorumresponsenodelistener.Host = &host
		} else {
			host, err := protocol.ReadString(r)
			if err != nil {
				return describequorumresponsenodelistener, err
			}
			describequorumresponsenodelistener.Host = &host
		}
	}

	// Port (versions: 2+)
	if res.ApiVersion >= 2 {
		port, err := protocol.ReadUInt16(r)
		if err != nil {
			return describequorumresponsenodelistener, err
		}
		describequorumresponsenodelistener.Port = port
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describequorumresponsenodelistener, err
		}
		describequorumresponsenodelistener.rawTaggedFields = &rawTaggedFields
	}

	return describequorumresponsenodelistener, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeQuorumResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeQuorumResponse:\n")
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.ErrorMessage != nil {
		fmt.Fprintf(w, "        ErrorMessage: %v\n", *res.ErrorMessage)
	} else {
		fmt.Fprintf(w, "        ErrorMessage: nil\n")
	}

	if res.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *res.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	if res.Nodes != nil {
		fmt.Fprintf(w, "        Nodes:\n")
		for _, nodes := range *res.Nodes {
			fmt.Fprintf(w, "%s", nodes.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Nodes: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeQuorumResponseTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TopicName != nil {
		fmt.Fprintf(w, "            TopicName: %v\n", *value.TopicName)
	} else {
		fmt.Fprintf(w, "            TopicName: nil\n")
	}

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
func (value *DescribeQuorumResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                ErrorMessage: nil\n")
	}

	fmt.Fprintf(w, "                LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)
	fmt.Fprintf(w, "                HighWatermark: %v\n", value.HighWatermark)

	if value.CurrentVoters != nil {
		fmt.Fprintf(w, "                CurrentVoters:\n")
		for _, currentvoters := range *value.CurrentVoters {
			fmt.Fprintf(w, "%s", currentvoters.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                CurrentVoters: nil\n")
	}

	if value.Observers != nil {
		fmt.Fprintf(w, "                Observers:\n")
		for _, observers := range *value.Observers {
			fmt.Fprintf(w, "%s", observers.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                Observers: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeQuorumResponseTopicPartitionCurrentVoter) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    ReplicaId: %v\n", value.ReplicaId)
	fmt.Fprintf(w, "                    ReplicaDirectoryId: %v\n", value.ReplicaDirectoryId)
	fmt.Fprintf(w, "                    LogEndOffset: %v\n", value.LogEndOffset)
	fmt.Fprintf(w, "                    LastFetchTimestamp: %v\n", value.LastFetchTimestamp)
	fmt.Fprintf(w, "                    LastCaughtUpTimestamp: %v\n", value.LastCaughtUpTimestamp)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeQuorumResponseTopicPartitionObserver) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    ReplicaId: %v\n", value.ReplicaId)
	fmt.Fprintf(w, "                    ReplicaDirectoryId: %v\n", value.ReplicaDirectoryId)
	fmt.Fprintf(w, "                    LogEndOffset: %v\n", value.LogEndOffset)
	fmt.Fprintf(w, "                    LastFetchTimestamp: %v\n", value.LastFetchTimestamp)
	fmt.Fprintf(w, "                    LastCaughtUpTimestamp: %v\n", value.LastCaughtUpTimestamp)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeQuorumResponseNode) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            NodeId: %v\n", value.NodeId)

	if value.Listeners != nil {
		fmt.Fprintf(w, "            Listeners:\n")
		for _, listeners := range *value.Listeners {
			fmt.Fprintf(w, "%s", listeners.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Listeners: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeQuorumResponseNodeListener) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

	if value.Host != nil {
		fmt.Fprintf(w, "                Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "                Host: nil\n")
	}

	fmt.Fprintf(w, "                Port: %v\n", value.Port)

	return w.String()
}
