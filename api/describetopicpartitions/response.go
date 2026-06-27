package describetopicpartitions

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeTopicPartitionsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                      // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	Topics          *[]DescribeTopicPartitionsResponseTopic    // Each topic in the response. (versions: 0+)
	NextCursor      *DescribeTopicPartitionsResponseNextCursor // The next topic and partition index to fetch details for. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeTopicPartitionsResponseTopic struct {
	ErrorCode                 int16                                            // The topic error, or 0 if there was no error. (versions: 0+)
	Name                      *string                                          // The topic name. (versions: 0+, nullable: 0+)
	TopicId                   uuid.UUID                                        // The topic id. (versions: 0+)
	IsInternal                bool                                             // True if the topic is internal. (versions: 0+)
	Partitions                *[]DescribeTopicPartitionsResponseTopicPartition // Each partition in the topic. (versions: 0+)
	TopicAuthorizedOperations int32                                            // 32-bit bitfield to represent authorized operations for this topic. (versions: 0+)
	rawTaggedFields           *[]protocol.TaggedField
}

type DescribeTopicPartitionsResponseTopicPartition struct {
	ErrorCode              int16    // The partition error, or 0 if there was no error. (versions: 0+)
	PartitionIndex         int32    // The partition index. (versions: 0+)
	LeaderId               int32    // The ID of the leader broker. (versions: 0+)
	LeaderEpoch            int32    // The leader epoch of this partition. (versions: 0+)
	ReplicaNodes           *[]int32 // The set of all nodes that host this partition. (versions: 0+)
	IsrNodes               *[]int32 // The set of nodes that are in sync with the leader for this partition. (versions: 0+)
	EligibleLeaderReplicas *[]int32 // The new eligible leader replicas otherwise. (versions: 0+, nullable: 0+)
	LastKnownElr           *[]int32 // The last known ELR. (versions: 0+, nullable: 0+)
	OfflineReplicas        *[]int32 // The set of offline replicas of this partition. (versions: 0+)
	rawTaggedFields        *[]protocol.TaggedField
}

type DescribeTopicPartitionsResponseNextCursor struct {
	TopicName       *string // The name for the first topic to process. (versions: 0+)
	PartitionIndex  int32   // The partition index to start with. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *DescribeTopicPartitionsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("DescribeTopicPartitionsResponse.Topics must not be nil in version %d", res.ApiVersion)
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

	// NextCursor (versions: 0+)
	if res.NextCursor == nil {
		if err := protocol.WriteInt8(w, -1); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteInt8(w, 1); err != nil {
			return err
		}
		if err := res.nextCursorEncoder(w, *res.NextCursor); err != nil {
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
func (res *DescribeTopicPartitionsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeTopicPartitionsResponse.Read: response or its body is nil")
	}

	*res = DescribeTopicPartitionsResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

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

	// NextCursor (versions: 0+)
	nextcursorFlag, err := protocol.ReadInt8(r)
	if err != nil {
		return err
	}
	if nextcursorFlag >= 0 {
		nextcursor, err := res.nextCursorDecoder(r)
		if err != nil {
			return err
		}
		res.NextCursor = &nextcursor
	} else {
		res.NextCursor = nil
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

func (res *DescribeTopicPartitionsResponse) topicsEncoder(w io.Writer, value DescribeTopicPartitionsResponseTopic) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.Name); err != nil {
			return err
		}
	}

	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// IsInternal (versions: 0+)
	if err := protocol.WriteBool(w, value.IsInternal); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("DescribeTopicPartitionsResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

	// TopicAuthorizedOperations (versions: 0+)
	if err := protocol.WriteInt32(w, value.TopicAuthorizedOperations); err != nil {
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

func (res *DescribeTopicPartitionsResponse) topicsDecoder(r io.Reader) (DescribeTopicPartitionsResponseTopic, error) {
	describetopicpartitionsresponsetopic := DescribeTopicPartitionsResponseTopic{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	describetopicpartitionsresponsetopic.TopicAuthorizedOperations = -2147483648

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return describetopicpartitionsresponsetopic, err
	}
	describetopicpartitionsresponsetopic.ErrorCode = errorcode

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return describetopicpartitionsresponsetopic, err
		}
		describetopicpartitionsresponsetopic.Name = name
	} else {
		name, err := protocol.ReadNullableString(r)
		if err != nil {
			return describetopicpartitionsresponsetopic, err
		}
		describetopicpartitionsresponsetopic.Name = name
	}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return describetopicpartitionsresponsetopic, err
	}
	describetopicpartitionsresponsetopic.TopicId = topicid

	// IsInternal (versions: 0+)
	isinternal, err := protocol.ReadBool(r)
	if err != nil {
		return describetopicpartitionsresponsetopic, err
	}
	describetopicpartitionsresponsetopic.IsInternal = isinternal

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return describetopicpartitionsresponsetopic, err
		}
		describetopicpartitionsresponsetopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return describetopicpartitionsresponsetopic, err
		}
		describetopicpartitionsresponsetopic.Partitions = &partitions
	}

	// TopicAuthorizedOperations (versions: 0+)
	topicauthorizedoperations, err := protocol.ReadInt32(r)
	if err != nil {
		return describetopicpartitionsresponsetopic, err
	}
	describetopicpartitionsresponsetopic.TopicAuthorizedOperations = topicauthorizedoperations

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describetopicpartitionsresponsetopic, err
		}
		describetopicpartitionsresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return describetopicpartitionsresponsetopic, nil
}

func (res *DescribeTopicPartitionsResponse) partitionsEncoder(w io.Writer, value DescribeTopicPartitionsResponseTopicPartition) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// LeaderId (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderId); err != nil {
		return err
	}

	// LeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
		return err
	}

	// ReplicaNodes (versions: 0+)
	if value.ReplicaNodes == nil {
		return fmt.Errorf("DescribeTopicPartitionsResponseTopicPartition.ReplicaNodes must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.ReplicaNodes); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.ReplicaNodes); err != nil {
			return err
		}
	}

	// IsrNodes (versions: 0+)
	if value.IsrNodes == nil {
		return fmt.Errorf("DescribeTopicPartitionsResponseTopicPartition.IsrNodes must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.IsrNodes); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.IsrNodes); err != nil {
			return err
		}
	}

	// EligibleLeaderReplicas (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.EligibleLeaderReplicas); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, protocol.WriteInt32, value.EligibleLeaderReplicas); err != nil {
			return err
		}
	}

	// LastKnownElr (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.LastKnownElr); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, protocol.WriteInt32, value.LastKnownElr); err != nil {
			return err
		}
	}

	// OfflineReplicas (versions: 0+)
	if value.OfflineReplicas == nil {
		return fmt.Errorf("DescribeTopicPartitionsResponseTopicPartition.OfflineReplicas must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.OfflineReplicas); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.OfflineReplicas); err != nil {
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

func (res *DescribeTopicPartitionsResponse) partitionsDecoder(r io.Reader) (DescribeTopicPartitionsResponseTopicPartition, error) {
	describetopicpartitionsresponsetopicpartition := DescribeTopicPartitionsResponseTopicPartition{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	describetopicpartitionsresponsetopicpartition.LeaderEpoch = -1

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return describetopicpartitionsresponsetopicpartition, err
	}
	describetopicpartitionsresponsetopicpartition.ErrorCode = errorcode

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return describetopicpartitionsresponsetopicpartition, err
	}
	describetopicpartitionsresponsetopicpartition.PartitionIndex = partitionindex

	// LeaderId (versions: 0+)
	leaderid, err := protocol.ReadInt32(r)
	if err != nil {
		return describetopicpartitionsresponsetopicpartition, err
	}
	describetopicpartitionsresponsetopicpartition.LeaderId = leaderid

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return describetopicpartitionsresponsetopicpartition, err
	}
	describetopicpartitionsresponsetopicpartition.LeaderEpoch = leaderepoch

	// ReplicaNodes (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		replicanodes, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return describetopicpartitionsresponsetopicpartition, err
		}
		describetopicpartitionsresponsetopicpartition.ReplicaNodes = &replicanodes
	} else {
		replicanodes, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return describetopicpartitionsresponsetopicpartition, err
		}
		describetopicpartitionsresponsetopicpartition.ReplicaNodes = &replicanodes
	}

	// IsrNodes (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		isrnodes, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return describetopicpartitionsresponsetopicpartition, err
		}
		describetopicpartitionsresponsetopicpartition.IsrNodes = &isrnodes
	} else {
		isrnodes, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return describetopicpartitionsresponsetopicpartition, err
		}
		describetopicpartitionsresponsetopicpartition.IsrNodes = &isrnodes
	}

	// EligibleLeaderReplicas (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		eligibleleaderreplicas, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return describetopicpartitionsresponsetopicpartition, err
		}
		describetopicpartitionsresponsetopicpartition.EligibleLeaderReplicas = eligibleleaderreplicas
	} else {
		eligibleleaderreplicas, err := protocol.ReadNullableArray(r, protocol.ReadInt32)
		if err != nil {
			return describetopicpartitionsresponsetopicpartition, err
		}
		describetopicpartitionsresponsetopicpartition.EligibleLeaderReplicas = eligibleleaderreplicas
	}

	// LastKnownElr (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		lastknownelr, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return describetopicpartitionsresponsetopicpartition, err
		}
		describetopicpartitionsresponsetopicpartition.LastKnownElr = lastknownelr
	} else {
		lastknownelr, err := protocol.ReadNullableArray(r, protocol.ReadInt32)
		if err != nil {
			return describetopicpartitionsresponsetopicpartition, err
		}
		describetopicpartitionsresponsetopicpartition.LastKnownElr = lastknownelr
	}

	// OfflineReplicas (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		offlinereplicas, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return describetopicpartitionsresponsetopicpartition, err
		}
		describetopicpartitionsresponsetopicpartition.OfflineReplicas = &offlinereplicas
	} else {
		offlinereplicas, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return describetopicpartitionsresponsetopicpartition, err
		}
		describetopicpartitionsresponsetopicpartition.OfflineReplicas = &offlinereplicas
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describetopicpartitionsresponsetopicpartition, err
		}
		describetopicpartitionsresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return describetopicpartitionsresponsetopicpartition, nil
}

func (res *DescribeTopicPartitionsResponse) nextCursorEncoder(w io.Writer, value DescribeTopicPartitionsResponseNextCursor) error {
	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("DescribeTopicPartitionsResponseNextCursor.TopicName must not be nil in version %d", res.ApiVersion)
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

	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
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

func (res *DescribeTopicPartitionsResponse) nextCursorDecoder(r io.Reader) (DescribeTopicPartitionsResponseNextCursor, error) {
	describetopicpartitionsresponsenextcursor := DescribeTopicPartitionsResponseNextCursor{}

	// TopicName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return describetopicpartitionsresponsenextcursor, err
		}
		describetopicpartitionsresponsenextcursor.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return describetopicpartitionsresponsenextcursor, err
		}
		describetopicpartitionsresponsenextcursor.TopicName = &topicname
	}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return describetopicpartitionsresponsenextcursor, err
	}
	describetopicpartitionsresponsenextcursor.PartitionIndex = partitionindex

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describetopicpartitionsresponsenextcursor, err
		}
		describetopicpartitionsresponsenextcursor.rawTaggedFields = &rawTaggedFields
	}

	return describetopicpartitionsresponsenextcursor, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeTopicPartitionsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeTopicPartitionsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	if res.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *res.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	fmt.Fprintf(w, "        NextCursor:\n")
	if res.NextCursor != nil {
		fmt.Fprintf(w, "%s", res.NextCursor.PrettyPrint())
	} else {
		fmt.Fprintf(w, "            nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeTopicPartitionsResponseTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	fmt.Fprintf(w, "            TopicId: %v\n", value.TopicId)
	fmt.Fprintf(w, "            IsInternal: %v\n", value.IsInternal)

	if value.Partitions != nil {
		fmt.Fprintf(w, "            Partitions:\n")
		for _, partitions := range *value.Partitions {
			fmt.Fprintf(w, "%s", partitions.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Partitions: nil\n")
	}

	fmt.Fprintf(w, "            TopicAuthorizedOperations: %v\n", value.TopicAuthorizedOperations)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeTopicPartitionsResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)
	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)

	if value.ReplicaNodes != nil {
		fmt.Fprintf(w, "                ReplicaNodes: %v\n", *value.ReplicaNodes)
	} else {
		fmt.Fprintf(w, "                ReplicaNodes: nil\n")
	}

	if value.IsrNodes != nil {
		fmt.Fprintf(w, "                IsrNodes: %v\n", *value.IsrNodes)
	} else {
		fmt.Fprintf(w, "                IsrNodes: nil\n")
	}

	if value.EligibleLeaderReplicas != nil {
		fmt.Fprintf(w, "                EligibleLeaderReplicas: %v\n", *value.EligibleLeaderReplicas)
	} else {
		fmt.Fprintf(w, "                EligibleLeaderReplicas: nil\n")
	}

	if value.LastKnownElr != nil {
		fmt.Fprintf(w, "                LastKnownElr: %v\n", *value.LastKnownElr)
	} else {
		fmt.Fprintf(w, "                LastKnownElr: nil\n")
	}

	if value.OfflineReplicas != nil {
		fmt.Fprintf(w, "                OfflineReplicas: %v\n", *value.OfflineReplicas)
	} else {
		fmt.Fprintf(w, "                OfflineReplicas: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeTopicPartitionsResponseNextCursor) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TopicName != nil {
		fmt.Fprintf(w, "            TopicName: %v\n", *value.TopicName)
	} else {
		fmt.Fprintf(w, "            TopicName: nil\n")
	}

	fmt.Fprintf(w, "            PartitionIndex: %v\n", value.PartitionIndex)

	return w.String()
}
