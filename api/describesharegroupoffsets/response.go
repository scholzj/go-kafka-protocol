package describesharegroupoffsets

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeShareGroupOffsetsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                     // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	Groups          *[]DescribeShareGroupOffsetsResponseGroup // The results for each group. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeShareGroupOffsetsResponseGroup struct {
	GroupId         *string                                        // The group identifier. (versions: 0+)
	Topics          *[]DescribeShareGroupOffsetsResponseGroupTopic // The results for each topic. (versions: 0+)
	ErrorCode       int16                                          // The group-level error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage    *string                                        // The group-level error message, or null if there was no error. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeShareGroupOffsetsResponseGroupTopic struct {
	TopicName       *string                                                 // The topic name. (versions: 0+)
	TopicId         uuid.UUID                                               // The unique topic ID. (versions: 0+)
	Partitions      *[]DescribeShareGroupOffsetsResponseGroupTopicPartition // (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeShareGroupOffsetsResponseGroupTopicPartition struct {
	PartitionIndex  int32   // The partition index. (versions: 0+)
	StartOffset     int64   // The share-partition start offset. (versions: 0+)
	LeaderEpoch     int32   // The leader epoch of the partition. (versions: 0+)
	Lag             int64   // The share-partition lag. (versions: 1+)
	ErrorCode       int16   // The partition-level error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage    *string // The partition-level error message, or null if there was no error. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *DescribeShareGroupOffsetsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// Groups (versions: 0+)
	if res.Groups == nil {
		return fmt.Errorf("DescribeShareGroupOffsetsResponse.Groups must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.groupsEncoder, res.Groups); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.groupsEncoder, *res.Groups); err != nil {
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
func (res *DescribeShareGroupOffsetsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeShareGroupOffsetsResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// Groups (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		groups, err := protocol.ReadNullableCompactArray(r, res.groupsDecoder)
		if err != nil {
			return err
		}
		res.Groups = groups
	} else {
		groups, err := protocol.ReadArray(r, res.groupsDecoder)
		if err != nil {
			return err
		}
		res.Groups = &groups
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

func (res *DescribeShareGroupOffsetsResponse) groupsEncoder(w io.Writer, value DescribeShareGroupOffsetsResponseGroup) error {
	// GroupId (versions: 0+)
	if value.GroupId == nil {
		return fmt.Errorf("DescribeShareGroupOffsetsResponseGroup.GroupId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.GroupId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.GroupId); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if value.Topics == nil {
		return fmt.Errorf("DescribeShareGroupOffsetsResponseGroup.Topics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.topicsEncoder, value.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicsEncoder, *value.Topics); err != nil {
			return err
		}
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

func (res *DescribeShareGroupOffsetsResponse) groupsDecoder(r io.Reader) (DescribeShareGroupOffsetsResponseGroup, error) {
	describesharegroupoffsetsresponsegroup := DescribeShareGroupOffsetsResponseGroup{}

	// GroupId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		groupid, err := protocol.ReadCompactString(r)
		if err != nil {
			return describesharegroupoffsetsresponsegroup, err
		}
		describesharegroupoffsetsresponsegroup.GroupId = &groupid
	} else {
		groupid, err := protocol.ReadString(r)
		if err != nil {
			return describesharegroupoffsetsresponsegroup, err
		}
		describesharegroupoffsetsresponsegroup.GroupId = &groupid
	}

	// Topics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, res.topicsDecoder)
		if err != nil {
			return describesharegroupoffsetsresponsegroup, err
		}
		describesharegroupoffsetsresponsegroup.Topics = topics
	} else {
		topics, err := protocol.ReadArray(r, res.topicsDecoder)
		if err != nil {
			return describesharegroupoffsetsresponsegroup, err
		}
		describesharegroupoffsetsresponsegroup.Topics = &topics
	}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return describesharegroupoffsetsresponsegroup, err
	}
	describesharegroupoffsetsresponsegroup.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return describesharegroupoffsetsresponsegroup, err
		}
		describesharegroupoffsetsresponsegroup.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return describesharegroupoffsetsresponsegroup, err
		}
		describesharegroupoffsetsresponsegroup.ErrorMessage = errormessage
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describesharegroupoffsetsresponsegroup, err
		}
		describesharegroupoffsetsresponsegroup.rawTaggedFields = &rawTaggedFields
	}

	return describesharegroupoffsetsresponsegroup, nil
}

func (res *DescribeShareGroupOffsetsResponse) topicsEncoder(w io.Writer, value DescribeShareGroupOffsetsResponseGroupTopic) error {
	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("DescribeShareGroupOffsetsResponseGroupTopic.TopicName must not be nil in version %d", res.ApiVersion)
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

	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("DescribeShareGroupOffsetsResponseGroupTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *DescribeShareGroupOffsetsResponse) topicsDecoder(r io.Reader) (DescribeShareGroupOffsetsResponseGroupTopic, error) {
	describesharegroupoffsetsresponsegrouptopic := DescribeShareGroupOffsetsResponseGroupTopic{}

	// TopicName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return describesharegroupoffsetsresponsegrouptopic, err
		}
		describesharegroupoffsetsresponsegrouptopic.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return describesharegroupoffsetsresponsegrouptopic, err
		}
		describesharegroupoffsetsresponsegrouptopic.TopicName = &topicname
	}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return describesharegroupoffsetsresponsegrouptopic, err
	}
	describesharegroupoffsetsresponsegrouptopic.TopicId = topicid

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return describesharegroupoffsetsresponsegrouptopic, err
		}
		describesharegroupoffsetsresponsegrouptopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return describesharegroupoffsetsresponsegrouptopic, err
		}
		describesharegroupoffsetsresponsegrouptopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describesharegroupoffsetsresponsegrouptopic, err
		}
		describesharegroupoffsetsresponsegrouptopic.rawTaggedFields = &rawTaggedFields
	}

	return describesharegroupoffsetsresponsegrouptopic, nil
}

func (res *DescribeShareGroupOffsetsResponse) partitionsEncoder(w io.Writer, value DescribeShareGroupOffsetsResponseGroupTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// StartOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.StartOffset); err != nil {
		return err
	}

	// LeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
		return err
	}

	// Lag (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt64(w, value.Lag); err != nil {
			return err
		}
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

func (res *DescribeShareGroupOffsetsResponse) partitionsDecoder(r io.Reader) (DescribeShareGroupOffsetsResponseGroupTopicPartition, error) {
	describesharegroupoffsetsresponsegrouptopicpartition := DescribeShareGroupOffsetsResponseGroupTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return describesharegroupoffsetsresponsegrouptopicpartition, err
	}
	describesharegroupoffsetsresponsegrouptopicpartition.PartitionIndex = partitionindex

	// StartOffset (versions: 0+)
	startoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return describesharegroupoffsetsresponsegrouptopicpartition, err
	}
	describesharegroupoffsetsresponsegrouptopicpartition.StartOffset = startoffset

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return describesharegroupoffsetsresponsegrouptopicpartition, err
	}
	describesharegroupoffsetsresponsegrouptopicpartition.LeaderEpoch = leaderepoch

	// Lag (versions: 1+)
	if res.ApiVersion >= 1 {
		lag, err := protocol.ReadInt64(r)
		if err != nil {
			return describesharegroupoffsetsresponsegrouptopicpartition, err
		}
		describesharegroupoffsetsresponsegrouptopicpartition.Lag = lag
	}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return describesharegroupoffsetsresponsegrouptopicpartition, err
	}
	describesharegroupoffsetsresponsegrouptopicpartition.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return describesharegroupoffsetsresponsegrouptopicpartition, err
		}
		describesharegroupoffsetsresponsegrouptopicpartition.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return describesharegroupoffsetsresponsegrouptopicpartition, err
		}
		describesharegroupoffsetsresponsegrouptopicpartition.ErrorMessage = errormessage
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describesharegroupoffsetsresponsegrouptopicpartition, err
		}
		describesharegroupoffsetsresponsegrouptopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return describesharegroupoffsetsresponsegrouptopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeShareGroupOffsetsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeShareGroupOffsetsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	if res.Groups != nil {
		fmt.Fprintf(w, "        Groups:\n")
		for _, groups := range *res.Groups {
			fmt.Fprintf(w, "%s", groups.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Groups: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeShareGroupOffsetsResponseGroup) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.GroupId != nil {
		fmt.Fprintf(w, "            GroupId: %v\n", *value.GroupId)
	} else {
		fmt.Fprintf(w, "            GroupId: nil\n")
	}

	if value.Topics != nil {
		fmt.Fprintf(w, "            Topics:\n")
		for _, topics := range *value.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Topics: nil\n")
	}

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeShareGroupOffsetsResponseGroupTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TopicName != nil {
		fmt.Fprintf(w, "                TopicName: %v\n", *value.TopicName)
	} else {
		fmt.Fprintf(w, "                TopicName: nil\n")
	}

	fmt.Fprintf(w, "                TopicId: %v\n", value.TopicId)

	if value.Partitions != nil {
		fmt.Fprintf(w, "                Partitions:\n")
		for _, partitions := range *value.Partitions {
			fmt.Fprintf(w, "%s", partitions.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeShareGroupOffsetsResponseGroupTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                    StartOffset: %v\n", value.StartOffset)
	fmt.Fprintf(w, "                    LeaderEpoch: %v\n", value.LeaderEpoch)
	fmt.Fprintf(w, "                    Lag: %v\n", value.Lag)
	fmt.Fprintf(w, "                    ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                    ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                    ErrorMessage: nil\n")
	}

	return w.String()
}
