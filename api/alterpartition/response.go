package alterpartition

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AlterPartitionResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                          // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                          // The top level response error code. (versions: 0+)
	Topics          *[]AlterPartitionResponseTopic // The responses for each topic. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterPartitionResponseTopic struct {
	TopicId         uuid.UUID                               // The ID of the topic. (versions: 2+)
	Partitions      *[]AlterPartitionResponseTopicPartition // The responses for each partition. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterPartitionResponseTopicPartition struct {
	PartitionIndex      int32    // The partition index. (versions: 0+)
	ErrorCode           int16    // The partition level error code. (versions: 0+)
	LeaderId            int32    // The broker ID of the leader. (versions: 0+)
	LeaderEpoch         int32    // The leader epoch. (versions: 0+)
	Isr                 *[]int32 // The in-sync replica IDs. (versions: 0+)
	LeaderRecoveryState int8     // 1 if the partition is recovering from an unclean leader election; 0 otherwise. (versions: 1+)
	PartitionEpoch      int32    // The current epoch for the partition for KRaft controllers. (versions: 0+)
	rawTaggedFields     *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *AlterPartitionResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("AlterPartitionResponse.Topics must not be nil in version %d", res.ApiVersion)
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
func (res *AlterPartitionResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("AlterPartitionResponse.Read: response or its body is nil")
	}

	*res = AlterPartitionResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

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

func (res *AlterPartitionResponse) topicsEncoder(w io.Writer, value AlterPartitionResponseTopic) error {
	// TopicId (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("AlterPartitionResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *AlterPartitionResponse) topicsDecoder(r io.Reader) (AlterPartitionResponseTopic, error) {
	alterpartitionresponsetopic := AlterPartitionResponseTopic{}

	// TopicId (versions: 2+)
	if res.ApiVersion >= 2 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return alterpartitionresponsetopic, err
		}
		alterpartitionresponsetopic.TopicId = topicid
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return alterpartitionresponsetopic, err
		}
		alterpartitionresponsetopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return alterpartitionresponsetopic, err
		}
		alterpartitionresponsetopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterpartitionresponsetopic, err
		}
		alterpartitionresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return alterpartitionresponsetopic, nil
}

func (res *AlterPartitionResponse) partitionsEncoder(w io.Writer, value AlterPartitionResponseTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
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

	// Isr (versions: 0+)
	if value.Isr == nil {
		return fmt.Errorf("AlterPartitionResponseTopicPartition.Isr must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Isr); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Isr); err != nil {
			return err
		}
	}

	// LeaderRecoveryState (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, value.LeaderRecoveryState); err != nil {
			return err
		}
	}

	// PartitionEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionEpoch); err != nil {
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

func (res *AlterPartitionResponse) partitionsDecoder(r io.Reader) (AlterPartitionResponseTopicPartition, error) {
	alterpartitionresponsetopicpartition := AlterPartitionResponseTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return alterpartitionresponsetopicpartition, err
	}
	alterpartitionresponsetopicpartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return alterpartitionresponsetopicpartition, err
	}
	alterpartitionresponsetopicpartition.ErrorCode = errorcode

	// LeaderId (versions: 0+)
	leaderid, err := protocol.ReadInt32(r)
	if err != nil {
		return alterpartitionresponsetopicpartition, err
	}
	alterpartitionresponsetopicpartition.LeaderId = leaderid

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return alterpartitionresponsetopicpartition, err
	}
	alterpartitionresponsetopicpartition.LeaderEpoch = leaderepoch

	// Isr (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		isr, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return alterpartitionresponsetopicpartition, err
		}
		alterpartitionresponsetopicpartition.Isr = &isr
	} else {
		isr, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return alterpartitionresponsetopicpartition, err
		}
		alterpartitionresponsetopicpartition.Isr = &isr
	}

	// LeaderRecoveryState (versions: 1+)
	if res.ApiVersion >= 1 {
		leaderrecoverystate, err := protocol.ReadInt8(r)
		if err != nil {
			return alterpartitionresponsetopicpartition, err
		}
		alterpartitionresponsetopicpartition.LeaderRecoveryState = leaderrecoverystate
	}

	// PartitionEpoch (versions: 0+)
	partitionepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return alterpartitionresponsetopicpartition, err
	}
	alterpartitionresponsetopicpartition.PartitionEpoch = partitionepoch

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterpartitionresponsetopicpartition, err
		}
		alterpartitionresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return alterpartitionresponsetopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *AlterPartitionResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- AlterPartitionResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *res.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterPartitionResponseTopic) PrettyPrint() string {
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
func (value *AlterPartitionResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)
	fmt.Fprintf(w, "                LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)

	if value.Isr != nil {
		fmt.Fprintf(w, "                Isr: %v\n", *value.Isr)
	} else {
		fmt.Fprintf(w, "                Isr: nil\n")
	}

	fmt.Fprintf(w, "                LeaderRecoveryState: %v\n", value.LeaderRecoveryState)
	fmt.Fprintf(w, "                PartitionEpoch: %v\n", value.PartitionEpoch)

	return w.String()
}
