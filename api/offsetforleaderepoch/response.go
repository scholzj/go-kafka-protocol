package offsetforleaderepoch

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type OffsetForLeaderEpochResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 2+)
	Topics          *[]OffsetForLeaderEpochResponseTopic // Each topic we fetched offsets for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetForLeaderEpochResponseTopic struct {
	Topic           *string                                       // The topic name. (versions: 0+)
	Partitions      *[]OffsetForLeaderEpochResponseTopicPartition // Each partition in the topic we fetched offsets for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetForLeaderEpochResponseTopicPartition struct {
	ErrorCode       int16 // The error code 0, or if there was no error. (versions: 0+)
	Partition       int32 // The partition index. (versions: 0+)
	LeaderEpoch     int32 // The leader epoch of the partition. (versions: 1+)
	EndOffset       int64 // The end offset of the epoch. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 4
}

func (res *OffsetForLeaderEpochResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("OffsetForLeaderEpochResponse.Topics must not be nil in version %d", res.ApiVersion)
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
func (res *OffsetForLeaderEpochResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("OffsetForLeaderEpochResponse.Read: response or its body is nil")
	}

	*res = OffsetForLeaderEpochResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 2+)
	if res.ApiVersion >= 2 {
		throttletimems, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttletimems
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

func (res *OffsetForLeaderEpochResponse) topicsEncoder(w io.Writer, value OffsetForLeaderEpochResponseTopic) error {
	// Topic (versions: 0+)
	if value.Topic == nil {
		return fmt.Errorf("OffsetForLeaderEpochResponseTopic.Topic must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Topic); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Topic); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("OffsetForLeaderEpochResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *OffsetForLeaderEpochResponse) topicsDecoder(r io.Reader) (OffsetForLeaderEpochResponseTopic, error) {
	offsetforleaderepochresponsetopic := OffsetForLeaderEpochResponseTopic{}

	// Topic (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topic, err := protocol.ReadCompactString(r)
		if err != nil {
			return offsetforleaderepochresponsetopic, err
		}
		offsetforleaderepochresponsetopic.Topic = &topic
	} else {
		topic, err := protocol.ReadString(r)
		if err != nil {
			return offsetforleaderepochresponsetopic, err
		}
		offsetforleaderepochresponsetopic.Topic = &topic
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return offsetforleaderepochresponsetopic, err
		}
		offsetforleaderepochresponsetopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return offsetforleaderepochresponsetopic, err
		}
		offsetforleaderepochresponsetopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetforleaderepochresponsetopic, err
		}
		offsetforleaderepochresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return offsetforleaderepochresponsetopic, nil
}

func (res *OffsetForLeaderEpochResponse) partitionsEncoder(w io.Writer, value OffsetForLeaderEpochResponseTopicPartition) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// Partition (versions: 0+)
	if err := protocol.WriteInt32(w, value.Partition); err != nil {
		return err
	}

	// LeaderEpoch (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
			return err
		}
	}

	// EndOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.EndOffset); err != nil {
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

func (res *OffsetForLeaderEpochResponse) partitionsDecoder(r io.Reader) (OffsetForLeaderEpochResponseTopicPartition, error) {
	offsetforleaderepochresponsetopicpartition := OffsetForLeaderEpochResponseTopicPartition{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	offsetforleaderepochresponsetopicpartition.LeaderEpoch = -1
	offsetforleaderepochresponsetopicpartition.EndOffset = -1

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return offsetforleaderepochresponsetopicpartition, err
	}
	offsetforleaderepochresponsetopicpartition.ErrorCode = errorcode

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return offsetforleaderepochresponsetopicpartition, err
	}
	offsetforleaderepochresponsetopicpartition.Partition = partition

	// LeaderEpoch (versions: 1+)
	if res.ApiVersion >= 1 {
		leaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return offsetforleaderepochresponsetopicpartition, err
		}
		offsetforleaderepochresponsetopicpartition.LeaderEpoch = leaderepoch
	}

	// EndOffset (versions: 0+)
	endoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return offsetforleaderepochresponsetopicpartition, err
	}
	offsetforleaderepochresponsetopicpartition.EndOffset = endoffset

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetforleaderepochresponsetopicpartition, err
		}
		offsetforleaderepochresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return offsetforleaderepochresponsetopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *OffsetForLeaderEpochResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- OffsetForLeaderEpochResponse:\n")
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

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *OffsetForLeaderEpochResponseTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Topic != nil {
		fmt.Fprintf(w, "            Topic: %v\n", *value.Topic)
	} else {
		fmt.Fprintf(w, "            Topic: nil\n")
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
func (value *OffsetForLeaderEpochResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)
	fmt.Fprintf(w, "                Partition: %v\n", value.Partition)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)
	fmt.Fprintf(w, "                EndOffset: %v\n", value.EndOffset)

	return w.String()
}
