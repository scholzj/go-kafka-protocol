package offsetcommit

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type OffsetCommitResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                        // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 3+)
	Topics          *[]OffsetCommitResponseTopic // The responses for each topic. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetCommitResponseTopic struct {
	Name            *string                               // The topic name. (versions: 0-9)
	TopicId         uuid.UUID                             // The topic ID. (versions: 10+)
	Partitions      *[]OffsetCommitResponseTopicPartition // The responses for each partition in the topic. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetCommitResponseTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	ErrorCode       int16 // The error code, or 0 if there was no error. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 8
}

func (res *OffsetCommitResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 3+)
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("OffsetCommitResponse.Topics must not be nil in version %d", res.ApiVersion)
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
func (res *OffsetCommitResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("OffsetCommitResponse.Read: response or its body is nil")
	}

	*res = OffsetCommitResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 3+)
	if res.ApiVersion >= 3 {
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

func (res *OffsetCommitResponse) topicsEncoder(w io.Writer, value OffsetCommitResponseTopic) error {
	// Name (versions: 0-9)
	if res.ApiVersion <= 9 {
		if value.Name == nil {
			return fmt.Errorf("OffsetCommitResponseTopic.Name must not be nil in version %d", res.ApiVersion)
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

	// TopicId (versions: 10+)
	if res.ApiVersion >= 10 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("OffsetCommitResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *OffsetCommitResponse) topicsDecoder(r io.Reader) (OffsetCommitResponseTopic, error) {
	offsetcommitresponsetopic := OffsetCommitResponseTopic{}

	// Name (versions: 0-9)
	if res.ApiVersion <= 9 {
		if isResponseFlexible(res.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return offsetcommitresponsetopic, err
			}
			offsetcommitresponsetopic.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return offsetcommitresponsetopic, err
			}
			offsetcommitresponsetopic.Name = &name
		}
	}

	// TopicId (versions: 10+)
	if res.ApiVersion >= 10 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return offsetcommitresponsetopic, err
		}
		offsetcommitresponsetopic.TopicId = topicid
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return offsetcommitresponsetopic, err
		}
		offsetcommitresponsetopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return offsetcommitresponsetopic, err
		}
		offsetcommitresponsetopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetcommitresponsetopic, err
		}
		offsetcommitresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return offsetcommitresponsetopic, nil
}

func (res *OffsetCommitResponse) partitionsEncoder(w io.Writer, value OffsetCommitResponseTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
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

func (res *OffsetCommitResponse) partitionsDecoder(r io.Reader) (OffsetCommitResponseTopicPartition, error) {
	offsetcommitresponsetopicpartition := OffsetCommitResponseTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return offsetcommitresponsetopicpartition, err
	}
	offsetcommitresponsetopicpartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return offsetcommitresponsetopicpartition, err
	}
	offsetcommitresponsetopicpartition.ErrorCode = errorcode

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetcommitresponsetopicpartition, err
		}
		offsetcommitresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return offsetcommitresponsetopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *OffsetCommitResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- OffsetCommitResponse:\n")
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
func (value *OffsetCommitResponseTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
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
func (value *OffsetCommitResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	return w.String()
}
