package offsetfetch

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type OffsetFetchResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                       // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 3+)
	Topics          *[]OffsetFetchResponseTopic // The responses per topic. (versions: 0-7)
	ErrorCode       int16                       // The top-level error code, or 0 if there was no error. (versions: 2-7)
	Groups          *[]OffsetFetchResponseGroup // The responses per group id. (versions: 8+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetFetchResponseTopic struct {
	Name            *string                              // The topic name. (versions: 0-7)
	Partitions      *[]OffsetFetchResponseTopicPartition // The responses per partition. (versions: 0-7)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetFetchResponseTopicPartition struct {
	PartitionIndex       int32   // The partition index. (versions: 0-7)
	CommittedOffset      int64   // The committed message offset. (versions: 0-7)
	CommittedLeaderEpoch int32   // The leader epoch. (versions: 5-7)
	Metadata             *string // The partition metadata. (versions: 0-7, nullable: 0-7)
	ErrorCode            int16   // The error code, or 0 if there was no error. (versions: 0-7)
	rawTaggedFields      *[]protocol.TaggedField
}

type OffsetFetchResponseGroup struct {
	GroupId         *string                          // The group ID. (versions: 8+)
	Topics          *[]OffsetFetchResponseGroupTopic // The responses per topic. (versions: 8+)
	ErrorCode       int16                            // The group-level error code, or 0 if there was no error. (versions: 8+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetFetchResponseGroupTopic struct {
	Name            *string                                   // The topic name. (versions: 8-9)
	TopicId         uuid.UUID                                 // The topic ID. (versions: 10+)
	Partitions      *[]OffsetFetchResponseGroupTopicPartition // The responses per partition. (versions: 8+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetFetchResponseGroupTopicPartition struct {
	PartitionIndex       int32   // The partition index. (versions: 8+)
	CommittedOffset      int64   // The committed message offset. (versions: 8+)
	CommittedLeaderEpoch int32   // The leader epoch. (versions: 8+)
	Metadata             *string // The partition metadata. (versions: 8+, nullable: 8+)
	ErrorCode            int16   // The partition-level error code, or 0 if there was no error. (versions: 8+)
	rawTaggedFields      *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 6
}

func (res *OffsetFetchResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 3+)
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// Topics (versions: 0-7)
	if res.ApiVersion <= 7 {
		if res.Topics == nil {
			return fmt.Errorf("OffsetFetchResponse.Topics must not be nil in version %d", res.ApiVersion)
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
	}

	// ErrorCode (versions: 2-7)
	if res.ApiVersion >= 2 && res.ApiVersion <= 7 {
		if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
			return err
		}
	}

	// Groups (versions: 8+)
	if res.ApiVersion >= 8 {
		if res.Groups == nil {
			return fmt.Errorf("OffsetFetchResponse.Groups must not be nil in version %d", res.ApiVersion)
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
func (res *OffsetFetchResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("OffsetFetchResponse.Read: response or its body is nil")
	}

	*res = OffsetFetchResponse{}

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

	// Topics (versions: 0-7)
	if res.ApiVersion <= 7 {
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
	}

	// ErrorCode (versions: 2-7)
	if res.ApiVersion >= 2 && res.ApiVersion <= 7 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		res.ErrorCode = errorcode
	}

	// Groups (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			groups, err := protocol.ReadCompactArray(r, res.groupsDecoder)
			if err != nil {
				return err
			}
			res.Groups = &groups
		} else {
			groups, err := protocol.ReadArray(r, res.groupsDecoder)
			if err != nil {
				return err
			}
			res.Groups = &groups
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

func (res *OffsetFetchResponse) topicsEncoder(w io.Writer, value OffsetFetchResponseTopic) error {
	// Name (versions: 0-7)
	if res.ApiVersion <= 7 {
		if value.Name == nil {
			return fmt.Errorf("OffsetFetchResponseTopic.Name must not be nil in version %d", res.ApiVersion)
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

	// Partitions (versions: 0-7)
	if res.ApiVersion <= 7 {
		if value.Partitions == nil {
			return fmt.Errorf("OffsetFetchResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *OffsetFetchResponse) topicsDecoder(r io.Reader) (OffsetFetchResponseTopic, error) {
	offsetfetchresponsetopic := OffsetFetchResponseTopic{}

	// Name (versions: 0-7)
	if res.ApiVersion <= 7 {
		if isResponseFlexible(res.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return offsetfetchresponsetopic, err
			}
			offsetfetchresponsetopic.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return offsetfetchresponsetopic, err
			}
			offsetfetchresponsetopic.Name = &name
		}
	}

	// Partitions (versions: 0-7)
	if res.ApiVersion <= 7 {
		if isResponseFlexible(res.ApiVersion) {
			partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
			if err != nil {
				return offsetfetchresponsetopic, err
			}
			offsetfetchresponsetopic.Partitions = &partitions
		} else {
			partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
			if err != nil {
				return offsetfetchresponsetopic, err
			}
			offsetfetchresponsetopic.Partitions = &partitions
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetfetchresponsetopic, err
		}
		offsetfetchresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return offsetfetchresponsetopic, nil
}

func (res *OffsetFetchResponse) partitionsEncoder(w io.Writer, value OffsetFetchResponseTopicPartition) error {
	// PartitionIndex (versions: 0-7)
	if res.ApiVersion <= 7 {
		if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
			return err
		}
	}

	// CommittedOffset (versions: 0-7)
	if res.ApiVersion <= 7 {
		if err := protocol.WriteInt64(w, value.CommittedOffset); err != nil {
			return err
		}
	}

	// CommittedLeaderEpoch (versions: 5-7)
	if res.ApiVersion >= 5 && res.ApiVersion <= 7 {
		if err := protocol.WriteInt32(w, value.CommittedLeaderEpoch); err != nil {
			return err
		}
	}

	// Metadata (versions: 0-7)
	if res.ApiVersion <= 7 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.Metadata); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.Metadata); err != nil {
				return err
			}
		}
	}

	// ErrorCode (versions: 0-7)
	if res.ApiVersion <= 7 {
		if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
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

func (res *OffsetFetchResponse) partitionsDecoder(r io.Reader) (OffsetFetchResponseTopicPartition, error) {
	offsetfetchresponsetopicpartition := OffsetFetchResponseTopicPartition{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	offsetfetchresponsetopicpartition.CommittedLeaderEpoch = -1

	// PartitionIndex (versions: 0-7)
	if res.ApiVersion <= 7 {
		partitionindex, err := protocol.ReadInt32(r)
		if err != nil {
			return offsetfetchresponsetopicpartition, err
		}
		offsetfetchresponsetopicpartition.PartitionIndex = partitionindex
	}

	// CommittedOffset (versions: 0-7)
	if res.ApiVersion <= 7 {
		committedoffset, err := protocol.ReadInt64(r)
		if err != nil {
			return offsetfetchresponsetopicpartition, err
		}
		offsetfetchresponsetopicpartition.CommittedOffset = committedoffset
	}

	// CommittedLeaderEpoch (versions: 5-7)
	if res.ApiVersion >= 5 && res.ApiVersion <= 7 {
		committedleaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return offsetfetchresponsetopicpartition, err
		}
		offsetfetchresponsetopicpartition.CommittedLeaderEpoch = committedleaderepoch
	}

	// Metadata (versions: 0-7)
	if res.ApiVersion <= 7 {
		if isResponseFlexible(res.ApiVersion) {
			metadata, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return offsetfetchresponsetopicpartition, err
			}
			offsetfetchresponsetopicpartition.Metadata = metadata
		} else {
			metadata, err := protocol.ReadNullableString(r)
			if err != nil {
				return offsetfetchresponsetopicpartition, err
			}
			offsetfetchresponsetopicpartition.Metadata = metadata
		}
	}

	// ErrorCode (versions: 0-7)
	if res.ApiVersion <= 7 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return offsetfetchresponsetopicpartition, err
		}
		offsetfetchresponsetopicpartition.ErrorCode = errorcode
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetfetchresponsetopicpartition, err
		}
		offsetfetchresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return offsetfetchresponsetopicpartition, nil
}

func (res *OffsetFetchResponse) groupsEncoder(w io.Writer, value OffsetFetchResponseGroup) error {
	// GroupId (versions: 8+)
	if res.ApiVersion >= 8 {
		if value.GroupId == nil {
			return fmt.Errorf("OffsetFetchResponseGroup.GroupId must not be nil in version %d", res.ApiVersion)
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
	}

	// Topics (versions: 8+)
	if res.ApiVersion >= 8 {
		if value.Topics == nil {
			return fmt.Errorf("OffsetFetchResponseGroup.Topics must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.offsetFetchResponseGroupTopicEncoder, value.Topics); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, res.offsetFetchResponseGroupTopicEncoder, *value.Topics); err != nil {
				return err
			}
		}
	}

	// ErrorCode (versions: 8+)
	if res.ApiVersion >= 8 {
		if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
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

func (res *OffsetFetchResponse) groupsDecoder(r io.Reader) (OffsetFetchResponseGroup, error) {
	offsetfetchresponsegroup := OffsetFetchResponseGroup{}

	// GroupId (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			groupid, err := protocol.ReadCompactString(r)
			if err != nil {
				return offsetfetchresponsegroup, err
			}
			offsetfetchresponsegroup.GroupId = &groupid
		} else {
			groupid, err := protocol.ReadString(r)
			if err != nil {
				return offsetfetchresponsegroup, err
			}
			offsetfetchresponsegroup.GroupId = &groupid
		}
	}

	// Topics (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			topics, err := protocol.ReadCompactArray(r, res.offsetFetchResponseGroupTopicDecoder)
			if err != nil {
				return offsetfetchresponsegroup, err
			}
			offsetfetchresponsegroup.Topics = &topics
		} else {
			topics, err := protocol.ReadArray(r, res.offsetFetchResponseGroupTopicDecoder)
			if err != nil {
				return offsetfetchresponsegroup, err
			}
			offsetfetchresponsegroup.Topics = &topics
		}
	}

	// ErrorCode (versions: 8+)
	if res.ApiVersion >= 8 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return offsetfetchresponsegroup, err
		}
		offsetfetchresponsegroup.ErrorCode = errorcode
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetfetchresponsegroup, err
		}
		offsetfetchresponsegroup.rawTaggedFields = &rawTaggedFields
	}

	return offsetfetchresponsegroup, nil
}

func (res *OffsetFetchResponse) offsetFetchResponseGroupTopicEncoder(w io.Writer, value OffsetFetchResponseGroupTopic) error {
	// Name (versions: 8-9)
	if res.ApiVersion >= 8 && res.ApiVersion <= 9 {
		if value.Name == nil {
			return fmt.Errorf("OffsetFetchResponseGroupTopic.Name must not be nil in version %d", res.ApiVersion)
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

	// Partitions (versions: 8+)
	if res.ApiVersion >= 8 {
		if value.Partitions == nil {
			return fmt.Errorf("OffsetFetchResponseGroupTopic.Partitions must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.offsetFetchResponseGroupTopicPartitionEncoder, value.Partitions); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, res.offsetFetchResponseGroupTopicPartitionEncoder, *value.Partitions); err != nil {
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

func (res *OffsetFetchResponse) offsetFetchResponseGroupTopicDecoder(r io.Reader) (OffsetFetchResponseGroupTopic, error) {
	offsetfetchresponsegrouptopic := OffsetFetchResponseGroupTopic{}

	// Name (versions: 8-9)
	if res.ApiVersion >= 8 && res.ApiVersion <= 9 {
		if isResponseFlexible(res.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return offsetfetchresponsegrouptopic, err
			}
			offsetfetchresponsegrouptopic.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return offsetfetchresponsegrouptopic, err
			}
			offsetfetchresponsegrouptopic.Name = &name
		}
	}

	// TopicId (versions: 10+)
	if res.ApiVersion >= 10 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return offsetfetchresponsegrouptopic, err
		}
		offsetfetchresponsegrouptopic.TopicId = topicid
	}

	// Partitions (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			partitions, err := protocol.ReadCompactArray(r, res.offsetFetchResponseGroupTopicPartitionDecoder)
			if err != nil {
				return offsetfetchresponsegrouptopic, err
			}
			offsetfetchresponsegrouptopic.Partitions = &partitions
		} else {
			partitions, err := protocol.ReadArray(r, res.offsetFetchResponseGroupTopicPartitionDecoder)
			if err != nil {
				return offsetfetchresponsegrouptopic, err
			}
			offsetfetchresponsegrouptopic.Partitions = &partitions
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetfetchresponsegrouptopic, err
		}
		offsetfetchresponsegrouptopic.rawTaggedFields = &rawTaggedFields
	}

	return offsetfetchresponsegrouptopic, nil
}

func (res *OffsetFetchResponse) offsetFetchResponseGroupTopicPartitionEncoder(w io.Writer, value OffsetFetchResponseGroupTopicPartition) error {
	// PartitionIndex (versions: 8+)
	if res.ApiVersion >= 8 {
		if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
			return err
		}
	}

	// CommittedOffset (versions: 8+)
	if res.ApiVersion >= 8 {
		if err := protocol.WriteInt64(w, value.CommittedOffset); err != nil {
			return err
		}
	}

	// CommittedLeaderEpoch (versions: 8+)
	if res.ApiVersion >= 8 {
		if err := protocol.WriteInt32(w, value.CommittedLeaderEpoch); err != nil {
			return err
		}
	}

	// Metadata (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.Metadata); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.Metadata); err != nil {
				return err
			}
		}
	}

	// ErrorCode (versions: 8+)
	if res.ApiVersion >= 8 {
		if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
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

func (res *OffsetFetchResponse) offsetFetchResponseGroupTopicPartitionDecoder(r io.Reader) (OffsetFetchResponseGroupTopicPartition, error) {
	offsetfetchresponsegrouptopicpartition := OffsetFetchResponseGroupTopicPartition{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	offsetfetchresponsegrouptopicpartition.CommittedLeaderEpoch = -1

	// PartitionIndex (versions: 8+)
	if res.ApiVersion >= 8 {
		partitionindex, err := protocol.ReadInt32(r)
		if err != nil {
			return offsetfetchresponsegrouptopicpartition, err
		}
		offsetfetchresponsegrouptopicpartition.PartitionIndex = partitionindex
	}

	// CommittedOffset (versions: 8+)
	if res.ApiVersion >= 8 {
		committedoffset, err := protocol.ReadInt64(r)
		if err != nil {
			return offsetfetchresponsegrouptopicpartition, err
		}
		offsetfetchresponsegrouptopicpartition.CommittedOffset = committedoffset
	}

	// CommittedLeaderEpoch (versions: 8+)
	if res.ApiVersion >= 8 {
		committedleaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return offsetfetchresponsegrouptopicpartition, err
		}
		offsetfetchresponsegrouptopicpartition.CommittedLeaderEpoch = committedleaderepoch
	}

	// Metadata (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			metadata, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return offsetfetchresponsegrouptopicpartition, err
			}
			offsetfetchresponsegrouptopicpartition.Metadata = metadata
		} else {
			metadata, err := protocol.ReadNullableString(r)
			if err != nil {
				return offsetfetchresponsegrouptopicpartition, err
			}
			offsetfetchresponsegrouptopicpartition.Metadata = metadata
		}
	}

	// ErrorCode (versions: 8+)
	if res.ApiVersion >= 8 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return offsetfetchresponsegrouptopicpartition, err
		}
		offsetfetchresponsegrouptopicpartition.ErrorCode = errorcode
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetfetchresponsegrouptopicpartition, err
		}
		offsetfetchresponsegrouptopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return offsetfetchresponsegrouptopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *OffsetFetchResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- OffsetFetchResponse:\n")
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

	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

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
func (value *OffsetFetchResponseTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
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
func (value *OffsetFetchResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                CommittedOffset: %v\n", value.CommittedOffset)
	fmt.Fprintf(w, "                CommittedLeaderEpoch: %v\n", value.CommittedLeaderEpoch)

	if value.Metadata != nil {
		fmt.Fprintf(w, "                Metadata: %v\n", *value.Metadata)
	} else {
		fmt.Fprintf(w, "                Metadata: nil\n")
	}

	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *OffsetFetchResponseGroup) PrettyPrint() string {
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

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *OffsetFetchResponseGroupTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
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
func (value *OffsetFetchResponseGroupTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                    CommittedOffset: %v\n", value.CommittedOffset)
	fmt.Fprintf(w, "                    CommittedLeaderEpoch: %v\n", value.CommittedLeaderEpoch)

	if value.Metadata != nil {
		fmt.Fprintf(w, "                    Metadata: %v\n", *value.Metadata)
	} else {
		fmt.Fprintf(w, "                    Metadata: nil\n")
	}

	fmt.Fprintf(w, "                    ErrorCode: %v\n", value.ErrorCode)

	return w.String()
}
