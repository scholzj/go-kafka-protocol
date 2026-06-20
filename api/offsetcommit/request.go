package offsetcommit

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type OffsetCommitRequest struct {
	ApiVersion                int16
	GroupId                   *string                     // The unique group identifier. (versions: 0+)
	GenerationIdOrMemberEpoch int32                       // The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol. (versions: 1+)
	MemberId                  *string                     // The member ID assigned by the group coordinator. (versions: 1+)
	GroupInstanceId           *string                     // The unique identifier of the consumer instance provided by end user. (versions: 7+, nullable: 7+)
	RetentionTimeMs           int64                       // The time period in ms to retain the offset. (versions: 2-4)
	Topics                    *[]OffsetCommitRequestTopic // The topics to commit offsets for. (versions: 0+)
	rawTaggedFields           *[]protocol.TaggedField
}

type OffsetCommitRequestTopic struct {
	Name            *string                              // The topic name. (versions: 0-9)
	TopicId         uuid.UUID                            // The topic ID. (versions: 10+)
	Partitions      *[]OffsetCommitRequestTopicPartition // Each partition to commit offsets for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetCommitRequestTopicPartition struct {
	PartitionIndex       int32   // The partition index. (versions: 0+)
	CommittedOffset      int64   // The message offset to be committed. (versions: 0+)
	CommittedLeaderEpoch int32   // The leader epoch of this partition. (versions: 6+)
	CommittedMetadata    *string // Any associated metadata the client wants to keep. (versions: 0+, nullable: 0+)
	rawTaggedFields      *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 8
}

func (req *OffsetCommitRequest) Write(w io.Writer) error {
	// GroupId (versions: 0+)
	if req.GroupId == nil {
		return fmt.Errorf("OffsetCommitRequest.GroupId must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *req.GroupId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *req.GroupId); err != nil {
			return err
		}
	}

	// GenerationIdOrMemberEpoch (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, req.GenerationIdOrMemberEpoch); err != nil {
			return err
		}
	}

	// MemberId (versions: 1+)
	if req.ApiVersion >= 1 {
		if req.MemberId == nil {
			return fmt.Errorf("OffsetCommitRequest.MemberId must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteCompactString(w, *req.MemberId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *req.MemberId); err != nil {
				return err
			}
		}
	}

	// GroupInstanceId (versions: 7+)
	if req.ApiVersion >= 7 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, req.GroupInstanceId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, req.GroupInstanceId); err != nil {
				return err
			}
		}
	}

	// RetentionTimeMs (versions: 2-4)
	if req.ApiVersion >= 2 && req.ApiVersion <= 4 {
		if err := protocol.WriteInt64(w, req.RetentionTimeMs); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("OffsetCommitRequest.Topics must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, req.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.topicsEncoder, *req.Topics); err != nil {
			return err
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
func (req *OffsetCommitRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("OffsetCommitRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// GroupId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		groupid, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		req.GroupId = &groupid
	} else {
		groupid, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		req.GroupId = &groupid
	}

	// GenerationIdOrMemberEpoch (versions: 1+)
	if req.ApiVersion >= 1 {
		generationidormemberepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.GenerationIdOrMemberEpoch = generationidormemberepoch
	}

	// MemberId (versions: 1+)
	if req.ApiVersion >= 1 {
		if isRequestFlexible(req.ApiVersion) {
			memberid, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			req.MemberId = &memberid
		} else {
			memberid, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			req.MemberId = &memberid
		}
	}

	// GroupInstanceId (versions: 7+)
	if req.ApiVersion >= 7 {
		if isRequestFlexible(req.ApiVersion) {
			groupinstanceid, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			req.GroupInstanceId = groupinstanceid
		} else {
			groupinstanceid, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			req.GroupInstanceId = groupinstanceid
		}
	}

	// RetentionTimeMs (versions: 2-4)
	if req.ApiVersion >= 2 && req.ApiVersion <= 4 {
		retentiontimems, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		req.RetentionTimeMs = retentiontimems
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

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		req.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

func (req *OffsetCommitRequest) topicsEncoder(w io.Writer, value OffsetCommitRequestTopic) error {
	// Name (versions: 0-9)
	if req.ApiVersion <= 9 {
		if value.Name == nil {
			return fmt.Errorf("OffsetCommitRequestTopic.Name must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
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
	if req.ApiVersion >= 10 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("OffsetCommitRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
	}
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

func (req *OffsetCommitRequest) topicsDecoder(r io.Reader) (OffsetCommitRequestTopic, error) {
	offsetcommitrequesttopic := OffsetCommitRequestTopic{}

	// Name (versions: 0-9)
	if req.ApiVersion <= 9 {
		if isRequestFlexible(req.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return offsetcommitrequesttopic, err
			}
			offsetcommitrequesttopic.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return offsetcommitrequesttopic, err
			}
			offsetcommitrequesttopic.Name = &name
		}
	}

	// TopicId (versions: 10+)
	if req.ApiVersion >= 10 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return offsetcommitrequesttopic, err
		}
		offsetcommitrequesttopic.TopicId = topicid
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return offsetcommitrequesttopic, err
		}
		offsetcommitrequesttopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return offsetcommitrequesttopic, err
		}
		offsetcommitrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetcommitrequesttopic, err
		}
		offsetcommitrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return offsetcommitrequesttopic, nil
}

func (req *OffsetCommitRequest) partitionsEncoder(w io.Writer, value OffsetCommitRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// CommittedOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.CommittedOffset); err != nil {
		return err
	}

	// CommittedLeaderEpoch (versions: 6+)
	if req.ApiVersion >= 6 {
		if err := protocol.WriteInt32(w, value.CommittedLeaderEpoch); err != nil {
			return err
		}
	}

	// CommittedMetadata (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.CommittedMetadata); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.CommittedMetadata); err != nil {
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

func (req *OffsetCommitRequest) partitionsDecoder(r io.Reader) (OffsetCommitRequestTopicPartition, error) {
	offsetcommitrequesttopicpartition := OffsetCommitRequestTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return offsetcommitrequesttopicpartition, err
	}
	offsetcommitrequesttopicpartition.PartitionIndex = partitionindex

	// CommittedOffset (versions: 0+)
	committedoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return offsetcommitrequesttopicpartition, err
	}
	offsetcommitrequesttopicpartition.CommittedOffset = committedoffset

	// CommittedLeaderEpoch (versions: 6+)
	if req.ApiVersion >= 6 {
		committedleaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return offsetcommitrequesttopicpartition, err
		}
		offsetcommitrequesttopicpartition.CommittedLeaderEpoch = committedleaderepoch
	}

	// CommittedMetadata (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		committedmetadata, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return offsetcommitrequesttopicpartition, err
		}
		offsetcommitrequesttopicpartition.CommittedMetadata = committedmetadata
	} else {
		committedmetadata, err := protocol.ReadNullableString(r)
		if err != nil {
			return offsetcommitrequesttopicpartition, err
		}
		offsetcommitrequesttopicpartition.CommittedMetadata = committedmetadata
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetcommitrequesttopicpartition, err
		}
		offsetcommitrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return offsetcommitrequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *OffsetCommitRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> OffsetCommitRequest:\n")

	if req.GroupId != nil {
		fmt.Fprintf(w, "        GroupId: %v\n", *req.GroupId)
	} else {
		fmt.Fprintf(w, "        GroupId: nil\n")
	}

	fmt.Fprintf(w, "        GenerationIdOrMemberEpoch: %v\n", req.GenerationIdOrMemberEpoch)

	if req.MemberId != nil {
		fmt.Fprintf(w, "        MemberId: %v\n", *req.MemberId)
	} else {
		fmt.Fprintf(w, "        MemberId: nil\n")
	}

	if req.GroupInstanceId != nil {
		fmt.Fprintf(w, "        GroupInstanceId: %v\n", *req.GroupInstanceId)
	} else {
		fmt.Fprintf(w, "        GroupInstanceId: nil\n")
	}

	fmt.Fprintf(w, "        RetentionTimeMs: %v\n", req.RetentionTimeMs)

	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *OffsetCommitRequestTopic) PrettyPrint() string {
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
func (value *OffsetCommitRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                CommittedOffset: %v\n", value.CommittedOffset)
	fmt.Fprintf(w, "                CommittedLeaderEpoch: %v\n", value.CommittedLeaderEpoch)

	if value.CommittedMetadata != nil {
		fmt.Fprintf(w, "                CommittedMetadata: %v\n", *value.CommittedMetadata)
	} else {
		fmt.Fprintf(w, "                CommittedMetadata: nil\n")
	}

	return w.String()
}
