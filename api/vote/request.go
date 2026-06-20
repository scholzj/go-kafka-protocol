package vote

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type VoteRequest struct {
	ApiVersion      int16
	ClusterId       *string             // The cluster id. (versions: 0+, nullable: 0+)
	VoterId         int32               // The replica id of the voter receiving the request. (versions: 1+)
	Topics          *[]VoteRequestTopic // The topic data. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type VoteRequestTopic struct {
	TopicName       *string                      // The topic name. (versions: 0+)
	Partitions      *[]VoteRequestTopicPartition // The partition data. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type VoteRequestTopicPartition struct {
	PartitionIndex     int32     // The partition index. (versions: 0+)
	ReplicaEpoch       int32     // The epoch of the voter sending the request (versions: 0+)
	ReplicaId          int32     // The replica id of the voter sending the request (versions: 0+)
	ReplicaDirectoryId uuid.UUID // The directory id of the voter sending the request (versions: 1+)
	VoterDirectoryId   uuid.UUID // The directory id of the voter receiving the request (versions: 1+)
	LastOffsetEpoch    int32     // The epoch of the last record written to the metadata log. (versions: 0+)
	LastOffset         int64     // The log end offset of the metadata log of the voter sending the request. (versions: 0+)
	PreVote            bool      // Whether the request is a PreVote request (not persisted) or not. (versions: 2+)
	rawTaggedFields    *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *VoteRequest) Write(w io.Writer) error {
	// ClusterId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, req.ClusterId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, req.ClusterId); err != nil {
			return err
		}
	}

	// VoterId (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, req.VoterId); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("VoteRequest.Topics must not be nil in version %d", req.ApiVersion)
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
func (req *VoteRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("VoteRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// ClusterId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		clusterid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.ClusterId = clusterid
	} else {
		clusterid, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		req.ClusterId = clusterid
	}

	// VoterId (versions: 1+)
	if req.ApiVersion >= 1 {
		voterid, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.VoterId = voterid
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

func (req *VoteRequest) topicsEncoder(w io.Writer, value VoteRequestTopic) error {
	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("VoteRequestTopic.TopicName must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
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
		return fmt.Errorf("VoteRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *VoteRequest) topicsDecoder(r io.Reader) (VoteRequestTopic, error) {
	voterequesttopic := VoteRequestTopic{}

	// TopicName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return voterequesttopic, err
		}
		voterequesttopic.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return voterequesttopic, err
		}
		voterequesttopic.TopicName = &topicname
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return voterequesttopic, err
		}
		voterequesttopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return voterequesttopic, err
		}
		voterequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return voterequesttopic, err
		}
		voterequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return voterequesttopic, nil
}

func (req *VoteRequest) partitionsEncoder(w io.Writer, value VoteRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// ReplicaEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.ReplicaEpoch); err != nil {
		return err
	}

	// ReplicaId (versions: 0+)
	if err := protocol.WriteInt32(w, value.ReplicaId); err != nil {
		return err
	}

	// ReplicaDirectoryId (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteUUID(w, value.ReplicaDirectoryId); err != nil {
			return err
		}
	}

	// VoterDirectoryId (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteUUID(w, value.VoterDirectoryId); err != nil {
			return err
		}
	}

	// LastOffsetEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.LastOffsetEpoch); err != nil {
		return err
	}

	// LastOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.LastOffset); err != nil {
		return err
	}

	// PreVote (versions: 2+)
	if req.ApiVersion >= 2 {
		if err := protocol.WriteBool(w, value.PreVote); err != nil {
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

func (req *VoteRequest) partitionsDecoder(r io.Reader) (VoteRequestTopicPartition, error) {
	voterequesttopicpartition := VoteRequestTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return voterequesttopicpartition, err
	}
	voterequesttopicpartition.PartitionIndex = partitionindex

	// ReplicaEpoch (versions: 0+)
	replicaepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return voterequesttopicpartition, err
	}
	voterequesttopicpartition.ReplicaEpoch = replicaepoch

	// ReplicaId (versions: 0+)
	replicaid, err := protocol.ReadInt32(r)
	if err != nil {
		return voterequesttopicpartition, err
	}
	voterequesttopicpartition.ReplicaId = replicaid

	// ReplicaDirectoryId (versions: 1+)
	if req.ApiVersion >= 1 {
		replicadirectoryid, err := protocol.ReadUUID(r)
		if err != nil {
			return voterequesttopicpartition, err
		}
		voterequesttopicpartition.ReplicaDirectoryId = replicadirectoryid
	}

	// VoterDirectoryId (versions: 1+)
	if req.ApiVersion >= 1 {
		voterdirectoryid, err := protocol.ReadUUID(r)
		if err != nil {
			return voterequesttopicpartition, err
		}
		voterequesttopicpartition.VoterDirectoryId = voterdirectoryid
	}

	// LastOffsetEpoch (versions: 0+)
	lastoffsetepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return voterequesttopicpartition, err
	}
	voterequesttopicpartition.LastOffsetEpoch = lastoffsetepoch

	// LastOffset (versions: 0+)
	lastoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return voterequesttopicpartition, err
	}
	voterequesttopicpartition.LastOffset = lastoffset

	// PreVote (versions: 2+)
	if req.ApiVersion >= 2 {
		prevote, err := protocol.ReadBool(r)
		if err != nil {
			return voterequesttopicpartition, err
		}
		voterequesttopicpartition.PreVote = prevote
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return voterequesttopicpartition, err
		}
		voterequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return voterequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *VoteRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> VoteRequest:\n")

	if req.ClusterId != nil {
		fmt.Fprintf(w, "        ClusterId: %v\n", *req.ClusterId)
	} else {
		fmt.Fprintf(w, "        ClusterId: nil\n")
	}

	fmt.Fprintf(w, "        VoterId: %v\n", req.VoterId)

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
func (value *VoteRequestTopic) PrettyPrint() string {
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
func (value *VoteRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ReplicaEpoch: %v\n", value.ReplicaEpoch)
	fmt.Fprintf(w, "                ReplicaId: %v\n", value.ReplicaId)
	fmt.Fprintf(w, "                ReplicaDirectoryId: %v\n", value.ReplicaDirectoryId)
	fmt.Fprintf(w, "                VoterDirectoryId: %v\n", value.VoterDirectoryId)
	fmt.Fprintf(w, "                LastOffsetEpoch: %v\n", value.LastOffsetEpoch)
	fmt.Fprintf(w, "                LastOffset: %v\n", value.LastOffset)
	fmt.Fprintf(w, "                PreVote: %v\n", value.PreVote)

	return w.String()
}
