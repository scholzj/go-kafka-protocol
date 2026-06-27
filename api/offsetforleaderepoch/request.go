package offsetforleaderepoch

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type OffsetForLeaderEpochRequest struct {
	ApiVersion      int16
	ReplicaId       int32                               // The broker ID of the follower, of -1 if this request is from a consumer. (versions: 3+)
	Topics          *[]OffsetForLeaderEpochRequestTopic // Each topic to get offsets for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetForLeaderEpochRequestTopic struct {
	Topic           *string                                      // The topic name. (versions: 0+)
	Partitions      *[]OffsetForLeaderEpochRequestTopicPartition // Each partition to get offsets for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetForLeaderEpochRequestTopicPartition struct {
	Partition          int32 // The partition index. (versions: 0+)
	CurrentLeaderEpoch int32 // An epoch used to fence consumers/replicas with old metadata. If the epoch provided by the client is larger than the current epoch known to the broker, then the UNKNOWN_LEADER_EPOCH error code will be returned. If the provided epoch is smaller, then the FENCED_LEADER_EPOCH error code will be returned. (versions: 2+)
	LeaderEpoch        int32 // The epoch to look up an offset for. (versions: 0+)
	rawTaggedFields    *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 4
}

func (req *OffsetForLeaderEpochRequest) Write(w io.Writer) error {
	// ReplicaId (versions: 3+)
	if req.ApiVersion >= 3 {
		if err := protocol.WriteInt32(w, req.ReplicaId); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("OffsetForLeaderEpochRequest.Topics must not be nil in version %d", req.ApiVersion)
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
func (req *OffsetForLeaderEpochRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("OffsetForLeaderEpochRequest.Read: request or its body is nil")
	}

	*req = OffsetForLeaderEpochRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.ReplicaId = -2

	// ReplicaId (versions: 3+)
	if req.ApiVersion >= 3 {
		replicaid, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.ReplicaId = replicaid
	}

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadCompactArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = &topics
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

func (req *OffsetForLeaderEpochRequest) topicsEncoder(w io.Writer, value OffsetForLeaderEpochRequestTopic) error {
	// Topic (versions: 0+)
	if value.Topic == nil {
		return fmt.Errorf("OffsetForLeaderEpochRequestTopic.Topic must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
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
		return fmt.Errorf("OffsetForLeaderEpochRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *OffsetForLeaderEpochRequest) topicsDecoder(r io.Reader) (OffsetForLeaderEpochRequestTopic, error) {
	offsetforleaderepochrequesttopic := OffsetForLeaderEpochRequestTopic{}

	// Topic (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topic, err := protocol.ReadCompactString(r)
		if err != nil {
			return offsetforleaderepochrequesttopic, err
		}
		offsetforleaderepochrequesttopic.Topic = &topic
	} else {
		topic, err := protocol.ReadString(r)
		if err != nil {
			return offsetforleaderepochrequesttopic, err
		}
		offsetforleaderepochrequesttopic.Topic = &topic
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return offsetforleaderepochrequesttopic, err
		}
		offsetforleaderepochrequesttopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return offsetforleaderepochrequesttopic, err
		}
		offsetforleaderepochrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetforleaderepochrequesttopic, err
		}
		offsetforleaderepochrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return offsetforleaderepochrequesttopic, nil
}

func (req *OffsetForLeaderEpochRequest) partitionsEncoder(w io.Writer, value OffsetForLeaderEpochRequestTopicPartition) error {
	// Partition (versions: 0+)
	if err := protocol.WriteInt32(w, value.Partition); err != nil {
		return err
	}

	// CurrentLeaderEpoch (versions: 2+)
	if req.ApiVersion >= 2 {
		if err := protocol.WriteInt32(w, value.CurrentLeaderEpoch); err != nil {
			return err
		}
	}

	// LeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
		return err
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

func (req *OffsetForLeaderEpochRequest) partitionsDecoder(r io.Reader) (OffsetForLeaderEpochRequestTopicPartition, error) {
	offsetforleaderepochrequesttopicpartition := OffsetForLeaderEpochRequestTopicPartition{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	offsetforleaderepochrequesttopicpartition.CurrentLeaderEpoch = -1

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return offsetforleaderepochrequesttopicpartition, err
	}
	offsetforleaderepochrequesttopicpartition.Partition = partition

	// CurrentLeaderEpoch (versions: 2+)
	if req.ApiVersion >= 2 {
		currentleaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return offsetforleaderepochrequesttopicpartition, err
		}
		offsetforleaderepochrequesttopicpartition.CurrentLeaderEpoch = currentleaderepoch
	}

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return offsetforleaderepochrequesttopicpartition, err
	}
	offsetforleaderepochrequesttopicpartition.LeaderEpoch = leaderepoch

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetforleaderepochrequesttopicpartition, err
		}
		offsetforleaderepochrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return offsetforleaderepochrequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *OffsetForLeaderEpochRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> OffsetForLeaderEpochRequest:\n")
	fmt.Fprintf(w, "        ReplicaId: %v\n", req.ReplicaId)

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
func (value *OffsetForLeaderEpochRequestTopic) PrettyPrint() string {
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
func (value *OffsetForLeaderEpochRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Partition: %v\n", value.Partition)
	fmt.Fprintf(w, "                CurrentLeaderEpoch: %v\n", value.CurrentLeaderEpoch)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)

	return w.String()
}
