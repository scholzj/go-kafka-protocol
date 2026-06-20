package writesharegroupstate

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type WriteShareGroupStateRequest struct {
	ApiVersion      int16
	GroupId         *string                             // The group identifier. (versions: 0+)
	Topics          *[]WriteShareGroupStateRequestTopic // The data for the topics. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type WriteShareGroupStateRequestTopic struct {
	TopicId         uuid.UUID                                    // The topic identifier. (versions: 0+)
	Partitions      *[]WriteShareGroupStateRequestTopicPartition // The data for the partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type WriteShareGroupStateRequestTopicPartition struct {
	Partition             int32                                                   // The partition index. (versions: 0+)
	StateEpoch            int32                                                   // The state epoch of the share-partition. (versions: 0+)
	LeaderEpoch           int32                                                   // The leader epoch of the share-partition. (versions: 0+)
	StartOffset           int64                                                   // The share-partition start offset, or -1 if the start offset is not being written. (versions: 0+)
	DeliveryCompleteCount int32                                                   // The number of offsets greater than or equal to share-partition start offset for which delivery has been completed. (versions: 1+)
	StateBatches          *[]WriteShareGroupStateRequestTopicPartitionStateBatche // The state batches for the share-partition. (versions: 0+)
	rawTaggedFields       *[]protocol.TaggedField
}

type WriteShareGroupStateRequestTopicPartitionStateBatche struct {
	FirstOffset     int64 // The first offset of this state batch. (versions: 0+)
	LastOffset      int64 // The last offset of this state batch. (versions: 0+)
	DeliveryState   int8  // The delivery state - 0:Available,2:Acked,4:Archived. (versions: 0+)
	DeliveryCount   int16 // The delivery count. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *WriteShareGroupStateRequest) Write(w io.Writer) error {
	// GroupId (versions: 0+)
	if req.GroupId == nil {
		return fmt.Errorf("WriteShareGroupStateRequest.GroupId must not be nil in version %d", req.ApiVersion)
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

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("WriteShareGroupStateRequest.Topics must not be nil in version %d", req.ApiVersion)
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
func (req *WriteShareGroupStateRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("WriteShareGroupStateRequest.Read: request or its body is nil")
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

func (req *WriteShareGroupStateRequest) topicsEncoder(w io.Writer, value WriteShareGroupStateRequestTopic) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("WriteShareGroupStateRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *WriteShareGroupStateRequest) topicsDecoder(r io.Reader) (WriteShareGroupStateRequestTopic, error) {
	writesharegroupstaterequesttopic := WriteShareGroupStateRequestTopic{}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return writesharegroupstaterequesttopic, err
	}
	writesharegroupstaterequesttopic.TopicId = topicid

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return writesharegroupstaterequesttopic, err
		}
		writesharegroupstaterequesttopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return writesharegroupstaterequesttopic, err
		}
		writesharegroupstaterequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return writesharegroupstaterequesttopic, err
		}
		writesharegroupstaterequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return writesharegroupstaterequesttopic, nil
}

func (req *WriteShareGroupStateRequest) partitionsEncoder(w io.Writer, value WriteShareGroupStateRequestTopicPartition) error {
	// Partition (versions: 0+)
	if err := protocol.WriteInt32(w, value.Partition); err != nil {
		return err
	}

	// StateEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.StateEpoch); err != nil {
		return err
	}

	// LeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
		return err
	}

	// StartOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.StartOffset); err != nil {
		return err
	}

	// DeliveryCompleteCount (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, value.DeliveryCompleteCount); err != nil {
			return err
		}
	}

	// StateBatches (versions: 0+)
	if value.StateBatches == nil {
		return fmt.Errorf("WriteShareGroupStateRequestTopicPartition.StateBatches must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.stateBatchesEncoder, value.StateBatches); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.stateBatchesEncoder, *value.StateBatches); err != nil {
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

func (req *WriteShareGroupStateRequest) partitionsDecoder(r io.Reader) (WriteShareGroupStateRequestTopicPartition, error) {
	writesharegroupstaterequesttopicpartition := WriteShareGroupStateRequestTopicPartition{}

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return writesharegroupstaterequesttopicpartition, err
	}
	writesharegroupstaterequesttopicpartition.Partition = partition

	// StateEpoch (versions: 0+)
	stateepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return writesharegroupstaterequesttopicpartition, err
	}
	writesharegroupstaterequesttopicpartition.StateEpoch = stateepoch

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return writesharegroupstaterequesttopicpartition, err
	}
	writesharegroupstaterequesttopicpartition.LeaderEpoch = leaderepoch

	// StartOffset (versions: 0+)
	startoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return writesharegroupstaterequesttopicpartition, err
	}
	writesharegroupstaterequesttopicpartition.StartOffset = startoffset

	// DeliveryCompleteCount (versions: 1+)
	if req.ApiVersion >= 1 {
		deliverycompletecount, err := protocol.ReadInt32(r)
		if err != nil {
			return writesharegroupstaterequesttopicpartition, err
		}
		writesharegroupstaterequesttopicpartition.DeliveryCompleteCount = deliverycompletecount
	}

	// StateBatches (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		statebatches, err := protocol.ReadNullableCompactArray(r, req.stateBatchesDecoder)
		if err != nil {
			return writesharegroupstaterequesttopicpartition, err
		}
		writesharegroupstaterequesttopicpartition.StateBatches = statebatches
	} else {
		statebatches, err := protocol.ReadArray(r, req.stateBatchesDecoder)
		if err != nil {
			return writesharegroupstaterequesttopicpartition, err
		}
		writesharegroupstaterequesttopicpartition.StateBatches = &statebatches
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return writesharegroupstaterequesttopicpartition, err
		}
		writesharegroupstaterequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return writesharegroupstaterequesttopicpartition, nil
}

func (req *WriteShareGroupStateRequest) stateBatchesEncoder(w io.Writer, value WriteShareGroupStateRequestTopicPartitionStateBatche) error {
	// FirstOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.FirstOffset); err != nil {
		return err
	}

	// LastOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.LastOffset); err != nil {
		return err
	}

	// DeliveryState (versions: 0+)
	if err := protocol.WriteInt8(w, value.DeliveryState); err != nil {
		return err
	}

	// DeliveryCount (versions: 0+)
	if err := protocol.WriteInt16(w, value.DeliveryCount); err != nil {
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

func (req *WriteShareGroupStateRequest) stateBatchesDecoder(r io.Reader) (WriteShareGroupStateRequestTopicPartitionStateBatche, error) {
	writesharegroupstaterequesttopicpartitionstatebatche := WriteShareGroupStateRequestTopicPartitionStateBatche{}

	// FirstOffset (versions: 0+)
	firstoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return writesharegroupstaterequesttopicpartitionstatebatche, err
	}
	writesharegroupstaterequesttopicpartitionstatebatche.FirstOffset = firstoffset

	// LastOffset (versions: 0+)
	lastoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return writesharegroupstaterequesttopicpartitionstatebatche, err
	}
	writesharegroupstaterequesttopicpartitionstatebatche.LastOffset = lastoffset

	// DeliveryState (versions: 0+)
	deliverystate, err := protocol.ReadInt8(r)
	if err != nil {
		return writesharegroupstaterequesttopicpartitionstatebatche, err
	}
	writesharegroupstaterequesttopicpartitionstatebatche.DeliveryState = deliverystate

	// DeliveryCount (versions: 0+)
	deliverycount, err := protocol.ReadInt16(r)
	if err != nil {
		return writesharegroupstaterequesttopicpartitionstatebatche, err
	}
	writesharegroupstaterequesttopicpartitionstatebatche.DeliveryCount = deliverycount

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return writesharegroupstaterequesttopicpartitionstatebatche, err
		}
		writesharegroupstaterequesttopicpartitionstatebatche.rawTaggedFields = &rawTaggedFields
	}

	return writesharegroupstaterequesttopicpartitionstatebatche, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *WriteShareGroupStateRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> WriteShareGroupStateRequest:\n")

	if req.GroupId != nil {
		fmt.Fprintf(w, "        GroupId: %v\n", *req.GroupId)
	} else {
		fmt.Fprintf(w, "        GroupId: nil\n")
	}

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
func (value *WriteShareGroupStateRequestTopic) PrettyPrint() string {
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
func (value *WriteShareGroupStateRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Partition: %v\n", value.Partition)
	fmt.Fprintf(w, "                StateEpoch: %v\n", value.StateEpoch)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)
	fmt.Fprintf(w, "                StartOffset: %v\n", value.StartOffset)
	fmt.Fprintf(w, "                DeliveryCompleteCount: %v\n", value.DeliveryCompleteCount)

	if value.StateBatches != nil {
		fmt.Fprintf(w, "                StateBatches:\n")
		for _, statebatches := range *value.StateBatches {
			fmt.Fprintf(w, "%s", statebatches.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                StateBatches: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *WriteShareGroupStateRequestTopicPartitionStateBatche) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    FirstOffset: %v\n", value.FirstOffset)
	fmt.Fprintf(w, "                    LastOffset: %v\n", value.LastOffset)
	fmt.Fprintf(w, "                    DeliveryState: %v\n", value.DeliveryState)
	fmt.Fprintf(w, "                    DeliveryCount: %v\n", value.DeliveryCount)

	return w.String()
}
