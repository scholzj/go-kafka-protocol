package initializesharegroupstate

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type InitializeShareGroupStateRequest struct {
	ApiVersion      int16
	GroupId         *string                                  // The group identifier. (versions: 0+)
	Topics          *[]InitializeShareGroupStateRequestTopic // The data for the topics. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type InitializeShareGroupStateRequestTopic struct {
	TopicId         uuid.UUID                                         // The topic identifier. (versions: 0+)
	Partitions      *[]InitializeShareGroupStateRequestTopicPartition // The data for the partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type InitializeShareGroupStateRequestTopicPartition struct {
	Partition       int32 // The partition index. (versions: 0+)
	StateEpoch      int32 // The state epoch for this share-partition. (versions: 0+)
	StartOffset     int64 // The share-partition start offset, or -1 if the start offset is not being initialized. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *InitializeShareGroupStateRequest) Write(w io.Writer) error {
	// GroupId (versions: 0+)
	if req.GroupId == nil {
		return fmt.Errorf("InitializeShareGroupStateRequest.GroupId must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("InitializeShareGroupStateRequest.Topics must not be nil in version %d", req.ApiVersion)
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
func (req *InitializeShareGroupStateRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("InitializeShareGroupStateRequest.Read: request or its body is nil")
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

func (req *InitializeShareGroupStateRequest) topicsEncoder(w io.Writer, value InitializeShareGroupStateRequestTopic) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("InitializeShareGroupStateRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *InitializeShareGroupStateRequest) topicsDecoder(r io.Reader) (InitializeShareGroupStateRequestTopic, error) {
	initializesharegroupstaterequesttopic := InitializeShareGroupStateRequestTopic{}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return initializesharegroupstaterequesttopic, err
	}
	initializesharegroupstaterequesttopic.TopicId = topicid

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return initializesharegroupstaterequesttopic, err
		}
		initializesharegroupstaterequesttopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return initializesharegroupstaterequesttopic, err
		}
		initializesharegroupstaterequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return initializesharegroupstaterequesttopic, err
		}
		initializesharegroupstaterequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return initializesharegroupstaterequesttopic, nil
}

func (req *InitializeShareGroupStateRequest) partitionsEncoder(w io.Writer, value InitializeShareGroupStateRequestTopicPartition) error {
	// Partition (versions: 0+)
	if err := protocol.WriteInt32(w, value.Partition); err != nil {
		return err
	}

	// StateEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.StateEpoch); err != nil {
		return err
	}

	// StartOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.StartOffset); err != nil {
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

func (req *InitializeShareGroupStateRequest) partitionsDecoder(r io.Reader) (InitializeShareGroupStateRequestTopicPartition, error) {
	initializesharegroupstaterequesttopicpartition := InitializeShareGroupStateRequestTopicPartition{}

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return initializesharegroupstaterequesttopicpartition, err
	}
	initializesharegroupstaterequesttopicpartition.Partition = partition

	// StateEpoch (versions: 0+)
	stateepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return initializesharegroupstaterequesttopicpartition, err
	}
	initializesharegroupstaterequesttopicpartition.StateEpoch = stateepoch

	// StartOffset (versions: 0+)
	startoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return initializesharegroupstaterequesttopicpartition, err
	}
	initializesharegroupstaterequesttopicpartition.StartOffset = startoffset

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return initializesharegroupstaterequesttopicpartition, err
		}
		initializesharegroupstaterequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return initializesharegroupstaterequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *InitializeShareGroupStateRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> InitializeShareGroupStateRequest:\n")

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
func (value *InitializeShareGroupStateRequestTopic) PrettyPrint() string {
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
func (value *InitializeShareGroupStateRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Partition: %v\n", value.Partition)
	fmt.Fprintf(w, "                StateEpoch: %v\n", value.StateEpoch)
	fmt.Fprintf(w, "                StartOffset: %v\n", value.StartOffset)

	return w.String()
}
