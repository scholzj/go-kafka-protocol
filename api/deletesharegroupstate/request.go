package deletesharegroupstate

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DeleteShareGroupStateRequest struct {
	ApiVersion      int16
	GroupId         *string                              // The group identifier. (versions: 0+)
	Topics          *[]DeleteShareGroupStateRequestTopic // The data for the topics. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DeleteShareGroupStateRequestTopic struct {
	TopicId         uuid.UUID                                     // The topic identifier. (versions: 0+)
	Partitions      *[]DeleteShareGroupStateRequestTopicPartition // The data for the partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DeleteShareGroupStateRequestTopicPartition struct {
	Partition       int32 // The partition index. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *DeleteShareGroupStateRequest) Write(w io.Writer) error {
	// GroupId (versions: 0+)
	if req.GroupId == nil {
		return fmt.Errorf("DeleteShareGroupStateRequest.GroupId must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("DeleteShareGroupStateRequest.Topics must not be nil in version %d", req.ApiVersion)
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
func (req *DeleteShareGroupStateRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DeleteShareGroupStateRequest.Read: request or its body is nil")
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

func (req *DeleteShareGroupStateRequest) topicsEncoder(w io.Writer, value DeleteShareGroupStateRequestTopic) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("DeleteShareGroupStateRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *DeleteShareGroupStateRequest) topicsDecoder(r io.Reader) (DeleteShareGroupStateRequestTopic, error) {
	deletesharegroupstaterequesttopic := DeleteShareGroupStateRequestTopic{}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return deletesharegroupstaterequesttopic, err
	}
	deletesharegroupstaterequesttopic.TopicId = topicid

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return deletesharegroupstaterequesttopic, err
		}
		deletesharegroupstaterequesttopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return deletesharegroupstaterequesttopic, err
		}
		deletesharegroupstaterequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return deletesharegroupstaterequesttopic, err
		}
		deletesharegroupstaterequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return deletesharegroupstaterequesttopic, nil
}

func (req *DeleteShareGroupStateRequest) partitionsEncoder(w io.Writer, value DeleteShareGroupStateRequestTopicPartition) error {
	// Partition (versions: 0+)
	if err := protocol.WriteInt32(w, value.Partition); err != nil {
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

func (req *DeleteShareGroupStateRequest) partitionsDecoder(r io.Reader) (DeleteShareGroupStateRequestTopicPartition, error) {
	deletesharegroupstaterequesttopicpartition := DeleteShareGroupStateRequestTopicPartition{}

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return deletesharegroupstaterequesttopicpartition, err
	}
	deletesharegroupstaterequesttopicpartition.Partition = partition

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return deletesharegroupstaterequesttopicpartition, err
		}
		deletesharegroupstaterequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return deletesharegroupstaterequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DeleteShareGroupStateRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DeleteShareGroupStateRequest:\n")

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
func (value *DeleteShareGroupStateRequestTopic) PrettyPrint() string {
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
func (value *DeleteShareGroupStateRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Partition: %v\n", value.Partition)

	return w.String()
}
