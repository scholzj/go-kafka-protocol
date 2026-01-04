package shareacknowledge

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ShareAcknowledgeRequest struct {
	ApiVersion        int16
	GroupId           *string                         // The group identifier.
	MemberId          *string                         // The member ID.
	ShareSessionEpoch int32                           // The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
	IsRenewAck        bool                            // Whether Renew type acknowledgements present in AcknowledgementBatches.
	Topics            *[]ShareAcknowledgeRequestTopic // The topics containing records to acknowledge.
	rawTaggedFields   *[]protocol.TaggedField
}

type ShareAcknowledgeRequestTopic struct {
	TopicId         uuid.UUID                                // The unique topic ID.
	Partitions      *[]ShareAcknowledgeRequestTopicPartition // The partitions containing records to acknowledge.
	rawTaggedFields *[]protocol.TaggedField
}

type ShareAcknowledgeRequestTopicPartition struct {
	PartitionIndex         int32                                                         // The partition index.
	AcknowledgementBatches *[]ShareAcknowledgeRequestTopicPartitionAcknowledgementBatche // Record batches to acknowledge.
	rawTaggedFields        *[]protocol.TaggedField
}

type ShareAcknowledgeRequestTopicPartitionAcknowledgementBatche struct {
	FirstOffset      int64   // First offset of batch of records to acknowledge.
	LastOffset       int64   // Last offset (inclusive) of batch of records to acknowledge.
	AcknowledgeTypes *[]int8 // Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
	rawTaggedFields  *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *ShareAcknowledgeRequest) Write(w io.Writer) error {
	// GroupId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, req.GroupId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, req.GroupId); err != nil {
			return err
		}
	}

	// MemberId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, req.MemberId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, req.MemberId); err != nil {
			return err
		}
	}

	// ShareSessionEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, req.ShareSessionEpoch); err != nil {
		return err
	}

	// IsRenewAck (versions: 2+)
	if req.ApiVersion >= 2 {
		if err := protocol.WriteBool(w, req.IsRenewAck); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
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
func (req *ShareAcknowledgeRequest) Read(request protocol.Request) error {
	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	var err error

	// GroupId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		groupid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.GroupId = groupid
	} else {
		groupid, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		req.GroupId = groupid
	}

	// MemberId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		memberid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.MemberId = memberid
	} else {
		memberid, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		req.MemberId = memberid
	}

	// ShareSessionEpoch (versions: 0+)
	sharesessionepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.ShareSessionEpoch = sharesessionepoch

	// IsRenewAck (versions: 2+)
	if request.ApiVersion >= 2 {
		isrenewack, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.IsRenewAck = isrenewack
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
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		req.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

func (req *ShareAcknowledgeRequest) topicsEncoder(w io.Writer, value ShareAcknowledgeRequestTopic) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
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

func (req *ShareAcknowledgeRequest) topicsDecoder(r io.Reader) (ShareAcknowledgeRequestTopic, error) {
	shareacknowledgerequesttopic := ShareAcknowledgeRequestTopic{}
	var err error

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return shareacknowledgerequesttopic, err
	}
	shareacknowledgerequesttopic.TopicId = topicid

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return shareacknowledgerequesttopic, err
		}
		shareacknowledgerequesttopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return shareacknowledgerequesttopic, err
		}
		shareacknowledgerequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return shareacknowledgerequesttopic, err
		}
		shareacknowledgerequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return shareacknowledgerequesttopic, nil
}

func (req *ShareAcknowledgeRequest) partitionsEncoder(w io.Writer, value ShareAcknowledgeRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// AcknowledgementBatches (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.acknowledgementBatchesEncoder, value.AcknowledgementBatches); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.acknowledgementBatchesEncoder, *value.AcknowledgementBatches); err != nil {
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

func (req *ShareAcknowledgeRequest) partitionsDecoder(r io.Reader) (ShareAcknowledgeRequestTopicPartition, error) {
	shareacknowledgerequesttopicpartition := ShareAcknowledgeRequestTopicPartition{}
	var err error

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return shareacknowledgerequesttopicpartition, err
	}
	shareacknowledgerequesttopicpartition.PartitionIndex = partitionindex

	// AcknowledgementBatches (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		acknowledgementbatches, err := protocol.ReadNullableCompactArray(r, req.acknowledgementBatchesDecoder)
		if err != nil {
			return shareacknowledgerequesttopicpartition, err
		}
		shareacknowledgerequesttopicpartition.AcknowledgementBatches = acknowledgementbatches
	} else {
		acknowledgementbatches, err := protocol.ReadArray(r, req.acknowledgementBatchesDecoder)
		if err != nil {
			return shareacknowledgerequesttopicpartition, err
		}
		shareacknowledgerequesttopicpartition.AcknowledgementBatches = &acknowledgementbatches
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return shareacknowledgerequesttopicpartition, err
		}
		shareacknowledgerequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return shareacknowledgerequesttopicpartition, nil
}

func (req *ShareAcknowledgeRequest) acknowledgementBatchesEncoder(w io.Writer, value ShareAcknowledgeRequestTopicPartitionAcknowledgementBatche) error {
	// FirstOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.FirstOffset); err != nil {
		return err
	}

	// LastOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.LastOffset); err != nil {
		return err
	}

	// AcknowledgeTypes (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt8, value.AcknowledgeTypes); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt8, *value.AcknowledgeTypes); err != nil {
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

func (req *ShareAcknowledgeRequest) acknowledgementBatchesDecoder(r io.Reader) (ShareAcknowledgeRequestTopicPartitionAcknowledgementBatche, error) {
	shareacknowledgerequesttopicpartitionacknowledgementbatche := ShareAcknowledgeRequestTopicPartitionAcknowledgementBatche{}
	var err error

	// FirstOffset (versions: 0+)
	firstoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return shareacknowledgerequesttopicpartitionacknowledgementbatche, err
	}
	shareacknowledgerequesttopicpartitionacknowledgementbatche.FirstOffset = firstoffset

	// LastOffset (versions: 0+)
	lastoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return shareacknowledgerequesttopicpartitionacknowledgementbatche, err
	}
	shareacknowledgerequesttopicpartitionacknowledgementbatche.LastOffset = lastoffset

	// AcknowledgeTypes (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		acknowledgetypes, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt8)
		if err != nil {
			return shareacknowledgerequesttopicpartitionacknowledgementbatche, err
		}
		shareacknowledgerequesttopicpartitionacknowledgementbatche.AcknowledgeTypes = acknowledgetypes
	} else {
		acknowledgetypes, err := protocol.ReadArray(r, protocol.ReadInt8)
		if err != nil {
			return shareacknowledgerequesttopicpartitionacknowledgementbatche, err
		}
		shareacknowledgerequesttopicpartitionacknowledgementbatche.AcknowledgeTypes = &acknowledgetypes
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return shareacknowledgerequesttopicpartitionacknowledgementbatche, err
		}
		shareacknowledgerequesttopicpartitionacknowledgementbatche.rawTaggedFields = &rawTaggedFields
	}

	return shareacknowledgerequesttopicpartitionacknowledgementbatche, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *ShareAcknowledgeRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ShareAcknowledgeRequest:\n")
	if req.GroupId != nil {
		fmt.Fprintf(w, "        GroupId: %v\n", *req.GroupId)
	} else {
		fmt.Fprintf(w, "        GroupId: nil\n")
	}
	if req.MemberId != nil {
		fmt.Fprintf(w, "        MemberId: %v\n", *req.MemberId)
	} else {
		fmt.Fprintf(w, "        MemberId: nil\n")
	}
	fmt.Fprintf(w, "        ShareSessionEpoch: %v\n", req.ShareSessionEpoch)
	fmt.Fprintf(w, "        IsRenewAck: %v\n", req.IsRenewAck)
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
func (value *ShareAcknowledgeRequestTopic) PrettyPrint() string {
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
func (value *ShareAcknowledgeRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	if value.AcknowledgementBatches != nil {
		fmt.Fprintf(w, "                AcknowledgementBatches:\n")
		for _, acknowledgementbatches := range *value.AcknowledgementBatches {
			fmt.Fprintf(w, "%s", acknowledgementbatches.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                AcknowledgementBatches: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ShareAcknowledgeRequestTopicPartitionAcknowledgementBatche) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    FirstOffset: %v\n", value.FirstOffset)
	fmt.Fprintf(w, "                    LastOffset: %v\n", value.LastOffset)
	if value.AcknowledgeTypes != nil {
		fmt.Fprintf(w, "                    AcknowledgeTypes: %v\n", *value.AcknowledgeTypes)
	} else {
		fmt.Fprintf(w, "                    AcknowledgeTypes: nil\n")
	}

	return w.String()
}
