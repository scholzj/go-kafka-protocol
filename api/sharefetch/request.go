package sharefetch

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ShareFetchRequest struct {
	ApiVersion          int16
	GroupId             *string                                 // The group identifier.
	MemberId            *string                                 // The member ID.
	ShareSessionEpoch   int32                                   // The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
	MaxWaitMs           int32                                   // The maximum time in milliseconds to wait for the response.
	MinBytes            int32                                   // The minimum bytes to accumulate in the response.
	MaxBytes            int32                                   // The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
	MaxRecords          int32                                   // The maximum number of records to fetch. This limit can be exceeded for alignment of batch boundaries.
	BatchSize           int32                                   // The optimal number of records for batches of acquired records and acknowledgements.
	ShareAcquireMode    int8                                    // The acquire mode to control the fetch behavior - 0:batch-optimized,1:record-limit.
	IsRenewAck          bool                                    // Whether Renew type acknowledgements present in AcknowledgementBatches.
	Topics              *[]ShareFetchRequestTopic               // The topics to fetch.
	ForgottenTopicsData *[]ShareFetchRequestForgottenTopicsData // The partitions to remove from this share session.
	rawTaggedFields     *[]protocol.TaggedField
}

type ShareFetchRequestTopic struct {
	TopicId         uuid.UUID                          // The unique topic ID.
	Partitions      *[]ShareFetchRequestTopicPartition // The partitions to fetch.
	rawTaggedFields *[]protocol.TaggedField
}

type ShareFetchRequestTopicPartition struct {
	PartitionIndex         int32                                                   // The partition index.
	PartitionMaxBytes      int32                                                   // The maximum bytes to fetch from this partition. 0 when only acknowledgement with no fetching is required. See KIP-74 for cases where this limit may not be honored.
	AcknowledgementBatches *[]ShareFetchRequestTopicPartitionAcknowledgementBatche // Record batches to acknowledge.
	rawTaggedFields        *[]protocol.TaggedField
}

type ShareFetchRequestTopicPartitionAcknowledgementBatche struct {
	FirstOffset      int64   // First offset of batch of records to acknowledge.
	LastOffset       int64   // Last offset (inclusive) of batch of records to acknowledge.
	AcknowledgeTypes *[]int8 // Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
	rawTaggedFields  *[]protocol.TaggedField
}

type ShareFetchRequestForgottenTopicsData struct {
	TopicId         uuid.UUID // The unique topic ID.
	Partitions      *[]int32  // The partitions indexes to forget.
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *ShareFetchRequest) Write(w io.Writer) error {
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

	// MaxWaitMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.MaxWaitMs); err != nil {
		return err
	}

	// MinBytes (versions: 0+)
	if err := protocol.WriteInt32(w, req.MinBytes); err != nil {
		return err
	}

	// MaxBytes (versions: 0+)
	if err := protocol.WriteInt32(w, req.MaxBytes); err != nil {
		return err
	}

	// MaxRecords (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, req.MaxRecords); err != nil {
			return err
		}
	}

	// BatchSize (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, req.BatchSize); err != nil {
			return err
		}
	}

	// ShareAcquireMode (versions: 2+)
	if req.ApiVersion >= 2 {
		if err := protocol.WriteInt8(w, req.ShareAcquireMode); err != nil {
			return err
		}
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

	// ForgottenTopicsData (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.forgottenTopicsDataEncoder, req.ForgottenTopicsData); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.forgottenTopicsDataEncoder, *req.ForgottenTopicsData); err != nil {
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
func (req *ShareFetchRequest) Read(request protocol.Request) error {
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

	// MaxWaitMs (versions: 0+)
	maxwaitms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.MaxWaitMs = maxwaitms

	// MinBytes (versions: 0+)
	minbytes, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.MinBytes = minbytes

	// MaxBytes (versions: 0+)
	maxbytes, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.MaxBytes = maxbytes

	// MaxRecords (versions: 1+)
	if request.ApiVersion >= 1 {
		maxrecords, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.MaxRecords = maxrecords
	}

	// BatchSize (versions: 1+)
	if request.ApiVersion >= 1 {
		batchsize, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.BatchSize = batchsize
	}

	// ShareAcquireMode (versions: 2+)
	if request.ApiVersion >= 2 {
		shareacquiremode, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		req.ShareAcquireMode = shareacquiremode
	}

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

	// ForgottenTopicsData (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		forgottentopicsdata, err := protocol.ReadNullableCompactArray(r, req.forgottenTopicsDataDecoder)
		if err != nil {
			return err
		}
		req.ForgottenTopicsData = forgottentopicsdata
	} else {
		forgottentopicsdata, err := protocol.ReadArray(r, req.forgottenTopicsDataDecoder)
		if err != nil {
			return err
		}
		req.ForgottenTopicsData = &forgottentopicsdata
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

func (req *ShareFetchRequest) topicsEncoder(w io.Writer, value ShareFetchRequestTopic) error {
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

func (req *ShareFetchRequest) topicsDecoder(r io.Reader) (ShareFetchRequestTopic, error) {
	sharefetchrequesttopic := ShareFetchRequestTopic{}
	var err error

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return sharefetchrequesttopic, err
	}
	sharefetchrequesttopic.TopicId = topicid

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return sharefetchrequesttopic, err
		}
		sharefetchrequesttopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return sharefetchrequesttopic, err
		}
		sharefetchrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return sharefetchrequesttopic, err
		}
		sharefetchrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return sharefetchrequesttopic, nil
}

func (req *ShareFetchRequest) partitionsEncoder(w io.Writer, value ShareFetchRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// PartitionMaxBytes (versions: 0)
	if req.ApiVersion == 0 {
		if err := protocol.WriteInt32(w, value.PartitionMaxBytes); err != nil {
			return err
		}
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

func (req *ShareFetchRequest) partitionsDecoder(r io.Reader) (ShareFetchRequestTopicPartition, error) {
	sharefetchrequesttopicpartition := ShareFetchRequestTopicPartition{}
	var err error

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return sharefetchrequesttopicpartition, err
	}
	sharefetchrequesttopicpartition.PartitionIndex = partitionindex

	// PartitionMaxBytes (versions: 0)
	if req.ApiVersion == 0 {
		partitionmaxbytes, err := protocol.ReadInt32(r)
		if err != nil {
			return sharefetchrequesttopicpartition, err
		}
		sharefetchrequesttopicpartition.PartitionMaxBytes = partitionmaxbytes
	}

	// AcknowledgementBatches (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		acknowledgementbatches, err := protocol.ReadNullableCompactArray(r, req.acknowledgementBatchesDecoder)
		if err != nil {
			return sharefetchrequesttopicpartition, err
		}
		sharefetchrequesttopicpartition.AcknowledgementBatches = acknowledgementbatches
	} else {
		acknowledgementbatches, err := protocol.ReadArray(r, req.acknowledgementBatchesDecoder)
		if err != nil {
			return sharefetchrequesttopicpartition, err
		}
		sharefetchrequesttopicpartition.AcknowledgementBatches = &acknowledgementbatches
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return sharefetchrequesttopicpartition, err
		}
		sharefetchrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return sharefetchrequesttopicpartition, nil
}

func (req *ShareFetchRequest) acknowledgementBatchesEncoder(w io.Writer, value ShareFetchRequestTopicPartitionAcknowledgementBatche) error {
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

func (req *ShareFetchRequest) acknowledgementBatchesDecoder(r io.Reader) (ShareFetchRequestTopicPartitionAcknowledgementBatche, error) {
	sharefetchrequesttopicpartitionacknowledgementbatche := ShareFetchRequestTopicPartitionAcknowledgementBatche{}
	var err error

	// FirstOffset (versions: 0+)
	firstoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return sharefetchrequesttopicpartitionacknowledgementbatche, err
	}
	sharefetchrequesttopicpartitionacknowledgementbatche.FirstOffset = firstoffset

	// LastOffset (versions: 0+)
	lastoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return sharefetchrequesttopicpartitionacknowledgementbatche, err
	}
	sharefetchrequesttopicpartitionacknowledgementbatche.LastOffset = lastoffset

	// AcknowledgeTypes (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		acknowledgetypes, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt8)
		if err != nil {
			return sharefetchrequesttopicpartitionacknowledgementbatche, err
		}
		sharefetchrequesttopicpartitionacknowledgementbatche.AcknowledgeTypes = acknowledgetypes
	} else {
		acknowledgetypes, err := protocol.ReadArray(r, protocol.ReadInt8)
		if err != nil {
			return sharefetchrequesttopicpartitionacknowledgementbatche, err
		}
		sharefetchrequesttopicpartitionacknowledgementbatche.AcknowledgeTypes = &acknowledgetypes
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return sharefetchrequesttopicpartitionacknowledgementbatche, err
		}
		sharefetchrequesttopicpartitionacknowledgementbatche.rawTaggedFields = &rawTaggedFields
	}

	return sharefetchrequesttopicpartitionacknowledgementbatche, nil
}

func (req *ShareFetchRequest) forgottenTopicsDataEncoder(w io.Writer, value ShareFetchRequestForgottenTopicsData) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
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

func (req *ShareFetchRequest) forgottenTopicsDataDecoder(r io.Reader) (ShareFetchRequestForgottenTopicsData, error) {
	sharefetchrequestforgottentopicsdata := ShareFetchRequestForgottenTopicsData{}
	var err error

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return sharefetchrequestforgottentopicsdata, err
	}
	sharefetchrequestforgottentopicsdata.TopicId = topicid

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return sharefetchrequestforgottentopicsdata, err
		}
		sharefetchrequestforgottentopicsdata.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return sharefetchrequestforgottentopicsdata, err
		}
		sharefetchrequestforgottentopicsdata.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return sharefetchrequestforgottentopicsdata, err
		}
		sharefetchrequestforgottentopicsdata.rawTaggedFields = &rawTaggedFields
	}

	return sharefetchrequestforgottentopicsdata, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *ShareFetchRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ShareFetchRequest:\n")
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
	fmt.Fprintf(w, "        MaxWaitMs: %v\n", req.MaxWaitMs)
	fmt.Fprintf(w, "        MinBytes: %v\n", req.MinBytes)
	fmt.Fprintf(w, "        MaxBytes: %v\n", req.MaxBytes)
	fmt.Fprintf(w, "        MaxRecords: %v\n", req.MaxRecords)
	fmt.Fprintf(w, "        BatchSize: %v\n", req.BatchSize)
	fmt.Fprintf(w, "        ShareAcquireMode: %v\n", req.ShareAcquireMode)
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
	if req.ForgottenTopicsData != nil {
		fmt.Fprintf(w, "        ForgottenTopicsData:\n")
		for _, forgottentopicsdata := range *req.ForgottenTopicsData {
			fmt.Fprintf(w, "%s", forgottentopicsdata.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        ForgottenTopicsData: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ShareFetchRequestTopic) PrettyPrint() string {
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
func (value *ShareFetchRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                PartitionMaxBytes: %v\n", value.PartitionMaxBytes)
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
func (value *ShareFetchRequestTopicPartitionAcknowledgementBatche) PrettyPrint() string {
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

//goland:noinspection GoUnhandledErrorResult
func (value *ShareFetchRequestForgottenTopicsData) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            TopicId: %v\n", value.TopicId)
	if value.Partitions != nil {
		fmt.Fprintf(w, "            Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "            Partitions: nil\n")
	}

	return w.String()
}
