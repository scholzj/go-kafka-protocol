package txnoffsetcommit

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type TxnOffsetCommitRequest struct {
	ApiVersion      int16
	TransactionalId *string                        // The ID of the transaction. (versions: 0+)
	GroupId         *string                        // The ID of the group. (versions: 0+)
	ProducerId      int64                          // The current producer ID in use by the transactional ID. (versions: 0+)
	ProducerEpoch   int16                          // The current epoch associated with the producer ID. (versions: 0+)
	GenerationId    int32                          // The generation of the consumer. (versions: 3+)
	MemberId        *string                        // The member ID assigned by the group coordinator. (versions: 3+)
	GroupInstanceId *string                        // The unique identifier of the consumer instance provided by end user. (versions: 3+, nullable: 3+)
	Topics          *[]TxnOffsetCommitRequestTopic // Each topic that we want to commit offsets for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type TxnOffsetCommitRequestTopic struct {
	Name            *string                                 // The topic name. (versions: 0+)
	Partitions      *[]TxnOffsetCommitRequestTopicPartition // The partitions inside the topic that we want to commit offsets for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type TxnOffsetCommitRequestTopicPartition struct {
	PartitionIndex       int32   // The index of the partition within the topic. (versions: 0+)
	CommittedOffset      int64   // The message offset to be committed. (versions: 0+)
	CommittedLeaderEpoch int32   // The leader epoch of the last consumed record. (versions: 2+)
	CommittedMetadata    *string // Any associated metadata the client wants to keep. (versions: 0+, nullable: 0+)
	rawTaggedFields      *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (req *TxnOffsetCommitRequest) Write(w io.Writer) error {
	// TransactionalId (versions: 0+)
	if req.TransactionalId == nil {
		return fmt.Errorf("TxnOffsetCommitRequest.TransactionalId must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *req.TransactionalId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *req.TransactionalId); err != nil {
			return err
		}
	}

	// GroupId (versions: 0+)
	if req.GroupId == nil {
		return fmt.Errorf("TxnOffsetCommitRequest.GroupId must not be nil in version %d", req.ApiVersion)
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

	// ProducerId (versions: 0+)
	if err := protocol.WriteInt64(w, req.ProducerId); err != nil {
		return err
	}

	// ProducerEpoch (versions: 0+)
	if err := protocol.WriteInt16(w, req.ProducerEpoch); err != nil {
		return err
	}

	// GenerationId (versions: 3+)
	if req.ApiVersion >= 3 {
		if err := protocol.WriteInt32(w, req.GenerationId); err != nil {
			return err
		}
	}

	// MemberId (versions: 3+)
	if req.ApiVersion >= 3 {
		if req.MemberId == nil {
			return fmt.Errorf("TxnOffsetCommitRequest.MemberId must not be nil in version %d", req.ApiVersion)
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

	// GroupInstanceId (versions: 3+)
	if req.ApiVersion >= 3 {
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

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("TxnOffsetCommitRequest.Topics must not be nil in version %d", req.ApiVersion)
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
func (req *TxnOffsetCommitRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("TxnOffsetCommitRequest.Read: request or its body is nil")
	}

	*req = TxnOffsetCommitRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.GenerationId = -1

	// TransactionalId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		transactionalid, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		req.TransactionalId = &transactionalid
	} else {
		transactionalid, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		req.TransactionalId = &transactionalid
	}

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

	// ProducerId (versions: 0+)
	producerid, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	req.ProducerId = producerid

	// ProducerEpoch (versions: 0+)
	producerepoch, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	req.ProducerEpoch = producerepoch

	// GenerationId (versions: 3+)
	if req.ApiVersion >= 3 {
		generationid, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.GenerationId = generationid
	}

	// MemberId (versions: 3+)
	if req.ApiVersion >= 3 {
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

	// GroupInstanceId (versions: 3+)
	if req.ApiVersion >= 3 {
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

func (req *TxnOffsetCommitRequest) topicsEncoder(w io.Writer, value TxnOffsetCommitRequestTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("TxnOffsetCommitRequestTopic.Name must not be nil in version %d", req.ApiVersion)
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

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("TxnOffsetCommitRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *TxnOffsetCommitRequest) topicsDecoder(r io.Reader) (TxnOffsetCommitRequestTopic, error) {
	txnoffsetcommitrequesttopic := TxnOffsetCommitRequestTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return txnoffsetcommitrequesttopic, err
		}
		txnoffsetcommitrequesttopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return txnoffsetcommitrequesttopic, err
		}
		txnoffsetcommitrequesttopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return txnoffsetcommitrequesttopic, err
		}
		txnoffsetcommitrequesttopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return txnoffsetcommitrequesttopic, err
		}
		txnoffsetcommitrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return txnoffsetcommitrequesttopic, err
		}
		txnoffsetcommitrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return txnoffsetcommitrequesttopic, nil
}

func (req *TxnOffsetCommitRequest) partitionsEncoder(w io.Writer, value TxnOffsetCommitRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// CommittedOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.CommittedOffset); err != nil {
		return err
	}

	// CommittedLeaderEpoch (versions: 2+)
	if req.ApiVersion >= 2 {
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

func (req *TxnOffsetCommitRequest) partitionsDecoder(r io.Reader) (TxnOffsetCommitRequestTopicPartition, error) {
	txnoffsetcommitrequesttopicpartition := TxnOffsetCommitRequestTopicPartition{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	txnoffsetcommitrequesttopicpartition.CommittedLeaderEpoch = -1

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return txnoffsetcommitrequesttopicpartition, err
	}
	txnoffsetcommitrequesttopicpartition.PartitionIndex = partitionindex

	// CommittedOffset (versions: 0+)
	committedoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return txnoffsetcommitrequesttopicpartition, err
	}
	txnoffsetcommitrequesttopicpartition.CommittedOffset = committedoffset

	// CommittedLeaderEpoch (versions: 2+)
	if req.ApiVersion >= 2 {
		committedleaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return txnoffsetcommitrequesttopicpartition, err
		}
		txnoffsetcommitrequesttopicpartition.CommittedLeaderEpoch = committedleaderepoch
	}

	// CommittedMetadata (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		committedmetadata, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return txnoffsetcommitrequesttopicpartition, err
		}
		txnoffsetcommitrequesttopicpartition.CommittedMetadata = committedmetadata
	} else {
		committedmetadata, err := protocol.ReadNullableString(r)
		if err != nil {
			return txnoffsetcommitrequesttopicpartition, err
		}
		txnoffsetcommitrequesttopicpartition.CommittedMetadata = committedmetadata
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return txnoffsetcommitrequesttopicpartition, err
		}
		txnoffsetcommitrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return txnoffsetcommitrequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *TxnOffsetCommitRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> TxnOffsetCommitRequest:\n")

	if req.TransactionalId != nil {
		fmt.Fprintf(w, "        TransactionalId: %v\n", *req.TransactionalId)
	} else {
		fmt.Fprintf(w, "        TransactionalId: nil\n")
	}

	if req.GroupId != nil {
		fmt.Fprintf(w, "        GroupId: %v\n", *req.GroupId)
	} else {
		fmt.Fprintf(w, "        GroupId: nil\n")
	}

	fmt.Fprintf(w, "        ProducerId: %v\n", req.ProducerId)
	fmt.Fprintf(w, "        ProducerEpoch: %v\n", req.ProducerEpoch)
	fmt.Fprintf(w, "        GenerationId: %v\n", req.GenerationId)

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
func (value *TxnOffsetCommitRequestTopic) PrettyPrint() string {
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
func (value *TxnOffsetCommitRequestTopicPartition) PrettyPrint() string {
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
