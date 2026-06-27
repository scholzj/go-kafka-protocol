package alterpartition

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AlterPartitionRequest struct {
	ApiVersion      int16
	BrokerId        int32                         // The ID of the requesting broker. (versions: 0+)
	BrokerEpoch     int64                         // The epoch of the requesting broker. (versions: 0+)
	Topics          *[]AlterPartitionRequestTopic // The topics to alter ISRs for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterPartitionRequestTopic struct {
	TopicId         uuid.UUID                              // The ID of the topic to alter ISRs for. (versions: 2+)
	Partitions      *[]AlterPartitionRequestTopicPartition // The partitions to alter ISRs for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterPartitionRequestTopicPartition struct {
	PartitionIndex      int32                                                 // The partition index. (versions: 0+)
	LeaderEpoch         int32                                                 // The leader epoch of this partition. (versions: 0+)
	NewIsr              *[]int32                                              // The ISR for this partition. Deprecated since version 3. (versions: 0-2)
	NewIsrWithEpochs    *[]AlterPartitionRequestTopicPartitionNewIsrWithEpoch // The ISR for this partition. (versions: 3+)
	LeaderRecoveryState int8                                                  // 1 if the partition is recovering from an unclean leader election; 0 otherwise. (versions: 1+)
	PartitionEpoch      int32                                                 // The expected epoch of the partition which is being updated. (versions: 0+)
	rawTaggedFields     *[]protocol.TaggedField
}

type AlterPartitionRequestTopicPartitionNewIsrWithEpoch struct {
	BrokerId        int32 // The ID of the broker. (versions: 3+)
	BrokerEpoch     int64 // The epoch of the broker. It will be -1 if the epoch check is not supported. (versions: 3+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *AlterPartitionRequest) Write(w io.Writer) error {
	// BrokerId (versions: 0+)
	if err := protocol.WriteInt32(w, req.BrokerId); err != nil {
		return err
	}

	// BrokerEpoch (versions: 0+)
	if err := protocol.WriteInt64(w, req.BrokerEpoch); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("AlterPartitionRequest.Topics must not be nil in version %d", req.ApiVersion)
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
func (req *AlterPartitionRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("AlterPartitionRequest.Read: request or its body is nil")
	}

	*req = AlterPartitionRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.BrokerEpoch = -1

	// BrokerId (versions: 0+)
	brokerid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.BrokerId = brokerid

	// BrokerEpoch (versions: 0+)
	brokerepoch, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	req.BrokerEpoch = brokerepoch

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

func (req *AlterPartitionRequest) topicsEncoder(w io.Writer, value AlterPartitionRequestTopic) error {
	// TopicId (versions: 2+)
	if req.ApiVersion >= 2 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("AlterPartitionRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *AlterPartitionRequest) topicsDecoder(r io.Reader) (AlterPartitionRequestTopic, error) {
	alterpartitionrequesttopic := AlterPartitionRequestTopic{}

	// TopicId (versions: 2+)
	if req.ApiVersion >= 2 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return alterpartitionrequesttopic, err
		}
		alterpartitionrequesttopic.TopicId = topicid
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return alterpartitionrequesttopic, err
		}
		alterpartitionrequesttopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return alterpartitionrequesttopic, err
		}
		alterpartitionrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterpartitionrequesttopic, err
		}
		alterpartitionrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return alterpartitionrequesttopic, nil
}

func (req *AlterPartitionRequest) partitionsEncoder(w io.Writer, value AlterPartitionRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// LeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
		return err
	}

	// NewIsr (versions: 0-2)
	if req.ApiVersion <= 2 {
		if value.NewIsr == nil {
			return fmt.Errorf("AlterPartitionRequestTopicPartition.NewIsr must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.NewIsr); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteInt32, *value.NewIsr); err != nil {
				return err
			}
		}
	}

	// NewIsrWithEpochs (versions: 3+)
	if req.ApiVersion >= 3 {
		if value.NewIsrWithEpochs == nil {
			return fmt.Errorf("AlterPartitionRequestTopicPartition.NewIsrWithEpochs must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, req.newIsrWithEpochsEncoder, value.NewIsrWithEpochs); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, req.newIsrWithEpochsEncoder, *value.NewIsrWithEpochs); err != nil {
				return err
			}
		}
	}

	// LeaderRecoveryState (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, value.LeaderRecoveryState); err != nil {
			return err
		}
	}

	// PartitionEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionEpoch); err != nil {
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

func (req *AlterPartitionRequest) partitionsDecoder(r io.Reader) (AlterPartitionRequestTopicPartition, error) {
	alterpartitionrequesttopicpartition := AlterPartitionRequestTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return alterpartitionrequesttopicpartition, err
	}
	alterpartitionrequesttopicpartition.PartitionIndex = partitionindex

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return alterpartitionrequesttopicpartition, err
	}
	alterpartitionrequesttopicpartition.LeaderEpoch = leaderepoch

	// NewIsr (versions: 0-2)
	if req.ApiVersion <= 2 {
		if isRequestFlexible(req.ApiVersion) {
			newisr, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
			if err != nil {
				return alterpartitionrequesttopicpartition, err
			}
			alterpartitionrequesttopicpartition.NewIsr = &newisr
		} else {
			newisr, err := protocol.ReadArray(r, protocol.ReadInt32)
			if err != nil {
				return alterpartitionrequesttopicpartition, err
			}
			alterpartitionrequesttopicpartition.NewIsr = &newisr
		}
	}

	// NewIsrWithEpochs (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			newisrwithepochs, err := protocol.ReadCompactArray(r, req.newIsrWithEpochsDecoder)
			if err != nil {
				return alterpartitionrequesttopicpartition, err
			}
			alterpartitionrequesttopicpartition.NewIsrWithEpochs = &newisrwithepochs
		} else {
			newisrwithepochs, err := protocol.ReadArray(r, req.newIsrWithEpochsDecoder)
			if err != nil {
				return alterpartitionrequesttopicpartition, err
			}
			alterpartitionrequesttopicpartition.NewIsrWithEpochs = &newisrwithepochs
		}
	}

	// LeaderRecoveryState (versions: 1+)
	if req.ApiVersion >= 1 {
		leaderrecoverystate, err := protocol.ReadInt8(r)
		if err != nil {
			return alterpartitionrequesttopicpartition, err
		}
		alterpartitionrequesttopicpartition.LeaderRecoveryState = leaderrecoverystate
	}

	// PartitionEpoch (versions: 0+)
	partitionepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return alterpartitionrequesttopicpartition, err
	}
	alterpartitionrequesttopicpartition.PartitionEpoch = partitionepoch

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterpartitionrequesttopicpartition, err
		}
		alterpartitionrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return alterpartitionrequesttopicpartition, nil
}

func (req *AlterPartitionRequest) newIsrWithEpochsEncoder(w io.Writer, value AlterPartitionRequestTopicPartitionNewIsrWithEpoch) error {
	// BrokerId (versions: 3+)
	if req.ApiVersion >= 3 {
		if err := protocol.WriteInt32(w, value.BrokerId); err != nil {
			return err
		}
	}

	// BrokerEpoch (versions: 3+)
	if req.ApiVersion >= 3 {
		if err := protocol.WriteInt64(w, value.BrokerEpoch); err != nil {
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

func (req *AlterPartitionRequest) newIsrWithEpochsDecoder(r io.Reader) (AlterPartitionRequestTopicPartitionNewIsrWithEpoch, error) {
	alterpartitionrequesttopicpartitionnewisrwithepoch := AlterPartitionRequestTopicPartitionNewIsrWithEpoch{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	alterpartitionrequesttopicpartitionnewisrwithepoch.BrokerEpoch = -1

	// BrokerId (versions: 3+)
	if req.ApiVersion >= 3 {
		brokerid, err := protocol.ReadInt32(r)
		if err != nil {
			return alterpartitionrequesttopicpartitionnewisrwithepoch, err
		}
		alterpartitionrequesttopicpartitionnewisrwithepoch.BrokerId = brokerid
	}

	// BrokerEpoch (versions: 3+)
	if req.ApiVersion >= 3 {
		brokerepoch, err := protocol.ReadInt64(r)
		if err != nil {
			return alterpartitionrequesttopicpartitionnewisrwithepoch, err
		}
		alterpartitionrequesttopicpartitionnewisrwithepoch.BrokerEpoch = brokerepoch
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterpartitionrequesttopicpartitionnewisrwithepoch, err
		}
		alterpartitionrequesttopicpartitionnewisrwithepoch.rawTaggedFields = &rawTaggedFields
	}

	return alterpartitionrequesttopicpartitionnewisrwithepoch, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *AlterPartitionRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> AlterPartitionRequest:\n")
	fmt.Fprintf(w, "        BrokerId: %v\n", req.BrokerId)
	fmt.Fprintf(w, "        BrokerEpoch: %v\n", req.BrokerEpoch)

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
func (value *AlterPartitionRequestTopic) PrettyPrint() string {
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
func (value *AlterPartitionRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)

	if value.NewIsr != nil {
		fmt.Fprintf(w, "                NewIsr: %v\n", *value.NewIsr)
	} else {
		fmt.Fprintf(w, "                NewIsr: nil\n")
	}

	if value.NewIsrWithEpochs != nil {
		fmt.Fprintf(w, "                NewIsrWithEpochs:\n")
		for _, newisrwithepochs := range *value.NewIsrWithEpochs {
			fmt.Fprintf(w, "%s", newisrwithepochs.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                NewIsrWithEpochs: nil\n")
	}

	fmt.Fprintf(w, "                LeaderRecoveryState: %v\n", value.LeaderRecoveryState)
	fmt.Fprintf(w, "                PartitionEpoch: %v\n", value.PartitionEpoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterPartitionRequestTopicPartitionNewIsrWithEpoch) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    BrokerId: %v\n", value.BrokerId)
	fmt.Fprintf(w, "                    BrokerEpoch: %v\n", value.BrokerEpoch)

	return w.String()
}
