package describeproducers

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeProducersResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                             // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	Topics          *[]DescribeProducersResponseTopic // Each topic in the response. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeProducersResponseTopic struct {
	Name            *string                                    // The topic name. (versions: 0+)
	Partitions      *[]DescribeProducersResponseTopicPartition // Each partition in the response. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeProducersResponseTopicPartition struct {
	PartitionIndex  int32                                                    // The partition index. (versions: 0+)
	ErrorCode       int16                                                    // The partition error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage    *string                                                  // The partition error message, which may be null if no additional details are available. (versions: 0+, nullable: 0+)
	ActiveProducers *[]DescribeProducersResponseTopicPartitionActiveProducer // The active producers for the partition. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeProducersResponseTopicPartitionActiveProducer struct {
	ProducerId            int64 // The producer id. (versions: 0+)
	ProducerEpoch         int32 // The producer epoch. (versions: 0+)
	LastSequence          int32 // The last sequence number sent by the producer. (versions: 0+)
	LastTimestamp         int64 // The last timestamp sent by the producer. (versions: 0+)
	CoordinatorEpoch      int32 // The current epoch of the producer group. (versions: 0+)
	CurrentTxnStartOffset int64 // The current transaction start offset of the producer. (versions: 0+)
	rawTaggedFields       *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *DescribeProducersResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("DescribeProducersResponse.Topics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.topicsEncoder, res.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicsEncoder, *res.Topics); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields := []protocol.TaggedField{}
		if res.rawTaggedFields != nil {
			rawTaggedFields = *res.rawTaggedFields
		}
		if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (res *DescribeProducersResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeProducersResponse.Read: response or its body is nil")
	}

	*res = DescribeProducersResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// Topics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topics, err := protocol.ReadCompactArray(r, res.topicsDecoder)
		if err != nil {
			return err
		}
		res.Topics = &topics
	} else {
		topics, err := protocol.ReadArray(r, res.topicsDecoder)
		if err != nil {
			return err
		}
		res.Topics = &topics
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		res.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

func (res *DescribeProducersResponse) topicsEncoder(w io.Writer, value DescribeProducersResponseTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("DescribeProducersResponseTopic.Name must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
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
		return fmt.Errorf("DescribeProducersResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.partitionsEncoder, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.partitionsEncoder, *value.Partitions); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
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

func (res *DescribeProducersResponse) topicsDecoder(r io.Reader) (DescribeProducersResponseTopic, error) {
	describeproducersresponsetopic := DescribeProducersResponseTopic{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeproducersresponsetopic, err
		}
		describeproducersresponsetopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return describeproducersresponsetopic, err
		}
		describeproducersresponsetopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return describeproducersresponsetopic, err
		}
		describeproducersresponsetopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return describeproducersresponsetopic, err
		}
		describeproducersresponsetopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeproducersresponsetopic, err
		}
		describeproducersresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return describeproducersresponsetopic, nil
}

func (res *DescribeProducersResponse) partitionsEncoder(w io.Writer, value DescribeProducersResponseTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.ErrorMessage); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.ErrorMessage); err != nil {
			return err
		}
	}

	// ActiveProducers (versions: 0+)
	if value.ActiveProducers == nil {
		return fmt.Errorf("DescribeProducersResponseTopicPartition.ActiveProducers must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.activeProducersEncoder, value.ActiveProducers); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.activeProducersEncoder, *value.ActiveProducers); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
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

func (res *DescribeProducersResponse) partitionsDecoder(r io.Reader) (DescribeProducersResponseTopicPartition, error) {
	describeproducersresponsetopicpartition := DescribeProducersResponseTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return describeproducersresponsetopicpartition, err
	}
	describeproducersresponsetopicpartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return describeproducersresponsetopicpartition, err
	}
	describeproducersresponsetopicpartition.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return describeproducersresponsetopicpartition, err
		}
		describeproducersresponsetopicpartition.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return describeproducersresponsetopicpartition, err
		}
		describeproducersresponsetopicpartition.ErrorMessage = errormessage
	}

	// ActiveProducers (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		activeproducers, err := protocol.ReadCompactArray(r, res.activeProducersDecoder)
		if err != nil {
			return describeproducersresponsetopicpartition, err
		}
		describeproducersresponsetopicpartition.ActiveProducers = &activeproducers
	} else {
		activeproducers, err := protocol.ReadArray(r, res.activeProducersDecoder)
		if err != nil {
			return describeproducersresponsetopicpartition, err
		}
		describeproducersresponsetopicpartition.ActiveProducers = &activeproducers
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeproducersresponsetopicpartition, err
		}
		describeproducersresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return describeproducersresponsetopicpartition, nil
}

func (res *DescribeProducersResponse) activeProducersEncoder(w io.Writer, value DescribeProducersResponseTopicPartitionActiveProducer) error {
	// ProducerId (versions: 0+)
	if err := protocol.WriteInt64(w, value.ProducerId); err != nil {
		return err
	}

	// ProducerEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.ProducerEpoch); err != nil {
		return err
	}

	// LastSequence (versions: 0+)
	if err := protocol.WriteInt32(w, value.LastSequence); err != nil {
		return err
	}

	// LastTimestamp (versions: 0+)
	if err := protocol.WriteInt64(w, value.LastTimestamp); err != nil {
		return err
	}

	// CoordinatorEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.CoordinatorEpoch); err != nil {
		return err
	}

	// CurrentTxnStartOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.CurrentTxnStartOffset); err != nil {
		return err
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
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

func (res *DescribeProducersResponse) activeProducersDecoder(r io.Reader) (DescribeProducersResponseTopicPartitionActiveProducer, error) {
	describeproducersresponsetopicpartitionactiveproducer := DescribeProducersResponseTopicPartitionActiveProducer{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	describeproducersresponsetopicpartitionactiveproducer.LastSequence = -1
	describeproducersresponsetopicpartitionactiveproducer.LastTimestamp = -1
	describeproducersresponsetopicpartitionactiveproducer.CurrentTxnStartOffset = -1

	// ProducerId (versions: 0+)
	producerid, err := protocol.ReadInt64(r)
	if err != nil {
		return describeproducersresponsetopicpartitionactiveproducer, err
	}
	describeproducersresponsetopicpartitionactiveproducer.ProducerId = producerid

	// ProducerEpoch (versions: 0+)
	producerepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return describeproducersresponsetopicpartitionactiveproducer, err
	}
	describeproducersresponsetopicpartitionactiveproducer.ProducerEpoch = producerepoch

	// LastSequence (versions: 0+)
	lastsequence, err := protocol.ReadInt32(r)
	if err != nil {
		return describeproducersresponsetopicpartitionactiveproducer, err
	}
	describeproducersresponsetopicpartitionactiveproducer.LastSequence = lastsequence

	// LastTimestamp (versions: 0+)
	lasttimestamp, err := protocol.ReadInt64(r)
	if err != nil {
		return describeproducersresponsetopicpartitionactiveproducer, err
	}
	describeproducersresponsetopicpartitionactiveproducer.LastTimestamp = lasttimestamp

	// CoordinatorEpoch (versions: 0+)
	coordinatorepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return describeproducersresponsetopicpartitionactiveproducer, err
	}
	describeproducersresponsetopicpartitionactiveproducer.CoordinatorEpoch = coordinatorepoch

	// CurrentTxnStartOffset (versions: 0+)
	currenttxnstartoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return describeproducersresponsetopicpartitionactiveproducer, err
	}
	describeproducersresponsetopicpartitionactiveproducer.CurrentTxnStartOffset = currenttxnstartoffset

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeproducersresponsetopicpartitionactiveproducer, err
		}
		describeproducersresponsetopicpartitionactiveproducer.rawTaggedFields = &rawTaggedFields
	}

	return describeproducersresponsetopicpartitionactiveproducer, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeProducersResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeProducersResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	if res.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *res.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeProducersResponseTopic) PrettyPrint() string {
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
func (value *DescribeProducersResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                ErrorMessage: nil\n")
	}

	if value.ActiveProducers != nil {
		fmt.Fprintf(w, "                ActiveProducers:\n")
		for _, activeproducers := range *value.ActiveProducers {
			fmt.Fprintf(w, "%s", activeproducers.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                ActiveProducers: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeProducersResponseTopicPartitionActiveProducer) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    ProducerId: %v\n", value.ProducerId)
	fmt.Fprintf(w, "                    ProducerEpoch: %v\n", value.ProducerEpoch)
	fmt.Fprintf(w, "                    LastSequence: %v\n", value.LastSequence)
	fmt.Fprintf(w, "                    LastTimestamp: %v\n", value.LastTimestamp)
	fmt.Fprintf(w, "                    CoordinatorEpoch: %v\n", value.CoordinatorEpoch)
	fmt.Fprintf(w, "                    CurrentTxnStartOffset: %v\n", value.CurrentTxnStartOffset)

	return w.String()
}
