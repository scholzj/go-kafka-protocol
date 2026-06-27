package readsharegroupstate

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ReadShareGroupStateResponse struct {
	ApiVersion      int16
	Results         *[]ReadShareGroupStateResponseResult // The read results. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ReadShareGroupStateResponseResult struct {
	TopicId         uuid.UUID                                     // The topic identifier. (versions: 0+)
	Partitions      *[]ReadShareGroupStateResponseResultPartition // The results for the partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ReadShareGroupStateResponseResultPartition struct {
	Partition       int32                                                    // The partition index. (versions: 0+)
	ErrorCode       int16                                                    // The error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage    *string                                                  // The error message, or null if there was no error. (versions: 0+, nullable: 0+)
	StateEpoch      int32                                                    // The state epoch of the share-partition. (versions: 0+)
	StartOffset     int64                                                    // The share-partition start offset, which can be -1 if it is not yet initialized. (versions: 0+)
	StateBatches    *[]ReadShareGroupStateResponseResultPartitionStateBatche // The state batches for this share-partition. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ReadShareGroupStateResponseResultPartitionStateBatche struct {
	FirstOffset     int64 // The first offset of this state batch. (versions: 0+)
	LastOffset      int64 // The last offset of this state batch. (versions: 0+)
	DeliveryState   int8  // The delivery state - 0:Available,2:Acked,4:Archived. (versions: 0+)
	DeliveryCount   int16 // The delivery count. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *ReadShareGroupStateResponse) Write(w io.Writer) error {
	// Results (versions: 0+)
	if res.Results == nil {
		return fmt.Errorf("ReadShareGroupStateResponse.Results must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.resultsEncoder, res.Results); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.resultsEncoder, *res.Results); err != nil {
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
func (res *ReadShareGroupStateResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("ReadShareGroupStateResponse.Read: response or its body is nil")
	}

	*res = ReadShareGroupStateResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// Results (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		results, err := protocol.ReadCompactArray(r, res.resultsDecoder)
		if err != nil {
			return err
		}
		res.Results = &results
	} else {
		results, err := protocol.ReadArray(r, res.resultsDecoder)
		if err != nil {
			return err
		}
		res.Results = &results
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

func (res *ReadShareGroupStateResponse) resultsEncoder(w io.Writer, value ReadShareGroupStateResponseResult) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("ReadShareGroupStateResponseResult.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *ReadShareGroupStateResponse) resultsDecoder(r io.Reader) (ReadShareGroupStateResponseResult, error) {
	readsharegroupstateresponseresult := ReadShareGroupStateResponseResult{}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return readsharegroupstateresponseresult, err
	}
	readsharegroupstateresponseresult.TopicId = topicid

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return readsharegroupstateresponseresult, err
		}
		readsharegroupstateresponseresult.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return readsharegroupstateresponseresult, err
		}
		readsharegroupstateresponseresult.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return readsharegroupstateresponseresult, err
		}
		readsharegroupstateresponseresult.rawTaggedFields = &rawTaggedFields
	}

	return readsharegroupstateresponseresult, nil
}

func (res *ReadShareGroupStateResponse) partitionsEncoder(w io.Writer, value ReadShareGroupStateResponseResultPartition) error {
	// Partition (versions: 0+)
	if err := protocol.WriteInt32(w, value.Partition); err != nil {
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

	// StateEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.StateEpoch); err != nil {
		return err
	}

	// StartOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.StartOffset); err != nil {
		return err
	}

	// StateBatches (versions: 0+)
	if value.StateBatches == nil {
		return fmt.Errorf("ReadShareGroupStateResponseResultPartition.StateBatches must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.stateBatchesEncoder, value.StateBatches); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.stateBatchesEncoder, *value.StateBatches); err != nil {
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

func (res *ReadShareGroupStateResponse) partitionsDecoder(r io.Reader) (ReadShareGroupStateResponseResultPartition, error) {
	readsharegroupstateresponseresultpartition := ReadShareGroupStateResponseResultPartition{}

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return readsharegroupstateresponseresultpartition, err
	}
	readsharegroupstateresponseresultpartition.Partition = partition

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return readsharegroupstateresponseresultpartition, err
	}
	readsharegroupstateresponseresultpartition.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return readsharegroupstateresponseresultpartition, err
		}
		readsharegroupstateresponseresultpartition.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return readsharegroupstateresponseresultpartition, err
		}
		readsharegroupstateresponseresultpartition.ErrorMessage = errormessage
	}

	// StateEpoch (versions: 0+)
	stateepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return readsharegroupstateresponseresultpartition, err
	}
	readsharegroupstateresponseresultpartition.StateEpoch = stateepoch

	// StartOffset (versions: 0+)
	startoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return readsharegroupstateresponseresultpartition, err
	}
	readsharegroupstateresponseresultpartition.StartOffset = startoffset

	// StateBatches (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		statebatches, err := protocol.ReadCompactArray(r, res.stateBatchesDecoder)
		if err != nil {
			return readsharegroupstateresponseresultpartition, err
		}
		readsharegroupstateresponseresultpartition.StateBatches = &statebatches
	} else {
		statebatches, err := protocol.ReadArray(r, res.stateBatchesDecoder)
		if err != nil {
			return readsharegroupstateresponseresultpartition, err
		}
		readsharegroupstateresponseresultpartition.StateBatches = &statebatches
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return readsharegroupstateresponseresultpartition, err
		}
		readsharegroupstateresponseresultpartition.rawTaggedFields = &rawTaggedFields
	}

	return readsharegroupstateresponseresultpartition, nil
}

func (res *ReadShareGroupStateResponse) stateBatchesEncoder(w io.Writer, value ReadShareGroupStateResponseResultPartitionStateBatche) error {
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

func (res *ReadShareGroupStateResponse) stateBatchesDecoder(r io.Reader) (ReadShareGroupStateResponseResultPartitionStateBatche, error) {
	readsharegroupstateresponseresultpartitionstatebatche := ReadShareGroupStateResponseResultPartitionStateBatche{}

	// FirstOffset (versions: 0+)
	firstoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return readsharegroupstateresponseresultpartitionstatebatche, err
	}
	readsharegroupstateresponseresultpartitionstatebatche.FirstOffset = firstoffset

	// LastOffset (versions: 0+)
	lastoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return readsharegroupstateresponseresultpartitionstatebatche, err
	}
	readsharegroupstateresponseresultpartitionstatebatche.LastOffset = lastoffset

	// DeliveryState (versions: 0+)
	deliverystate, err := protocol.ReadInt8(r)
	if err != nil {
		return readsharegroupstateresponseresultpartitionstatebatche, err
	}
	readsharegroupstateresponseresultpartitionstatebatche.DeliveryState = deliverystate

	// DeliveryCount (versions: 0+)
	deliverycount, err := protocol.ReadInt16(r)
	if err != nil {
		return readsharegroupstateresponseresultpartitionstatebatche, err
	}
	readsharegroupstateresponseresultpartitionstatebatche.DeliveryCount = deliverycount

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return readsharegroupstateresponseresultpartitionstatebatche, err
		}
		readsharegroupstateresponseresultpartitionstatebatche.rawTaggedFields = &rawTaggedFields
	}

	return readsharegroupstateresponseresultpartitionstatebatche, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ReadShareGroupStateResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ReadShareGroupStateResponse:\n")

	if res.Results != nil {
		fmt.Fprintf(w, "        Results:\n")
		for _, results := range *res.Results {
			fmt.Fprintf(w, "%s", results.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Results: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ReadShareGroupStateResponseResult) PrettyPrint() string {
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
func (value *ReadShareGroupStateResponseResultPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Partition: %v\n", value.Partition)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                ErrorMessage: nil\n")
	}

	fmt.Fprintf(w, "                StateEpoch: %v\n", value.StateEpoch)
	fmt.Fprintf(w, "                StartOffset: %v\n", value.StartOffset)

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
func (value *ReadShareGroupStateResponseResultPartitionStateBatche) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    FirstOffset: %v\n", value.FirstOffset)
	fmt.Fprintf(w, "                    LastOffset: %v\n", value.LastOffset)
	fmt.Fprintf(w, "                    DeliveryState: %v\n", value.DeliveryState)
	fmt.Fprintf(w, "                    DeliveryCount: %v\n", value.DeliveryCount)

	return w.String()
}
