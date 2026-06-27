package readsharegroupstatesummary

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ReadShareGroupStateSummaryResponse struct {
	ApiVersion      int16
	Results         *[]ReadShareGroupStateSummaryResponseResult // The read results. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ReadShareGroupStateSummaryResponseResult struct {
	TopicId         uuid.UUID                                            // The topic identifier. (versions: 0+)
	Partitions      *[]ReadShareGroupStateSummaryResponseResultPartition // The results for the partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ReadShareGroupStateSummaryResponseResultPartition struct {
	Partition             int32   // The partition index. (versions: 0+)
	ErrorCode             int16   // The error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage          *string // The error message, or null if there was no error. (versions: 0+, nullable: 0+)
	StateEpoch            int32   // The state epoch of the share-partition. (versions: 0+)
	LeaderEpoch           int32   // The leader epoch of the share-partition. (versions: 0+)
	StartOffset           int64   // The share-partition start offset. (versions: 0+)
	DeliveryCompleteCount int32   // The number of offsets greater than or equal to share-partition start offset for which delivery has been completed. (versions: 1+)
	rawTaggedFields       *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *ReadShareGroupStateSummaryResponse) Write(w io.Writer) error {
	// Results (versions: 0+)
	if res.Results == nil {
		return fmt.Errorf("ReadShareGroupStateSummaryResponse.Results must not be nil in version %d", res.ApiVersion)
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
func (res *ReadShareGroupStateSummaryResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("ReadShareGroupStateSummaryResponse.Read: response or its body is nil")
	}

	*res = ReadShareGroupStateSummaryResponse{}

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

func (res *ReadShareGroupStateSummaryResponse) resultsEncoder(w io.Writer, value ReadShareGroupStateSummaryResponseResult) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("ReadShareGroupStateSummaryResponseResult.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *ReadShareGroupStateSummaryResponse) resultsDecoder(r io.Reader) (ReadShareGroupStateSummaryResponseResult, error) {
	readsharegroupstatesummaryresponseresult := ReadShareGroupStateSummaryResponseResult{}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return readsharegroupstatesummaryresponseresult, err
	}
	readsharegroupstatesummaryresponseresult.TopicId = topicid

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return readsharegroupstatesummaryresponseresult, err
		}
		readsharegroupstatesummaryresponseresult.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return readsharegroupstatesummaryresponseresult, err
		}
		readsharegroupstatesummaryresponseresult.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return readsharegroupstatesummaryresponseresult, err
		}
		readsharegroupstatesummaryresponseresult.rawTaggedFields = &rawTaggedFields
	}

	return readsharegroupstatesummaryresponseresult, nil
}

func (res *ReadShareGroupStateSummaryResponse) partitionsEncoder(w io.Writer, value ReadShareGroupStateSummaryResponseResultPartition) error {
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

	// LeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
		return err
	}

	// StartOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.StartOffset); err != nil {
		return err
	}

	// DeliveryCompleteCount (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, value.DeliveryCompleteCount); err != nil {
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

func (res *ReadShareGroupStateSummaryResponse) partitionsDecoder(r io.Reader) (ReadShareGroupStateSummaryResponseResultPartition, error) {
	readsharegroupstatesummaryresponseresultpartition := ReadShareGroupStateSummaryResponseResultPartition{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	readsharegroupstatesummaryresponseresultpartition.DeliveryCompleteCount = -1

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return readsharegroupstatesummaryresponseresultpartition, err
	}
	readsharegroupstatesummaryresponseresultpartition.Partition = partition

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return readsharegroupstatesummaryresponseresultpartition, err
	}
	readsharegroupstatesummaryresponseresultpartition.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return readsharegroupstatesummaryresponseresultpartition, err
		}
		readsharegroupstatesummaryresponseresultpartition.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return readsharegroupstatesummaryresponseresultpartition, err
		}
		readsharegroupstatesummaryresponseresultpartition.ErrorMessage = errormessage
	}

	// StateEpoch (versions: 0+)
	stateepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return readsharegroupstatesummaryresponseresultpartition, err
	}
	readsharegroupstatesummaryresponseresultpartition.StateEpoch = stateepoch

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return readsharegroupstatesummaryresponseresultpartition, err
	}
	readsharegroupstatesummaryresponseresultpartition.LeaderEpoch = leaderepoch

	// StartOffset (versions: 0+)
	startoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return readsharegroupstatesummaryresponseresultpartition, err
	}
	readsharegroupstatesummaryresponseresultpartition.StartOffset = startoffset

	// DeliveryCompleteCount (versions: 1+)
	if res.ApiVersion >= 1 {
		deliverycompletecount, err := protocol.ReadInt32(r)
		if err != nil {
			return readsharegroupstatesummaryresponseresultpartition, err
		}
		readsharegroupstatesummaryresponseresultpartition.DeliveryCompleteCount = deliverycompletecount
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return readsharegroupstatesummaryresponseresultpartition, err
		}
		readsharegroupstatesummaryresponseresultpartition.rawTaggedFields = &rawTaggedFields
	}

	return readsharegroupstatesummaryresponseresultpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ReadShareGroupStateSummaryResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ReadShareGroupStateSummaryResponse:\n")

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
func (value *ReadShareGroupStateSummaryResponseResult) PrettyPrint() string {
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
func (value *ReadShareGroupStateSummaryResponseResultPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Partition: %v\n", value.Partition)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                ErrorMessage: nil\n")
	}

	fmt.Fprintf(w, "                StateEpoch: %v\n", value.StateEpoch)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)
	fmt.Fprintf(w, "                StartOffset: %v\n", value.StartOffset)
	fmt.Fprintf(w, "                DeliveryCompleteCount: %v\n", value.DeliveryCompleteCount)

	return w.String()
}
