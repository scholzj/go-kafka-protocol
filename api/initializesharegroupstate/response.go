package initializesharegroupstate

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type InitializeShareGroupStateResponse struct {
	ApiVersion      int16
	Results         *[]InitializeShareGroupStateResponseResult // The initialization results. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type InitializeShareGroupStateResponseResult struct {
	TopicId         uuid.UUID                                           // The topic identifier. (versions: 0+)
	Partitions      *[]InitializeShareGroupStateResponseResultPartition // The results for the partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type InitializeShareGroupStateResponseResultPartition struct {
	Partition       int32   // The partition index. (versions: 0+)
	ErrorCode       int16   // The error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage    *string // The error message, or null if there was no error. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *InitializeShareGroupStateResponse) Write(w io.Writer) error {
	// Results (versions: 0+)
	if res.Results == nil {
		return fmt.Errorf("InitializeShareGroupStateResponse.Results must not be nil in version %d", res.ApiVersion)
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
func (res *InitializeShareGroupStateResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("InitializeShareGroupStateResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// Results (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		results, err := protocol.ReadNullableCompactArray(r, res.resultsDecoder)
		if err != nil {
			return err
		}
		res.Results = results
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

func (res *InitializeShareGroupStateResponse) resultsEncoder(w io.Writer, value InitializeShareGroupStateResponseResult) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("InitializeShareGroupStateResponseResult.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *InitializeShareGroupStateResponse) resultsDecoder(r io.Reader) (InitializeShareGroupStateResponseResult, error) {
	initializesharegroupstateresponseresult := InitializeShareGroupStateResponseResult{}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return initializesharegroupstateresponseresult, err
	}
	initializesharegroupstateresponseresult.TopicId = topicid

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return initializesharegroupstateresponseresult, err
		}
		initializesharegroupstateresponseresult.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return initializesharegroupstateresponseresult, err
		}
		initializesharegroupstateresponseresult.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return initializesharegroupstateresponseresult, err
		}
		initializesharegroupstateresponseresult.rawTaggedFields = &rawTaggedFields
	}

	return initializesharegroupstateresponseresult, nil
}

func (res *InitializeShareGroupStateResponse) partitionsEncoder(w io.Writer, value InitializeShareGroupStateResponseResultPartition) error {
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

func (res *InitializeShareGroupStateResponse) partitionsDecoder(r io.Reader) (InitializeShareGroupStateResponseResultPartition, error) {
	initializesharegroupstateresponseresultpartition := InitializeShareGroupStateResponseResultPartition{}

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return initializesharegroupstateresponseresultpartition, err
	}
	initializesharegroupstateresponseresultpartition.Partition = partition

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return initializesharegroupstateresponseresultpartition, err
	}
	initializesharegroupstateresponseresultpartition.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return initializesharegroupstateresponseresultpartition, err
		}
		initializesharegroupstateresponseresultpartition.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return initializesharegroupstateresponseresultpartition, err
		}
		initializesharegroupstateresponseresultpartition.ErrorMessage = errormessage
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return initializesharegroupstateresponseresultpartition, err
		}
		initializesharegroupstateresponseresultpartition.rawTaggedFields = &rawTaggedFields
	}

	return initializesharegroupstateresponseresultpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *InitializeShareGroupStateResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- InitializeShareGroupStateResponse:\n")

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
func (value *InitializeShareGroupStateResponseResult) PrettyPrint() string {
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
func (value *InitializeShareGroupStateResponseResultPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Partition: %v\n", value.Partition)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                ErrorMessage: nil\n")
	}

	return w.String()
}
