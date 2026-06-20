package electleaders

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ElectLeadersResponse struct {
	ApiVersion             int16
	ThrottleTimeMs         int32                                        // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode              int16                                        // The top level response error code. (versions: 1+)
	ReplicaElectionResults *[]ElectLeadersResponseReplicaElectionResult // The election results, or an empty array if the requester did not have permission and the request asks for all partitions. (versions: 0+)
	rawTaggedFields        *[]protocol.TaggedField
}

type ElectLeadersResponseReplicaElectionResult struct {
	Topic           *string                                                     // The topic name. (versions: 0+)
	PartitionResult *[]ElectLeadersResponseReplicaElectionResultPartitionResult // The results for each partition. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ElectLeadersResponseReplicaElectionResultPartitionResult struct {
	PartitionId     int32   // The partition id. (versions: 0+)
	ErrorCode       int16   // The result error, or zero if there was no error. (versions: 0+)
	ErrorMessage    *string // The result message, or null if there was no error. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (res *ElectLeadersResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
			return err
		}
	}

	// ReplicaElectionResults (versions: 0+)
	if res.ReplicaElectionResults == nil {
		return fmt.Errorf("ElectLeadersResponse.ReplicaElectionResults must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.replicaElectionResultsEncoder, res.ReplicaElectionResults); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.replicaElectionResultsEncoder, *res.ReplicaElectionResults); err != nil {
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
func (res *ElectLeadersResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("ElectLeadersResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// ErrorCode (versions: 1+)
	if res.ApiVersion >= 1 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		res.ErrorCode = errorcode
	}

	// ReplicaElectionResults (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		replicaelectionresults, err := protocol.ReadNullableCompactArray(r, res.replicaElectionResultsDecoder)
		if err != nil {
			return err
		}
		res.ReplicaElectionResults = replicaelectionresults
	} else {
		replicaelectionresults, err := protocol.ReadArray(r, res.replicaElectionResultsDecoder)
		if err != nil {
			return err
		}
		res.ReplicaElectionResults = &replicaelectionresults
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

func (res *ElectLeadersResponse) replicaElectionResultsEncoder(w io.Writer, value ElectLeadersResponseReplicaElectionResult) error {
	// Topic (versions: 0+)
	if value.Topic == nil {
		return fmt.Errorf("ElectLeadersResponseReplicaElectionResult.Topic must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Topic); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Topic); err != nil {
			return err
		}
	}

	// PartitionResult (versions: 0+)
	if value.PartitionResult == nil {
		return fmt.Errorf("ElectLeadersResponseReplicaElectionResult.PartitionResult must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.partitionResultEncoder, value.PartitionResult); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.partitionResultEncoder, *value.PartitionResult); err != nil {
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

func (res *ElectLeadersResponse) replicaElectionResultsDecoder(r io.Reader) (ElectLeadersResponseReplicaElectionResult, error) {
	electleadersresponsereplicaelectionresult := ElectLeadersResponseReplicaElectionResult{}

	// Topic (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topic, err := protocol.ReadCompactString(r)
		if err != nil {
			return electleadersresponsereplicaelectionresult, err
		}
		electleadersresponsereplicaelectionresult.Topic = &topic
	} else {
		topic, err := protocol.ReadString(r)
		if err != nil {
			return electleadersresponsereplicaelectionresult, err
		}
		electleadersresponsereplicaelectionresult.Topic = &topic
	}

	// PartitionResult (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitionresult, err := protocol.ReadNullableCompactArray(r, res.partitionResultDecoder)
		if err != nil {
			return electleadersresponsereplicaelectionresult, err
		}
		electleadersresponsereplicaelectionresult.PartitionResult = partitionresult
	} else {
		partitionresult, err := protocol.ReadArray(r, res.partitionResultDecoder)
		if err != nil {
			return electleadersresponsereplicaelectionresult, err
		}
		electleadersresponsereplicaelectionresult.PartitionResult = &partitionresult
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return electleadersresponsereplicaelectionresult, err
		}
		electleadersresponsereplicaelectionresult.rawTaggedFields = &rawTaggedFields
	}

	return electleadersresponsereplicaelectionresult, nil
}

func (res *ElectLeadersResponse) partitionResultEncoder(w io.Writer, value ElectLeadersResponseReplicaElectionResultPartitionResult) error {
	// PartitionId (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionId); err != nil {
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

func (res *ElectLeadersResponse) partitionResultDecoder(r io.Reader) (ElectLeadersResponseReplicaElectionResultPartitionResult, error) {
	electleadersresponsereplicaelectionresultpartitionresult := ElectLeadersResponseReplicaElectionResultPartitionResult{}

	// PartitionId (versions: 0+)
	partitionid, err := protocol.ReadInt32(r)
	if err != nil {
		return electleadersresponsereplicaelectionresultpartitionresult, err
	}
	electleadersresponsereplicaelectionresultpartitionresult.PartitionId = partitionid

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return electleadersresponsereplicaelectionresultpartitionresult, err
	}
	electleadersresponsereplicaelectionresultpartitionresult.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return electleadersresponsereplicaelectionresultpartitionresult, err
		}
		electleadersresponsereplicaelectionresultpartitionresult.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return electleadersresponsereplicaelectionresultpartitionresult, err
		}
		electleadersresponsereplicaelectionresultpartitionresult.ErrorMessage = errormessage
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return electleadersresponsereplicaelectionresultpartitionresult, err
		}
		electleadersresponsereplicaelectionresultpartitionresult.rawTaggedFields = &rawTaggedFields
	}

	return electleadersresponsereplicaelectionresultpartitionresult, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ElectLeadersResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ElectLeadersResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.ReplicaElectionResults != nil {
		fmt.Fprintf(w, "        ReplicaElectionResults:\n")
		for _, replicaelectionresults := range *res.ReplicaElectionResults {
			fmt.Fprintf(w, "%s", replicaelectionresults.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        ReplicaElectionResults: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ElectLeadersResponseReplicaElectionResult) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Topic != nil {
		fmt.Fprintf(w, "            Topic: %v\n", *value.Topic)
	} else {
		fmt.Fprintf(w, "            Topic: nil\n")
	}

	if value.PartitionResult != nil {
		fmt.Fprintf(w, "            PartitionResult:\n")
		for _, partitionresult := range *value.PartitionResult {
			fmt.Fprintf(w, "%s", partitionresult.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            PartitionResult: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ElectLeadersResponseReplicaElectionResultPartitionResult) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionId: %v\n", value.PartitionId)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                ErrorMessage: nil\n")
	}

	return w.String()
}
