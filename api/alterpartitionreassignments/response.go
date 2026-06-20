package alterpartitionreassignments

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AlterPartitionReassignmentsResponse struct {
	ApiVersion                   int16
	ThrottleTimeMs               int32                                          // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	AllowReplicationFactorChange bool                                           // The option indicating whether changing the replication factor of any given partition as part of the request was allowed. (versions: 1+)
	ErrorCode                    int16                                          // The top-level error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage                 *string                                        // The top-level error message, or null if there was no error. (versions: 0+, nullable: 0+)
	Responses                    *[]AlterPartitionReassignmentsResponseResponse // The responses to topics to reassign. (versions: 0+)
	rawTaggedFields              *[]protocol.TaggedField
}

type AlterPartitionReassignmentsResponseResponse struct {
	Name            *string                                                 // The topic name. (versions: 0+)
	Partitions      *[]AlterPartitionReassignmentsResponseResponsePartition // The responses to partitions to reassign. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterPartitionReassignmentsResponseResponsePartition struct {
	PartitionIndex  int32   // The partition index. (versions: 0+)
	ErrorCode       int16   // The error code for this partition, or 0 if there was no error. (versions: 0+)
	ErrorMessage    *string // The error message for this partition, or null if there was no error. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *AlterPartitionReassignmentsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// AllowReplicationFactorChange (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteBool(w, res.AllowReplicationFactorChange); err != nil {
			return err
		}
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, res.ErrorMessage); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, res.ErrorMessage); err != nil {
			return err
		}
	}

	// Responses (versions: 0+)
	if res.Responses == nil {
		return fmt.Errorf("AlterPartitionReassignmentsResponse.Responses must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.responsesEncoder, res.Responses); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.responsesEncoder, *res.Responses); err != nil {
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
func (res *AlterPartitionReassignmentsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("AlterPartitionReassignmentsResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// AllowReplicationFactorChange (versions: 1+)
	if res.ApiVersion >= 1 {
		allowreplicationfactorchange, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		res.AllowReplicationFactorChange = allowreplicationfactorchange
	}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		res.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		res.ErrorMessage = errormessage
	}

	// Responses (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		responses, err := protocol.ReadNullableCompactArray(r, res.responsesDecoder)
		if err != nil {
			return err
		}
		res.Responses = responses
	} else {
		responses, err := protocol.ReadArray(r, res.responsesDecoder)
		if err != nil {
			return err
		}
		res.Responses = &responses
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

func (res *AlterPartitionReassignmentsResponse) responsesEncoder(w io.Writer, value AlterPartitionReassignmentsResponseResponse) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("AlterPartitionReassignmentsResponseResponse.Name must not be nil in version %d", res.ApiVersion)
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
		return fmt.Errorf("AlterPartitionReassignmentsResponseResponse.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *AlterPartitionReassignmentsResponse) responsesDecoder(r io.Reader) (AlterPartitionReassignmentsResponseResponse, error) {
	alterpartitionreassignmentsresponseresponse := AlterPartitionReassignmentsResponseResponse{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return alterpartitionreassignmentsresponseresponse, err
		}
		alterpartitionreassignmentsresponseresponse.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return alterpartitionreassignmentsresponseresponse, err
		}
		alterpartitionreassignmentsresponseresponse.Name = &name
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return alterpartitionreassignmentsresponseresponse, err
		}
		alterpartitionreassignmentsresponseresponse.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return alterpartitionreassignmentsresponseresponse, err
		}
		alterpartitionreassignmentsresponseresponse.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterpartitionreassignmentsresponseresponse, err
		}
		alterpartitionreassignmentsresponseresponse.rawTaggedFields = &rawTaggedFields
	}

	return alterpartitionreassignmentsresponseresponse, nil
}

func (res *AlterPartitionReassignmentsResponse) partitionsEncoder(w io.Writer, value AlterPartitionReassignmentsResponseResponsePartition) error {
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

func (res *AlterPartitionReassignmentsResponse) partitionsDecoder(r io.Reader) (AlterPartitionReassignmentsResponseResponsePartition, error) {
	alterpartitionreassignmentsresponseresponsepartition := AlterPartitionReassignmentsResponseResponsePartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return alterpartitionreassignmentsresponseresponsepartition, err
	}
	alterpartitionreassignmentsresponseresponsepartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return alterpartitionreassignmentsresponseresponsepartition, err
	}
	alterpartitionreassignmentsresponseresponsepartition.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return alterpartitionreassignmentsresponseresponsepartition, err
		}
		alterpartitionreassignmentsresponseresponsepartition.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return alterpartitionreassignmentsresponseresponsepartition, err
		}
		alterpartitionreassignmentsresponseresponsepartition.ErrorMessage = errormessage
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterpartitionreassignmentsresponseresponsepartition, err
		}
		alterpartitionreassignmentsresponseresponsepartition.rawTaggedFields = &rawTaggedFields
	}

	return alterpartitionreassignmentsresponseresponsepartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *AlterPartitionReassignmentsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- AlterPartitionReassignmentsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        AllowReplicationFactorChange: %v\n", res.AllowReplicationFactorChange)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.ErrorMessage != nil {
		fmt.Fprintf(w, "        ErrorMessage: %v\n", *res.ErrorMessage)
	} else {
		fmt.Fprintf(w, "        ErrorMessage: nil\n")
	}

	if res.Responses != nil {
		fmt.Fprintf(w, "        Responses:\n")
		for _, responses := range *res.Responses {
			fmt.Fprintf(w, "%s", responses.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Responses: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterPartitionReassignmentsResponseResponse) PrettyPrint() string {
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
func (value *AlterPartitionReassignmentsResponseResponsePartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                ErrorMessage: nil\n")
	}

	return w.String()
}
