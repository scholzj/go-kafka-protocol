package altersharegroupoffsets

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AlterShareGroupOffsetsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                     // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                                     // The top-level error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage    *string                                   // The top-level error message, or null if there was no error. (versions: 0+, nullable: 0+)
	Responses       *[]AlterShareGroupOffsetsResponseResponse // The results for each topic. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterShareGroupOffsetsResponseResponse struct {
	TopicName       *string                                            // The topic name. (versions: 0+)
	TopicId         uuid.UUID                                          // The unique topic ID. (versions: 0+)
	Partitions      *[]AlterShareGroupOffsetsResponseResponsePartition // (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterShareGroupOffsetsResponseResponsePartition struct {
	PartitionIndex  int32   // The partition index. (versions: 0+)
	ErrorCode       int16   // The error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage    *string // The error message, or null if there was no error. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *AlterShareGroupOffsetsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
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
		return fmt.Errorf("AlterShareGroupOffsetsResponse.Responses must not be nil in version %d", res.ApiVersion)
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
func (res *AlterShareGroupOffsetsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("AlterShareGroupOffsetsResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

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

func (res *AlterShareGroupOffsetsResponse) responsesEncoder(w io.Writer, value AlterShareGroupOffsetsResponseResponse) error {
	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("AlterShareGroupOffsetsResponseResponse.TopicName must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.TopicName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.TopicName); err != nil {
			return err
		}
	}

	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("AlterShareGroupOffsetsResponseResponse.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *AlterShareGroupOffsetsResponse) responsesDecoder(r io.Reader) (AlterShareGroupOffsetsResponseResponse, error) {
	altersharegroupoffsetsresponseresponse := AlterShareGroupOffsetsResponseResponse{}

	// TopicName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return altersharegroupoffsetsresponseresponse, err
		}
		altersharegroupoffsetsresponseresponse.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return altersharegroupoffsetsresponseresponse, err
		}
		altersharegroupoffsetsresponseresponse.TopicName = &topicname
	}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return altersharegroupoffsetsresponseresponse, err
	}
	altersharegroupoffsetsresponseresponse.TopicId = topicid

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return altersharegroupoffsetsresponseresponse, err
		}
		altersharegroupoffsetsresponseresponse.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return altersharegroupoffsetsresponseresponse, err
		}
		altersharegroupoffsetsresponseresponse.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return altersharegroupoffsetsresponseresponse, err
		}
		altersharegroupoffsetsresponseresponse.rawTaggedFields = &rawTaggedFields
	}

	return altersharegroupoffsetsresponseresponse, nil
}

func (res *AlterShareGroupOffsetsResponse) partitionsEncoder(w io.Writer, value AlterShareGroupOffsetsResponseResponsePartition) error {
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

func (res *AlterShareGroupOffsetsResponse) partitionsDecoder(r io.Reader) (AlterShareGroupOffsetsResponseResponsePartition, error) {
	altersharegroupoffsetsresponseresponsepartition := AlterShareGroupOffsetsResponseResponsePartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return altersharegroupoffsetsresponseresponsepartition, err
	}
	altersharegroupoffsetsresponseresponsepartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return altersharegroupoffsetsresponseresponsepartition, err
	}
	altersharegroupoffsetsresponseresponsepartition.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return altersharegroupoffsetsresponseresponsepartition, err
		}
		altersharegroupoffsetsresponseresponsepartition.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return altersharegroupoffsetsresponseresponsepartition, err
		}
		altersharegroupoffsetsresponseresponsepartition.ErrorMessage = errormessage
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return altersharegroupoffsetsresponseresponsepartition, err
		}
		altersharegroupoffsetsresponseresponsepartition.rawTaggedFields = &rawTaggedFields
	}

	return altersharegroupoffsetsresponseresponsepartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *AlterShareGroupOffsetsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- AlterShareGroupOffsetsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
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
func (value *AlterShareGroupOffsetsResponseResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TopicName != nil {
		fmt.Fprintf(w, "            TopicName: %v\n", *value.TopicName)
	} else {
		fmt.Fprintf(w, "            TopicName: nil\n")
	}

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
func (value *AlterShareGroupOffsetsResponseResponsePartition) PrettyPrint() string {
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
