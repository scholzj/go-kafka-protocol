package produce

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ProduceResponse struct {
	ApiVersion      int16
	Responses       *[]ProduceResponseResponse     // Each produce response.
	ThrottleTimeMs  int32                          // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	NodeEndpoints   *[]ProduceResponseNodeEndpoint // tag 0: Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
	rawTaggedFields *[]protocol.TaggedField
}

type ProduceResponseResponse struct {
	Name               *string                                     // The topic name.
	TopicId            uuid.UUID                                   // The unique topic ID
	PartitionResponses *[]ProduceResponseResponsePartitionResponse // Each partition that we produced to within the topic.
	rawTaggedFields    *[]protocol.TaggedField
}

type ProduceResponseResponsePartitionResponse struct {
	Index           int32                                                  // The partition index.
	ErrorCode       int16                                                  // The error code, or 0 if there was no error.
	BaseOffset      int64                                                  // The base offset.
	LogAppendTimeMs int64                                                  // The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
	LogStartOffset  int64                                                  // The log start offset.
	RecordErrors    *[]ProduceResponseResponsePartitionResponseRecordError // The batch indices of records that caused the batch to be dropped.
	ErrorMessage    *string                                                // The global error message summarizing the common root cause of the records that caused the batch to be dropped.
	CurrentLeader   *ProduceResponseResponsePartitionResponseCurrentLeader // tag 0: The leader broker that the producer should use for future requests.
	rawTaggedFields *[]protocol.TaggedField
}

type ProduceResponseResponsePartitionResponseRecordError struct {
	BatchIndex             int32   // The batch index of the record that caused the batch to be dropped.
	BatchIndexErrorMessage *string // The error message of the record that caused the batch to be dropped.
	rawTaggedFields        *[]protocol.TaggedField
}

type ProduceResponseResponsePartitionResponseCurrentLeader struct {
	LeaderId        int32 // The ID of the current leader or -1 if the leader is unknown.
	LeaderEpoch     int32 // The latest known leader epoch.
	rawTaggedFields *[]protocol.TaggedField
}

type ProduceResponseNodeEndpoint struct {
	NodeId          int32   // The ID of the associated node.
	Host            *string // The node's hostname.
	Port            int32   // The node's port.
	Rack            *string // The rack of the node, or null if it has not been assigned to a rack.
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 9
}

func (res *ProduceResponse) Write(w io.Writer) error {
	// Responses (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.responsesEncoder, res.Responses); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.responsesEncoder, *res.Responses); err != nil {
			return err
		}
	}

	// ThrottleTimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		taggedFields, err := res.taggedFieldsEncoder()
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (res *ProduceResponse) Read(response protocol.Response) error {
	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	var err error

	// Responses (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
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

	// ThrottleTimeMs (versions: 1+)
	if response.ApiVersion >= 1 {
		throttletimems, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttletimems
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		err = protocol.ReadTaggedFields(r, res.taggedFieldsDecoder)
		if err != nil {
			return err
		}
	}

	return nil
}

func (res *ProduceResponse) responsesEncoder(w io.Writer, value ProduceResponseResponse) error {
	// Name (versions: 0-12)
	if res.ApiVersion <= 12 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.Name); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.Name); err != nil {
				return err
			}
		}
	}

	// TopicId (versions: 13+)
	if res.ApiVersion >= 13 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// PartitionResponses (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.partitionResponsesEncoder, value.PartitionResponses); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.partitionResponsesEncoder, *value.PartitionResponses); err != nil {
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

func (res *ProduceResponse) responsesDecoder(r io.Reader) (ProduceResponseResponse, error) {
	produceresponseresponse := ProduceResponseResponse{}
	var err error

	// Name (versions: 0-12)
	if res.ApiVersion <= 12 {
		if isResponseFlexible(res.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return produceresponseresponse, err
			}
			produceresponseresponse.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return produceresponseresponse, err
			}
			produceresponseresponse.Name = &name
		}
	}

	// TopicId (versions: 13+)
	if res.ApiVersion >= 13 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return produceresponseresponse, err
		}
		produceresponseresponse.TopicId = topicid
	}

	// PartitionResponses (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitionresponses, err := protocol.ReadNullableCompactArray(r, res.partitionResponsesDecoder)
		if err != nil {
			return produceresponseresponse, err
		}
		produceresponseresponse.PartitionResponses = partitionresponses
	} else {
		partitionresponses, err := protocol.ReadArray(r, res.partitionResponsesDecoder)
		if err != nil {
			return produceresponseresponse, err
		}
		produceresponseresponse.PartitionResponses = &partitionresponses
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return produceresponseresponse, err
		}
		produceresponseresponse.rawTaggedFields = &rawTaggedFields
	}

	return produceresponseresponse, nil
}

func (res *ProduceResponse) partitionResponsesEncoder(w io.Writer, value ProduceResponseResponsePartitionResponse) error {
	// Index (versions: 0+)
	if err := protocol.WriteInt32(w, value.Index); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// BaseOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.BaseOffset); err != nil {
		return err
	}

	// LogAppendTimeMs (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteInt64(w, value.LogAppendTimeMs); err != nil {
			return err
		}
	}

	// LogStartOffset (versions: 5+)
	if res.ApiVersion >= 5 {
		if err := protocol.WriteInt64(w, value.LogStartOffset); err != nil {
			return err
		}
	}

	// RecordErrors (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.recordErrorsEncoder, value.RecordErrors); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, res.recordErrorsEncoder, *value.RecordErrors); err != nil {
				return err
			}
		}
	}

	// ErrorMessage (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.ErrorMessage); err != nil {
				return err
			}
		}
	}

	// CurrentLeader (versions: 10+)
	if !isResponseFlexible(res.ApiVersion) {
		if res.ApiVersion >= 10 {
			if err := res.currentLeaderEncoder(w, *value.CurrentLeader); err != nil {
				return err
			}
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		taggedFields, err := res.taggedFieldsEncoderPartitionResponses(value)
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *ProduceResponse) partitionResponsesDecoder(r io.Reader) (ProduceResponseResponsePartitionResponse, error) {
	produceresponseresponsepartitionresponse := ProduceResponseResponsePartitionResponse{}
	var err error

	// Index (versions: 0+)
	index, err := protocol.ReadInt32(r)
	if err != nil {
		return produceresponseresponsepartitionresponse, err
	}
	produceresponseresponsepartitionresponse.Index = index

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return produceresponseresponsepartitionresponse, err
	}
	produceresponseresponsepartitionresponse.ErrorCode = errorcode

	// BaseOffset (versions: 0+)
	baseoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return produceresponseresponsepartitionresponse, err
	}
	produceresponseresponsepartitionresponse.BaseOffset = baseoffset

	// LogAppendTimeMs (versions: 2+)
	if res.ApiVersion >= 2 {
		logappendtimems, err := protocol.ReadInt64(r)
		if err != nil {
			return produceresponseresponsepartitionresponse, err
		}
		produceresponseresponsepartitionresponse.LogAppendTimeMs = logappendtimems
	}

	// LogStartOffset (versions: 5+)
	if res.ApiVersion >= 5 {
		logstartoffset, err := protocol.ReadInt64(r)
		if err != nil {
			return produceresponseresponsepartitionresponse, err
		}
		produceresponseresponsepartitionresponse.LogStartOffset = logstartoffset
	}

	// RecordErrors (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			recorderrors, err := protocol.ReadNullableCompactArray(r, res.recordErrorsDecoder)
			if err != nil {
				return produceresponseresponsepartitionresponse, err
			}
			produceresponseresponsepartitionresponse.RecordErrors = recorderrors
		} else {
			recorderrors, err := protocol.ReadArray(r, res.recordErrorsDecoder)
			if err != nil {
				return produceresponseresponsepartitionresponse, err
			}
			produceresponseresponsepartitionresponse.RecordErrors = &recorderrors
		}
	}

	// ErrorMessage (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			errormessage, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return produceresponseresponsepartitionresponse, err
			}
			produceresponseresponsepartitionresponse.ErrorMessage = errormessage
		} else {
			errormessage, err := protocol.ReadNullableString(r)
			if err != nil {
				return produceresponseresponsepartitionresponse, err
			}
			produceresponseresponsepartitionresponse.ErrorMessage = errormessage
		}
	}

	// CurrentLeader (versions: 10+)
	if !isResponseFlexible(res.ApiVersion) {
		if res.ApiVersion >= 10 {
			currentleader, err := res.currentLeaderDecoder(r)
			if err != nil {
				return produceresponseresponsepartitionresponse, err
			}
			produceresponseresponsepartitionresponse.CurrentLeader = &currentleader
			if err != nil {
				return produceresponseresponsepartitionresponse, err
			}
			produceresponseresponsepartitionresponse.CurrentLeader = &currentleader
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		// Decode tagged fields
		err = protocol.ReadTaggedFields(r, func(r io.Reader, tag uint64, tagLength uint64) error {
			return res.taggedFieldsDecoderPartitionResponses(r, tag, tagLength, &produceresponseresponsepartitionresponse)
		})
		if err != nil {
			return produceresponseresponsepartitionresponse, err
		}
	}

	return produceresponseresponsepartitionresponse, nil
}

func (res *ProduceResponse) taggedFieldsEncoderPartitionResponses(value ProduceResponseResponsePartitionResponse) ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if value.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*value.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 2+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	if value.CurrentLeader != nil {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := res.currentLeaderEncoder(buf, *value.CurrentLeader); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})
	}

	// We append any raw tagged fields to the end of the array
	if value.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *value.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (res *ProduceResponse) taggedFieldsDecoderPartitionResponses(r io.Reader, tag uint64, tagLength uint64, value *ProduceResponseResponsePartitionResponse) error {
	rawTaggedFields := make([]protocol.TaggedField, 0)

	switch tag {
	case 0:
		// CurrentLeader
		currentleaderVal, err := res.currentLeaderDecoder(r)
		if err != nil {
			return err
		}
		value.CurrentLeader = &currentleaderVal
	default:
		// Decode as raw tags
		taggedField, err := protocol.ReadRawTaggedField(r)
		if err != nil {
			return err
		}
		rawTaggedFields = append(rawTaggedFields, taggedField)
	}

	// Set the raw tagged fields
	value.rawTaggedFields = &rawTaggedFields

	return nil
}

func (res *ProduceResponse) recordErrorsEncoder(w io.Writer, value ProduceResponseResponsePartitionResponseRecordError) error {
	// BatchIndex (versions: 8+)
	if res.ApiVersion >= 8 {
		if err := protocol.WriteInt32(w, value.BatchIndex); err != nil {
			return err
		}
	}

	// BatchIndexErrorMessage (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.BatchIndexErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.BatchIndexErrorMessage); err != nil {
				return err
			}
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

func (res *ProduceResponse) recordErrorsDecoder(r io.Reader) (ProduceResponseResponsePartitionResponseRecordError, error) {
	produceresponseresponsepartitionresponserecorderror := ProduceResponseResponsePartitionResponseRecordError{}
	var err error

	// BatchIndex (versions: 8+)
	if res.ApiVersion >= 8 {
		batchindex, err := protocol.ReadInt32(r)
		if err != nil {
			return produceresponseresponsepartitionresponserecorderror, err
		}
		produceresponseresponsepartitionresponserecorderror.BatchIndex = batchindex
	}

	// BatchIndexErrorMessage (versions: 8+)
	if res.ApiVersion >= 8 {
		if isResponseFlexible(res.ApiVersion) {
			batchindexerrormessage, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return produceresponseresponsepartitionresponserecorderror, err
			}
			produceresponseresponsepartitionresponserecorderror.BatchIndexErrorMessage = batchindexerrormessage
		} else {
			batchindexerrormessage, err := protocol.ReadNullableString(r)
			if err != nil {
				return produceresponseresponsepartitionresponserecorderror, err
			}
			produceresponseresponsepartitionresponserecorderror.BatchIndexErrorMessage = batchindexerrormessage
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return produceresponseresponsepartitionresponserecorderror, err
		}
		produceresponseresponsepartitionresponserecorderror.rawTaggedFields = &rawTaggedFields
	}

	return produceresponseresponsepartitionresponserecorderror, nil
}

func (res *ProduceResponse) currentLeaderEncoder(w io.Writer, value ProduceResponseResponsePartitionResponseCurrentLeader) error {
	// LeaderId (versions: 10+)
	if res.ApiVersion >= 10 {
		if err := protocol.WriteInt32(w, value.LeaderId); err != nil {
			return err
		}
	}

	// LeaderEpoch (versions: 10+)
	if res.ApiVersion >= 10 {
		if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
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

func (res *ProduceResponse) currentLeaderDecoder(r io.Reader) (ProduceResponseResponsePartitionResponseCurrentLeader, error) {
	produceresponseresponsepartitionresponsecurrentleader := ProduceResponseResponsePartitionResponseCurrentLeader{}
	var err error

	// LeaderId (versions: 10+)
	if res.ApiVersion >= 10 {
		leaderid, err := protocol.ReadInt32(r)
		if err != nil {
			return produceresponseresponsepartitionresponsecurrentleader, err
		}
		produceresponseresponsepartitionresponsecurrentleader.LeaderId = leaderid
	}

	// LeaderEpoch (versions: 10+)
	if res.ApiVersion >= 10 {
		leaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return produceresponseresponsepartitionresponsecurrentleader, err
		}
		produceresponseresponsepartitionresponsecurrentleader.LeaderEpoch = leaderepoch
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return produceresponseresponsepartitionresponsecurrentleader, err
		}
		produceresponseresponsepartitionresponsecurrentleader.rawTaggedFields = &rawTaggedFields
	}

	return produceresponseresponsepartitionresponsecurrentleader, nil
}

func (res *ProduceResponse) nodeEndpointsEncoder(w io.Writer, value ProduceResponseNodeEndpoint) error {
	// NodeId (versions: 10+)
	if res.ApiVersion >= 10 {
		if err := protocol.WriteInt32(w, value.NodeId); err != nil {
			return err
		}
	}

	// Host (versions: 10+)
	if res.ApiVersion >= 10 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.Host); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.Host); err != nil {
				return err
			}
		}
	}

	// Port (versions: 10+)
	if res.ApiVersion >= 10 {
		if err := protocol.WriteInt32(w, value.Port); err != nil {
			return err
		}
	}

	// Rack (versions: 10+)
	if res.ApiVersion >= 10 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.Rack); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.Rack); err != nil {
				return err
			}
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

func (res *ProduceResponse) nodeEndpointsDecoder(r io.Reader) (ProduceResponseNodeEndpoint, error) {
	produceresponsenodeendpoint := ProduceResponseNodeEndpoint{}
	var err error

	// NodeId (versions: 10+)
	if res.ApiVersion >= 10 {
		nodeid, err := protocol.ReadInt32(r)
		if err != nil {
			return produceresponsenodeendpoint, err
		}
		produceresponsenodeendpoint.NodeId = nodeid
	}

	// Host (versions: 10+)
	if res.ApiVersion >= 10 {
		if isResponseFlexible(res.ApiVersion) {
			host, err := protocol.ReadCompactString(r)
			if err != nil {
				return produceresponsenodeendpoint, err
			}
			produceresponsenodeendpoint.Host = &host
		} else {
			host, err := protocol.ReadString(r)
			if err != nil {
				return produceresponsenodeendpoint, err
			}
			produceresponsenodeendpoint.Host = &host
		}
	}

	// Port (versions: 10+)
	if res.ApiVersion >= 10 {
		port, err := protocol.ReadInt32(r)
		if err != nil {
			return produceresponsenodeendpoint, err
		}
		produceresponsenodeendpoint.Port = port
	}

	// Rack (versions: 10+)
	if res.ApiVersion >= 10 {
		if isResponseFlexible(res.ApiVersion) {
			rack, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return produceresponsenodeendpoint, err
			}
			produceresponsenodeendpoint.Rack = rack
		} else {
			rack, err := protocol.ReadNullableString(r)
			if err != nil {
				return produceresponsenodeendpoint, err
			}
			produceresponsenodeendpoint.Rack = rack
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return produceresponsenodeendpoint, err
		}
		produceresponsenodeendpoint.rawTaggedFields = &rawTaggedFields
	}

	return produceresponsenodeendpoint, nil
}

func (res *ProduceResponse) taggedFieldsEncoder() ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if res.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*res.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 2+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	if res.NodeEndpoints != nil {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := protocol.WriteNullableCompactArray(buf, res.nodeEndpointsEncoder, res.NodeEndpoints); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})
	}

	// We append any raw tagged fields to the end of the array
	if res.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *res.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (res *ProduceResponse) taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {
	rawTaggedFields := make([]protocol.TaggedField, 0)

	switch tag {
	case 0:
		// NodeEndpoints
		nodeendpoints, err := protocol.ReadNullableCompactArray(r, res.nodeEndpointsDecoder)
		if err != nil {
			return err
		}
		res.NodeEndpoints = nodeendpoints
	default:
		// Decode as raw tags
		taggedField, err := protocol.ReadRawTaggedField(r)
		if err != nil {
			return err
		}
		rawTaggedFields = append(rawTaggedFields, taggedField)
	}

	// Set the raw tagged fields
	res.rawTaggedFields = &rawTaggedFields

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ProduceResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ProduceResponse:\n")
	if res.Responses != nil {
		fmt.Fprintf(w, "        Responses:\n")
		for _, responses := range *res.Responses {
			fmt.Fprintf(w, "%s", responses.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Responses: nil\n")
	}
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	if res.NodeEndpoints != nil {
		fmt.Fprintf(w, "        NodeEndpoints:\n")
		for _, nodeendpoints := range *res.NodeEndpoints {
			fmt.Fprintf(w, "%s", nodeendpoints.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        NodeEndpoints: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ProduceResponseResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}
	fmt.Fprintf(w, "            TopicId: %v\n", value.TopicId)
	if value.PartitionResponses != nil {
		fmt.Fprintf(w, "            PartitionResponses:\n")
		for _, partitionresponses := range *value.PartitionResponses {
			fmt.Fprintf(w, "%s", partitionresponses.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            PartitionResponses: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ProduceResponseResponsePartitionResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Index: %v\n", value.Index)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)
	fmt.Fprintf(w, "                BaseOffset: %v\n", value.BaseOffset)
	fmt.Fprintf(w, "                LogAppendTimeMs: %v\n", value.LogAppendTimeMs)
	fmt.Fprintf(w, "                LogStartOffset: %v\n", value.LogStartOffset)
	if value.RecordErrors != nil {
		fmt.Fprintf(w, "                RecordErrors:\n")
		for _, recorderrors := range *value.RecordErrors {
			fmt.Fprintf(w, "%s", recorderrors.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                RecordErrors: nil\n")
	}
	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "                ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "                ErrorMessage: nil\n")
	}
	fmt.Fprintf(w, "                CurrentLeader:\n")
	if value.CurrentLeader != nil {
		fmt.Fprintf(w, "%s", value.CurrentLeader.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ProduceResponseResponsePartitionResponseRecordError) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    BatchIndex: %v\n", value.BatchIndex)
	if value.BatchIndexErrorMessage != nil {
		fmt.Fprintf(w, "                    BatchIndexErrorMessage: %v\n", *value.BatchIndexErrorMessage)
	} else {
		fmt.Fprintf(w, "                    BatchIndexErrorMessage: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ProduceResponseResponsePartitionResponseCurrentLeader) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                    LeaderEpoch: %v\n", value.LeaderEpoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ProduceResponseNodeEndpoint) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            NodeId: %v\n", value.NodeId)
	if value.Host != nil {
		fmt.Fprintf(w, "            Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "            Host: nil\n")
	}
	fmt.Fprintf(w, "            Port: %v\n", value.Port)
	if value.Rack != nil {
		fmt.Fprintf(w, "            Rack: %v\n", *value.Rack)
	} else {
		fmt.Fprintf(w, "            Rack: nil\n")
	}

	return w.String()
}
