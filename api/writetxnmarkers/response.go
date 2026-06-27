package writetxnmarkers

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type WriteTxnMarkersResponse struct {
	ApiVersion      int16
	Markers         *[]WriteTxnMarkersResponseMarker // The results for writing makers. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type WriteTxnMarkersResponseMarker struct {
	ProducerId      int64                                 // The current producer ID in use by the transactional ID. (versions: 0+)
	Topics          *[]WriteTxnMarkersResponseMarkerTopic // The results by topic. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type WriteTxnMarkersResponseMarkerTopic struct {
	Name            *string                                        // The topic name. (versions: 0+)
	Partitions      *[]WriteTxnMarkersResponseMarkerTopicPartition // The results by partition. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type WriteTxnMarkersResponseMarkerTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	ErrorCode       int16 // The error code, or 0 if there was no error. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 1
}

func (res *WriteTxnMarkersResponse) Write(w io.Writer) error {
	// Markers (versions: 0+)
	if res.Markers == nil {
		return fmt.Errorf("WriteTxnMarkersResponse.Markers must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.markersEncoder, res.Markers); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.markersEncoder, *res.Markers); err != nil {
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
func (res *WriteTxnMarkersResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("WriteTxnMarkersResponse.Read: response or its body is nil")
	}

	*res = WriteTxnMarkersResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// Markers (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		markers, err := protocol.ReadCompactArray(r, res.markersDecoder)
		if err != nil {
			return err
		}
		res.Markers = &markers
	} else {
		markers, err := protocol.ReadArray(r, res.markersDecoder)
		if err != nil {
			return err
		}
		res.Markers = &markers
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

func (res *WriteTxnMarkersResponse) markersEncoder(w io.Writer, value WriteTxnMarkersResponseMarker) error {
	// ProducerId (versions: 0+)
	if err := protocol.WriteInt64(w, value.ProducerId); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if value.Topics == nil {
		return fmt.Errorf("WriteTxnMarkersResponseMarker.Topics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.topicsEncoder, value.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicsEncoder, *value.Topics); err != nil {
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

func (res *WriteTxnMarkersResponse) markersDecoder(r io.Reader) (WriteTxnMarkersResponseMarker, error) {
	writetxnmarkersresponsemarker := WriteTxnMarkersResponseMarker{}

	// ProducerId (versions: 0+)
	producerid, err := protocol.ReadInt64(r)
	if err != nil {
		return writetxnmarkersresponsemarker, err
	}
	writetxnmarkersresponsemarker.ProducerId = producerid

	// Topics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topics, err := protocol.ReadCompactArray(r, res.topicsDecoder)
		if err != nil {
			return writetxnmarkersresponsemarker, err
		}
		writetxnmarkersresponsemarker.Topics = &topics
	} else {
		topics, err := protocol.ReadArray(r, res.topicsDecoder)
		if err != nil {
			return writetxnmarkersresponsemarker, err
		}
		writetxnmarkersresponsemarker.Topics = &topics
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return writetxnmarkersresponsemarker, err
		}
		writetxnmarkersresponsemarker.rawTaggedFields = &rawTaggedFields
	}

	return writetxnmarkersresponsemarker, nil
}

func (res *WriteTxnMarkersResponse) topicsEncoder(w io.Writer, value WriteTxnMarkersResponseMarkerTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("WriteTxnMarkersResponseMarkerTopic.Name must not be nil in version %d", res.ApiVersion)
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
		return fmt.Errorf("WriteTxnMarkersResponseMarkerTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *WriteTxnMarkersResponse) topicsDecoder(r io.Reader) (WriteTxnMarkersResponseMarkerTopic, error) {
	writetxnmarkersresponsemarkertopic := WriteTxnMarkersResponseMarkerTopic{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return writetxnmarkersresponsemarkertopic, err
		}
		writetxnmarkersresponsemarkertopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return writetxnmarkersresponsemarkertopic, err
		}
		writetxnmarkersresponsemarkertopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return writetxnmarkersresponsemarkertopic, err
		}
		writetxnmarkersresponsemarkertopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return writetxnmarkersresponsemarkertopic, err
		}
		writetxnmarkersresponsemarkertopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return writetxnmarkersresponsemarkertopic, err
		}
		writetxnmarkersresponsemarkertopic.rawTaggedFields = &rawTaggedFields
	}

	return writetxnmarkersresponsemarkertopic, nil
}

func (res *WriteTxnMarkersResponse) partitionsEncoder(w io.Writer, value WriteTxnMarkersResponseMarkerTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
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

func (res *WriteTxnMarkersResponse) partitionsDecoder(r io.Reader) (WriteTxnMarkersResponseMarkerTopicPartition, error) {
	writetxnmarkersresponsemarkertopicpartition := WriteTxnMarkersResponseMarkerTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return writetxnmarkersresponsemarkertopicpartition, err
	}
	writetxnmarkersresponsemarkertopicpartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return writetxnmarkersresponsemarkertopicpartition, err
	}
	writetxnmarkersresponsemarkertopicpartition.ErrorCode = errorcode

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return writetxnmarkersresponsemarkertopicpartition, err
		}
		writetxnmarkersresponsemarkertopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return writetxnmarkersresponsemarkertopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *WriteTxnMarkersResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- WriteTxnMarkersResponse:\n")

	if res.Markers != nil {
		fmt.Fprintf(w, "        Markers:\n")
		for _, markers := range *res.Markers {
			fmt.Fprintf(w, "%s", markers.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Markers: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *WriteTxnMarkersResponseMarker) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ProducerId: %v\n", value.ProducerId)

	if value.Topics != nil {
		fmt.Fprintf(w, "            Topics:\n")
		for _, topics := range *value.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *WriteTxnMarkersResponseMarkerTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                Partitions:\n")
		for _, partitions := range *value.Partitions {
			fmt.Fprintf(w, "%s", partitions.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *WriteTxnMarkersResponseMarkerTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                    ErrorCode: %v\n", value.ErrorCode)

	return w.String()
}
