package writetxnmarkers

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type WriteTxnMarkersRequest struct {
	ApiVersion      int16
	Markers         *[]WriteTxnMarkersRequestMarker // The transaction markers to be written. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type WriteTxnMarkersRequestMarker struct {
	ProducerId         int64                                // The current producer ID. (versions: 0+)
	ProducerEpoch      int16                                // The current epoch associated with the producer ID. (versions: 0+)
	TransactionResult  bool                                 // The result of the transaction to write to the partitions (false = ABORT, true = COMMIT). (versions: 0+)
	Topics             *[]WriteTxnMarkersRequestMarkerTopic // Each topic that we want to write transaction marker(s) for. (versions: 0+)
	CoordinatorEpoch   int32                                // Epoch associated with the transaction state partition hosted by this transaction coordinator. (versions: 0+)
	TransactionVersion int8                                 // Transaction version of the marker. Ex: 0/1 = legacy (TV0/TV1), 2 = TV2 etc. (versions: 2+)
	rawTaggedFields    *[]protocol.TaggedField
}

type WriteTxnMarkersRequestMarkerTopic struct {
	Name             *string  // The topic name. (versions: 0+)
	PartitionIndexes *[]int32 // The indexes of the partitions to write transaction markers for. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 1
}

func (req *WriteTxnMarkersRequest) Write(w io.Writer) error {
	// Markers (versions: 0+)
	if req.Markers == nil {
		return fmt.Errorf("WriteTxnMarkersRequest.Markers must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.markersEncoder, req.Markers); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.markersEncoder, *req.Markers); err != nil {
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
func (req *WriteTxnMarkersRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("WriteTxnMarkersRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Markers (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		markers, err := protocol.ReadNullableCompactArray(r, req.markersDecoder)
		if err != nil {
			return err
		}
		req.Markers = markers
	} else {
		markers, err := protocol.ReadArray(r, req.markersDecoder)
		if err != nil {
			return err
		}
		req.Markers = &markers
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

func (req *WriteTxnMarkersRequest) markersEncoder(w io.Writer, value WriteTxnMarkersRequestMarker) error {
	// ProducerId (versions: 0+)
	if err := protocol.WriteInt64(w, value.ProducerId); err != nil {
		return err
	}

	// ProducerEpoch (versions: 0+)
	if err := protocol.WriteInt16(w, value.ProducerEpoch); err != nil {
		return err
	}

	// TransactionResult (versions: 0+)
	if err := protocol.WriteBool(w, value.TransactionResult); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if value.Topics == nil {
		return fmt.Errorf("WriteTxnMarkersRequestMarker.Topics must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, value.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.topicsEncoder, *value.Topics); err != nil {
			return err
		}
	}

	// CoordinatorEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.CoordinatorEpoch); err != nil {
		return err
	}

	// TransactionVersion (versions: 2+)
	if req.ApiVersion >= 2 {
		if err := protocol.WriteInt8(w, value.TransactionVersion); err != nil {
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

func (req *WriteTxnMarkersRequest) markersDecoder(r io.Reader) (WriteTxnMarkersRequestMarker, error) {
	writetxnmarkersrequestmarker := WriteTxnMarkersRequestMarker{}

	// ProducerId (versions: 0+)
	producerid, err := protocol.ReadInt64(r)
	if err != nil {
		return writetxnmarkersrequestmarker, err
	}
	writetxnmarkersrequestmarker.ProducerId = producerid

	// ProducerEpoch (versions: 0+)
	producerepoch, err := protocol.ReadInt16(r)
	if err != nil {
		return writetxnmarkersrequestmarker, err
	}
	writetxnmarkersrequestmarker.ProducerEpoch = producerepoch

	// TransactionResult (versions: 0+)
	transactionresult, err := protocol.ReadBool(r)
	if err != nil {
		return writetxnmarkersrequestmarker, err
	}
	writetxnmarkersrequestmarker.TransactionResult = transactionresult

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, req.topicsDecoder)
		if err != nil {
			return writetxnmarkersrequestmarker, err
		}
		writetxnmarkersrequestmarker.Topics = topics
	} else {
		topics, err := protocol.ReadArray(r, req.topicsDecoder)
		if err != nil {
			return writetxnmarkersrequestmarker, err
		}
		writetxnmarkersrequestmarker.Topics = &topics
	}

	// CoordinatorEpoch (versions: 0+)
	coordinatorepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return writetxnmarkersrequestmarker, err
	}
	writetxnmarkersrequestmarker.CoordinatorEpoch = coordinatorepoch

	// TransactionVersion (versions: 2+)
	if req.ApiVersion >= 2 {
		transactionversion, err := protocol.ReadInt8(r)
		if err != nil {
			return writetxnmarkersrequestmarker, err
		}
		writetxnmarkersrequestmarker.TransactionVersion = transactionversion
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return writetxnmarkersrequestmarker, err
		}
		writetxnmarkersrequestmarker.rawTaggedFields = &rawTaggedFields
	}

	return writetxnmarkersrequestmarker, nil
}

func (req *WriteTxnMarkersRequest) topicsEncoder(w io.Writer, value WriteTxnMarkersRequestMarkerTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("WriteTxnMarkersRequestMarkerTopic.Name must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// PartitionIndexes (versions: 0+)
	if value.PartitionIndexes == nil {
		return fmt.Errorf("WriteTxnMarkersRequestMarkerTopic.PartitionIndexes must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.PartitionIndexes); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.PartitionIndexes); err != nil {
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

func (req *WriteTxnMarkersRequest) topicsDecoder(r io.Reader) (WriteTxnMarkersRequestMarkerTopic, error) {
	writetxnmarkersrequestmarkertopic := WriteTxnMarkersRequestMarkerTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return writetxnmarkersrequestmarkertopic, err
		}
		writetxnmarkersrequestmarkertopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return writetxnmarkersrequestmarkertopic, err
		}
		writetxnmarkersrequestmarkertopic.Name = &name
	}

	// PartitionIndexes (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitionindexes, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return writetxnmarkersrequestmarkertopic, err
		}
		writetxnmarkersrequestmarkertopic.PartitionIndexes = partitionindexes
	} else {
		partitionindexes, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return writetxnmarkersrequestmarkertopic, err
		}
		writetxnmarkersrequestmarkertopic.PartitionIndexes = &partitionindexes
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return writetxnmarkersrequestmarkertopic, err
		}
		writetxnmarkersrequestmarkertopic.rawTaggedFields = &rawTaggedFields
	}

	return writetxnmarkersrequestmarkertopic, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *WriteTxnMarkersRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> WriteTxnMarkersRequest:\n")

	if req.Markers != nil {
		fmt.Fprintf(w, "        Markers:\n")
		for _, markers := range *req.Markers {
			fmt.Fprintf(w, "%s", markers.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Markers: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *WriteTxnMarkersRequestMarker) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ProducerId: %v\n", value.ProducerId)
	fmt.Fprintf(w, "            ProducerEpoch: %v\n", value.ProducerEpoch)
	fmt.Fprintf(w, "            TransactionResult: %v\n", value.TransactionResult)

	if value.Topics != nil {
		fmt.Fprintf(w, "            Topics:\n")
		for _, topics := range *value.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Topics: nil\n")
	}

	fmt.Fprintf(w, "            CoordinatorEpoch: %v\n", value.CoordinatorEpoch)
	fmt.Fprintf(w, "            TransactionVersion: %v\n", value.TransactionVersion)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *WriteTxnMarkersRequestMarkerTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

	if value.PartitionIndexes != nil {
		fmt.Fprintf(w, "                PartitionIndexes: %v\n", *value.PartitionIndexes)
	} else {
		fmt.Fprintf(w, "                PartitionIndexes: nil\n")
	}

	return w.String()
}
