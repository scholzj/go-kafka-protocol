package produce

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ProduceRequest struct {
	ApiVersion      int16
	TransactionalId *string                    // The transactional ID, or null if the producer is not transactional.
	Acks            int16                      // The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
	TimeoutMs       int32                      // The timeout to await a response in milliseconds.
	TopicData       *[]ProduceRequestTopicData // Each topic to produce to.
	rawTaggedFields *[]protocol.TaggedField
}

type ProduceRequestTopicData struct {
	Name            *string                                 // The topic name.
	TopicId         uuid.UUID                               // The unique topic ID
	PartitionData   *[]ProduceRequestTopicDataPartitionData // Each partition to produce to.
	rawTaggedFields *[]protocol.TaggedField
}

type ProduceRequestTopicDataPartitionData struct {
	Index           int32   // The partition index.
	Records         *[]byte // The record data to be produced.
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 9
}

func (req *ProduceRequest) Write(w io.Writer) error {
	// TransactionalId (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, req.TransactionalId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, req.TransactionalId); err != nil {
				return err
			}
		}
	}

	// Acks (versions: 0+)
	if err := protocol.WriteInt16(w, req.Acks); err != nil {
		return err
	}

	// TimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.TimeoutMs); err != nil {
		return err
	}

	// TopicData (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicDataEncoder, req.TopicData); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.topicDataEncoder, *req.TopicData); err != nil {
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
func (req *ProduceRequest) Read(request protocol.Request) error {
	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	var err error

	// TransactionalId (versions: 3+)
	if request.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			transactionalid, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			req.TransactionalId = transactionalid
		} else {
			transactionalid, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			req.TransactionalId = transactionalid
		}
	}

	// Acks (versions: 0+)
	acks, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	req.Acks = acks

	// TimeoutMs (versions: 0+)
	timeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.TimeoutMs = timeoutms

	// TopicData (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topicdata, err := protocol.ReadNullableCompactArray(r, req.topicDataDecoder)
		if err != nil {
			return err
		}
		req.TopicData = topicdata
	} else {
		topicdata, err := protocol.ReadArray(r, req.topicDataDecoder)
		if err != nil {
			return err
		}
		req.TopicData = &topicdata
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		req.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

func (req *ProduceRequest) topicDataEncoder(w io.Writer, value ProduceRequestTopicData) error {
	// Name (versions: 0-12)
	if req.ApiVersion <= 12 {
		if isRequestFlexible(req.ApiVersion) {
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
	if req.ApiVersion >= 13 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// PartitionData (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.partitionDataEncoder, value.PartitionData); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.partitionDataEncoder, *value.PartitionData); err != nil {
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

func (req *ProduceRequest) topicDataDecoder(r io.Reader) (ProduceRequestTopicData, error) {
	producerequesttopicdata := ProduceRequestTopicData{}
	var err error

	// Name (versions: 0-12)
	if req.ApiVersion <= 12 {
		if isRequestFlexible(req.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return producerequesttopicdata, err
			}
			producerequesttopicdata.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return producerequesttopicdata, err
			}
			producerequesttopicdata.Name = &name
		}
	}

	// TopicId (versions: 13+)
	if req.ApiVersion >= 13 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return producerequesttopicdata, err
		}
		producerequesttopicdata.TopicId = topicid
	}

	// PartitionData (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitiondata, err := protocol.ReadNullableCompactArray(r, req.partitionDataDecoder)
		if err != nil {
			return producerequesttopicdata, err
		}
		producerequesttopicdata.PartitionData = partitiondata
	} else {
		partitiondata, err := protocol.ReadArray(r, req.partitionDataDecoder)
		if err != nil {
			return producerequesttopicdata, err
		}
		producerequesttopicdata.PartitionData = &partitiondata
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return producerequesttopicdata, err
		}
		producerequesttopicdata.rawTaggedFields = &rawTaggedFields
	}

	return producerequesttopicdata, nil
}

func (req *ProduceRequest) partitionDataEncoder(w io.Writer, value ProduceRequestTopicDataPartitionData) error {
	// Index (versions: 0+)
	if err := protocol.WriteInt32(w, value.Index); err != nil {
		return err
	}

	// Records (versions: 0+)
	if err := protocol.WriteCompactRecords(w, value.Records); err != nil {
		return err
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

func (req *ProduceRequest) partitionDataDecoder(r io.Reader) (ProduceRequestTopicDataPartitionData, error) {
	producerequesttopicdatapartitiondata := ProduceRequestTopicDataPartitionData{}
	var err error

	// Index (versions: 0+)
	index, err := protocol.ReadInt32(r)
	if err != nil {
		return producerequesttopicdatapartitiondata, err
	}
	producerequesttopicdatapartitiondata.Index = index

	// Records (versions: 0+)
	records, err := protocol.ReadCompactRecords(r)
	if err != nil {
		return producerequesttopicdatapartitiondata, err
	}
	producerequesttopicdatapartitiondata.Records = records

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return producerequesttopicdatapartitiondata, err
		}
		producerequesttopicdatapartitiondata.rawTaggedFields = &rawTaggedFields
	}

	return producerequesttopicdatapartitiondata, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *ProduceRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ProduceRequest:\n")
	if req.TransactionalId != nil {
		fmt.Fprintf(w, "        TransactionalId: %v\n", *req.TransactionalId)
	} else {
		fmt.Fprintf(w, "        TransactionalId: nil\n")
	}
	fmt.Fprintf(w, "        Acks: %v\n", req.Acks)
	fmt.Fprintf(w, "        TimeoutMs: %v\n", req.TimeoutMs)
	if req.TopicData != nil {
		fmt.Fprintf(w, "        TopicData:\n")
		for _, topicdata := range *req.TopicData {
			fmt.Fprintf(w, "%s", topicdata.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        TopicData: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ProduceRequestTopicData) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}
	fmt.Fprintf(w, "            TopicId: %v\n", value.TopicId)
	if value.PartitionData != nil {
		fmt.Fprintf(w, "            PartitionData:\n")
		for _, partitiondata := range *value.PartitionData {
			fmt.Fprintf(w, "%s", partitiondata.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            PartitionData: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ProduceRequestTopicDataPartitionData) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Index: %v\n", value.Index)
	fmt.Fprintf(w, "                Records: %v\n", value.Records)

	return w.String()
}
