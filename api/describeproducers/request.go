package describeproducers

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeProducersRequest struct {
	ApiVersion      int16
	Topics          *[]DescribeProducersRequestTopic // The topics to list producers for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeProducersRequestTopic struct {
	Name             *string  // The topic name. (versions: 0+)
	PartitionIndexes *[]int32 // The indexes of the partitions to list producers for. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *DescribeProducersRequest) Write(w io.Writer) error {
	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("DescribeProducersRequest.Topics must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, req.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.topicsEncoder, *req.Topics); err != nil {
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
func (req *DescribeProducersRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeProducersRequest.Read: request or its body is nil")
	}

	*req = DescribeProducersRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadCompactArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = &topics
	} else {
		topics, err := protocol.ReadArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = &topics
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

func (req *DescribeProducersRequest) topicsEncoder(w io.Writer, value DescribeProducersRequestTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("DescribeProducersRequestTopic.Name must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("DescribeProducersRequestTopic.PartitionIndexes must not be nil in version %d", req.ApiVersion)
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

func (req *DescribeProducersRequest) topicsDecoder(r io.Reader) (DescribeProducersRequestTopic, error) {
	describeproducersrequesttopic := DescribeProducersRequestTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeproducersrequesttopic, err
		}
		describeproducersrequesttopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return describeproducersrequesttopic, err
		}
		describeproducersrequesttopic.Name = &name
	}

	// PartitionIndexes (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitionindexes, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return describeproducersrequesttopic, err
		}
		describeproducersrequesttopic.PartitionIndexes = &partitionindexes
	} else {
		partitionindexes, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return describeproducersrequesttopic, err
		}
		describeproducersrequesttopic.PartitionIndexes = &partitionindexes
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeproducersrequesttopic, err
		}
		describeproducersrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return describeproducersrequesttopic, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DescribeProducersRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeProducersRequest:\n")

	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeProducersRequestTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	if value.PartitionIndexes != nil {
		fmt.Fprintf(w, "            PartitionIndexes: %v\n", *value.PartitionIndexes)
	} else {
		fmt.Fprintf(w, "            PartitionIndexes: nil\n")
	}

	return w.String()
}
