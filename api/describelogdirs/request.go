package describelogdirs

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeLogDirsRequest struct {
	ApiVersion      int16
	Topics          *[]DescribeLogDirsRequestTopic // Each topic that we want to describe log directories for, or null for all topics. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeLogDirsRequestTopic struct {
	Topic           *string  // The topic name. (versions: 0+)
	Partitions      *[]int32 // The partition indexes. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *DescribeLogDirsRequest) Write(w io.Writer) error {
	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, req.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, req.topicsEncoder, req.Topics); err != nil {
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
func (req *DescribeLogDirsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeLogDirsRequest.Read: request or its body is nil")
	}

	*req = DescribeLogDirsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = topics
	} else {
		topics, err := protocol.ReadNullableArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = topics
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

func (req *DescribeLogDirsRequest) topicsEncoder(w io.Writer, value DescribeLogDirsRequestTopic) error {
	// Topic (versions: 0+)
	if value.Topic == nil {
		return fmt.Errorf("DescribeLogDirsRequestTopic.Topic must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Topic); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Topic); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("DescribeLogDirsRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
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

func (req *DescribeLogDirsRequest) topicsDecoder(r io.Reader) (DescribeLogDirsRequestTopic, error) {
	describelogdirsrequesttopic := DescribeLogDirsRequestTopic{}

	// Topic (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topic, err := protocol.ReadCompactString(r)
		if err != nil {
			return describelogdirsrequesttopic, err
		}
		describelogdirsrequesttopic.Topic = &topic
	} else {
		topic, err := protocol.ReadString(r)
		if err != nil {
			return describelogdirsrequesttopic, err
		}
		describelogdirsrequesttopic.Topic = &topic
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return describelogdirsrequesttopic, err
		}
		describelogdirsrequesttopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return describelogdirsrequesttopic, err
		}
		describelogdirsrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describelogdirsrequesttopic, err
		}
		describelogdirsrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return describelogdirsrequesttopic, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DescribeLogDirsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeLogDirsRequest:\n")

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
func (value *DescribeLogDirsRequestTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Topic != nil {
		fmt.Fprintf(w, "            Topic: %v\n", *value.Topic)
	} else {
		fmt.Fprintf(w, "            Topic: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "            Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "            Partitions: nil\n")
	}

	return w.String()
}
