package createpartitions

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type CreatePartitionsRequest struct {
	ApiVersion      int16
	Topics          *[]CreatePartitionsRequestTopic // Each topic that we want to create new partitions inside. (versions: 0+)
	TimeoutMs       int32                           // The time in ms to wait for the partitions to be created. (versions: 0+)
	ValidateOnly    bool                            // If true, then validate the request, but don't actually increase the number of partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type CreatePartitionsRequestTopic struct {
	Name            *string                                   // The topic name. (versions: 0+)
	Count           int32                                     // The new partition count. (versions: 0+)
	Assignments     *[]CreatePartitionsRequestTopicAssignment // The new partition assignments. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type CreatePartitionsRequestTopicAssignment struct {
	BrokerIds       *[]int32 // The assigned broker IDs. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *CreatePartitionsRequest) Write(w io.Writer) error {
	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("CreatePartitionsRequest.Topics must not be nil in version %d", req.ApiVersion)
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

	// TimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.TimeoutMs); err != nil {
		return err
	}

	// ValidateOnly (versions: 0+)
	if err := protocol.WriteBool(w, req.ValidateOnly); err != nil {
		return err
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
func (req *CreatePartitionsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("CreatePartitionsRequest.Read: request or its body is nil")
	}

	*req = CreatePartitionsRequest{}

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

	// TimeoutMs (versions: 0+)
	timeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.TimeoutMs = timeoutms

	// ValidateOnly (versions: 0+)
	validateonly, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	req.ValidateOnly = validateonly

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

func (req *CreatePartitionsRequest) topicsEncoder(w io.Writer, value CreatePartitionsRequestTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("CreatePartitionsRequestTopic.Name must not be nil in version %d", req.ApiVersion)
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

	// Count (versions: 0+)
	if err := protocol.WriteInt32(w, value.Count); err != nil {
		return err
	}

	// Assignments (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.assignmentsEncoder, value.Assignments); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, req.assignmentsEncoder, value.Assignments); err != nil {
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

func (req *CreatePartitionsRequest) topicsDecoder(r io.Reader) (CreatePartitionsRequestTopic, error) {
	createpartitionsrequesttopic := CreatePartitionsRequestTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return createpartitionsrequesttopic, err
		}
		createpartitionsrequesttopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return createpartitionsrequesttopic, err
		}
		createpartitionsrequesttopic.Name = &name
	}

	// Count (versions: 0+)
	count, err := protocol.ReadInt32(r)
	if err != nil {
		return createpartitionsrequesttopic, err
	}
	createpartitionsrequesttopic.Count = count

	// Assignments (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		assignments, err := protocol.ReadNullableCompactArray(r, req.assignmentsDecoder)
		if err != nil {
			return createpartitionsrequesttopic, err
		}
		createpartitionsrequesttopic.Assignments = assignments
	} else {
		assignments, err := protocol.ReadNullableArray(r, req.assignmentsDecoder)
		if err != nil {
			return createpartitionsrequesttopic, err
		}
		createpartitionsrequesttopic.Assignments = assignments
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return createpartitionsrequesttopic, err
		}
		createpartitionsrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return createpartitionsrequesttopic, nil
}

func (req *CreatePartitionsRequest) assignmentsEncoder(w io.Writer, value CreatePartitionsRequestTopicAssignment) error {
	// BrokerIds (versions: 0+)
	if value.BrokerIds == nil {
		return fmt.Errorf("CreatePartitionsRequestTopicAssignment.BrokerIds must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.BrokerIds); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.BrokerIds); err != nil {
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

func (req *CreatePartitionsRequest) assignmentsDecoder(r io.Reader) (CreatePartitionsRequestTopicAssignment, error) {
	createpartitionsrequesttopicassignment := CreatePartitionsRequestTopicAssignment{}

	// BrokerIds (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		brokerids, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return createpartitionsrequesttopicassignment, err
		}
		createpartitionsrequesttopicassignment.BrokerIds = &brokerids
	} else {
		brokerids, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return createpartitionsrequesttopicassignment, err
		}
		createpartitionsrequesttopicassignment.BrokerIds = &brokerids
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return createpartitionsrequesttopicassignment, err
		}
		createpartitionsrequesttopicassignment.rawTaggedFields = &rawTaggedFields
	}

	return createpartitionsrequesttopicassignment, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *CreatePartitionsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> CreatePartitionsRequest:\n")

	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	fmt.Fprintf(w, "        TimeoutMs: %v\n", req.TimeoutMs)
	fmt.Fprintf(w, "        ValidateOnly: %v\n", req.ValidateOnly)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *CreatePartitionsRequestTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	fmt.Fprintf(w, "            Count: %v\n", value.Count)

	if value.Assignments != nil {
		fmt.Fprintf(w, "            Assignments:\n")
		for _, assignments := range *value.Assignments {
			fmt.Fprintf(w, "%s", assignments.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Assignments: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *CreatePartitionsRequestTopicAssignment) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.BrokerIds != nil {
		fmt.Fprintf(w, "                BrokerIds: %v\n", *value.BrokerIds)
	} else {
		fmt.Fprintf(w, "                BrokerIds: nil\n")
	}

	return w.String()
}
