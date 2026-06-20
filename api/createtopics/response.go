package createtopics

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type CreateTopicsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                        // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 2+)
	Topics          *[]CreateTopicsResponseTopic // Results for each topic we tried to create. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type CreateTopicsResponseTopic struct {
	Name                 *string                            // The topic name. (versions: 0+)
	TopicId              uuid.UUID                          // The unique topic ID. (versions: 7+)
	ErrorCode            int16                              // The error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage         *string                            // The error message, or null if there was no error. (versions: 1+, nullable: 0+)
	TopicConfigErrorCode int16                              // tag 0: Optional topic config error returned if configs are not returned in the response. (versions: 5+)
	NumPartitions        int32                              // Number of partitions of the topic. (versions: 5+)
	ReplicationFactor    int16                              // Replication factor of the topic. (versions: 5+)
	Configs              *[]CreateTopicsResponseTopicConfig // Configuration of the topic. (versions: 5+, nullable: 5+)
	rawTaggedFields      *[]protocol.TaggedField
}

type CreateTopicsResponseTopicConfig struct {
	Name            *string // The configuration name. (versions: 5+)
	Value           *string // The configuration value. (versions: 5+, nullable: 5+)
	ReadOnly        bool    // True if the configuration is read-only. (versions: 5+)
	ConfigSource    int8    // The configuration source. (versions: 5+)
	IsSensitive     bool    // True if this configuration is sensitive. (versions: 5+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 5
}

func (res *CreateTopicsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("CreateTopicsResponse.Topics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.topicsEncoder, res.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicsEncoder, *res.Topics); err != nil {
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
func (res *CreateTopicsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("CreateTopicsResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 2+)
	if res.ApiVersion >= 2 {
		throttletimems, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttletimems
	}

	// Topics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, res.topicsDecoder)
		if err != nil {
			return err
		}
		res.Topics = topics
	} else {
		topics, err := protocol.ReadArray(r, res.topicsDecoder)
		if err != nil {
			return err
		}
		res.Topics = &topics
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

func (res *CreateTopicsResponse) topicsEncoder(w io.Writer, value CreateTopicsResponseTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("CreateTopicsResponseTopic.Name must not be nil in version %d", res.ApiVersion)
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

	// TopicId (versions: 7+)
	if res.ApiVersion >= 7 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 1+)
	if res.ApiVersion >= 1 {
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

	// TopicConfigErrorCode (versions: 5+)
	if !isResponseFlexible(res.ApiVersion) {
		if res.ApiVersion >= 5 {
			if err := protocol.WriteInt16(w, value.TopicConfigErrorCode); err != nil {
				return err
			}
		}
	}

	// NumPartitions (versions: 5+)
	if res.ApiVersion >= 5 {
		if err := protocol.WriteInt32(w, value.NumPartitions); err != nil {
			return err
		}
	}

	// ReplicationFactor (versions: 5+)
	if res.ApiVersion >= 5 {
		if err := protocol.WriteInt16(w, value.ReplicationFactor); err != nil {
			return err
		}
	}

	// Configs (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.configsEncoder, value.Configs); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableArray(w, res.configsEncoder, value.Configs); err != nil {
				return err
			}
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		taggedFields, err := res.taggedFieldsEncoderTopics(value)
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *CreateTopicsResponse) topicsDecoder(r io.Reader) (CreateTopicsResponseTopic, error) {
	createtopicsresponsetopic := CreateTopicsResponseTopic{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return createtopicsresponsetopic, err
		}
		createtopicsresponsetopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return createtopicsresponsetopic, err
		}
		createtopicsresponsetopic.Name = &name
	}

	// TopicId (versions: 7+)
	if res.ApiVersion >= 7 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return createtopicsresponsetopic, err
		}
		createtopicsresponsetopic.TopicId = topicid
	}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return createtopicsresponsetopic, err
	}
	createtopicsresponsetopic.ErrorCode = errorcode

	// ErrorMessage (versions: 1+)
	if res.ApiVersion >= 1 {
		if isResponseFlexible(res.ApiVersion) {
			errormessage, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return createtopicsresponsetopic, err
			}
			createtopicsresponsetopic.ErrorMessage = errormessage
		} else {
			errormessage, err := protocol.ReadNullableString(r)
			if err != nil {
				return createtopicsresponsetopic, err
			}
			createtopicsresponsetopic.ErrorMessage = errormessage
		}
	}

	// TopicConfigErrorCode (versions: 5+)
	if !isResponseFlexible(res.ApiVersion) {
		if res.ApiVersion >= 5 {
			topicconfigerrorcode, err := protocol.ReadInt16(r)
			if err != nil {
				return createtopicsresponsetopic, err
			}
			createtopicsresponsetopic.TopicConfigErrorCode = topicconfigerrorcode
		}
	}

	// NumPartitions (versions: 5+)
	if res.ApiVersion >= 5 {
		numpartitions, err := protocol.ReadInt32(r)
		if err != nil {
			return createtopicsresponsetopic, err
		}
		createtopicsresponsetopic.NumPartitions = numpartitions
	}

	// ReplicationFactor (versions: 5+)
	if res.ApiVersion >= 5 {
		replicationfactor, err := protocol.ReadInt16(r)
		if err != nil {
			return createtopicsresponsetopic, err
		}
		createtopicsresponsetopic.ReplicationFactor = replicationfactor
	}

	// Configs (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			configs, err := protocol.ReadNullableCompactArray(r, res.configsDecoder)
			if err != nil {
				return createtopicsresponsetopic, err
			}
			createtopicsresponsetopic.Configs = configs
		} else {
			configs, err := protocol.ReadNullableArray(r, res.configsDecoder)
			if err != nil {
				return createtopicsresponsetopic, err
			}
			createtopicsresponsetopic.Configs = configs
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.ReadTaggedFields(r, func(r io.Reader, tag uint64, tagLength uint64) error {
			return res.taggedFieldsDecoderTopics(r, tag, tagLength, &createtopicsresponsetopic)
		}); err != nil {
			return createtopicsresponsetopic, err
		}
	}

	return createtopicsresponsetopic, nil
}

func (res *CreateTopicsResponse) taggedFieldsEncoderTopics(value CreateTopicsResponseTopic) ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if value.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*value.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 1+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	buf = bytes.NewBuffer(make([]byte, 0))
	if err := protocol.WriteInt16(buf, value.TopicConfigErrorCode); err != nil {
		return taggedFields, err
	}

	taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})

	// We append any raw tagged fields to the end of the array
	if value.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *value.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (res *CreateTopicsResponse) taggedFieldsDecoderTopics(r io.Reader, tag uint64, tagLength uint64, value *CreateTopicsResponseTopic) error {
	rawTaggedFields := make([]protocol.TaggedField, 0)

	switch tag {
	case 0:
		// TopicConfigErrorCode
		topicconfigerrorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		value.TopicConfigErrorCode = topicconfigerrorcode
	default:
		// Unknown tag - keep the raw bytes (r is bounded to this tag's length by ReadTaggedFields)
		field, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		rawTaggedFields = append(rawTaggedFields, protocol.TaggedField{Tag: tag, Field: field})
	}

	// Set the raw tagged fields
	value.rawTaggedFields = &rawTaggedFields

	return nil
}

func (res *CreateTopicsResponse) configsEncoder(w io.Writer, value CreateTopicsResponseTopicConfig) error {
	// Name (versions: 5+)
	if res.ApiVersion >= 5 {
		if value.Name == nil {
			return fmt.Errorf("CreateTopicsResponseTopicConfig.Name must not be nil in version %d", res.ApiVersion)
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
	}

	// Value (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.Value); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.Value); err != nil {
				return err
			}
		}
	}

	// ReadOnly (versions: 5+)
	if res.ApiVersion >= 5 {
		if err := protocol.WriteBool(w, value.ReadOnly); err != nil {
			return err
		}
	}

	// ConfigSource (versions: 5+)
	if res.ApiVersion >= 5 {
		if err := protocol.WriteInt8(w, value.ConfigSource); err != nil {
			return err
		}
	}

	// IsSensitive (versions: 5+)
	if res.ApiVersion >= 5 {
		if err := protocol.WriteBool(w, value.IsSensitive); err != nil {
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

func (res *CreateTopicsResponse) configsDecoder(r io.Reader) (CreateTopicsResponseTopicConfig, error) {
	createtopicsresponsetopicconfig := CreateTopicsResponseTopicConfig{}

	// Name (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return createtopicsresponsetopicconfig, err
			}
			createtopicsresponsetopicconfig.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return createtopicsresponsetopicconfig, err
			}
			createtopicsresponsetopicconfig.Name = &name
		}
	}

	// Value (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			value, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return createtopicsresponsetopicconfig, err
			}
			createtopicsresponsetopicconfig.Value = value
		} else {
			value, err := protocol.ReadNullableString(r)
			if err != nil {
				return createtopicsresponsetopicconfig, err
			}
			createtopicsresponsetopicconfig.Value = value
		}
	}

	// ReadOnly (versions: 5+)
	if res.ApiVersion >= 5 {
		readonly, err := protocol.ReadBool(r)
		if err != nil {
			return createtopicsresponsetopicconfig, err
		}
		createtopicsresponsetopicconfig.ReadOnly = readonly
	}

	// ConfigSource (versions: 5+)
	if res.ApiVersion >= 5 {
		configsource, err := protocol.ReadInt8(r)
		if err != nil {
			return createtopicsresponsetopicconfig, err
		}
		createtopicsresponsetopicconfig.ConfigSource = configsource
	}

	// IsSensitive (versions: 5+)
	if res.ApiVersion >= 5 {
		issensitive, err := protocol.ReadBool(r)
		if err != nil {
			return createtopicsresponsetopicconfig, err
		}
		createtopicsresponsetopicconfig.IsSensitive = issensitive
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return createtopicsresponsetopicconfig, err
		}
		createtopicsresponsetopicconfig.rawTaggedFields = &rawTaggedFields
	}

	return createtopicsresponsetopicconfig, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *CreateTopicsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- CreateTopicsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	if res.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *res.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *CreateTopicsResponseTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	fmt.Fprintf(w, "            TopicId: %v\n", value.TopicId)
	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	fmt.Fprintf(w, "            TopicConfigErrorCode: %v\n", value.TopicConfigErrorCode)
	fmt.Fprintf(w, "            NumPartitions: %v\n", value.NumPartitions)
	fmt.Fprintf(w, "            ReplicationFactor: %v\n", value.ReplicationFactor)

	if value.Configs != nil {
		fmt.Fprintf(w, "            Configs:\n")
		for _, configs := range *value.Configs {
			fmt.Fprintf(w, "%s", configs.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Configs: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *CreateTopicsResponseTopicConfig) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

	if value.Value != nil {
		fmt.Fprintf(w, "                Value: %v\n", *value.Value)
	} else {
		fmt.Fprintf(w, "                Value: nil\n")
	}

	fmt.Fprintf(w, "                ReadOnly: %v\n", value.ReadOnly)
	fmt.Fprintf(w, "                ConfigSource: %v\n", value.ConfigSource)
	fmt.Fprintf(w, "                IsSensitive: %v\n", value.IsSensitive)

	return w.String()
}
