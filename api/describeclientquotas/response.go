package describeclientquotas

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeClientQuotasResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                 // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                                 // The error code, or `0` if the quota description succeeded. (versions: 0+)
	ErrorMessage    *string                               // The error message, or `null` if the quota description succeeded. (versions: 0+, nullable: 0+)
	Entries         *[]DescribeClientQuotasResponseEntrie // A result entry. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeClientQuotasResponseEntrie struct {
	Entity          *[]DescribeClientQuotasResponseEntrieEntity // The quota entity description. (versions: 0+)
	Values          *[]DescribeClientQuotasResponseEntrieValue  // The quota values for the entity. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeClientQuotasResponseEntrieEntity struct {
	EntityType      *string // The entity type. (versions: 0+)
	EntityName      *string // The entity name, or null if the default. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeClientQuotasResponseEntrieValue struct {
	Key             *string // The quota configuration key. (versions: 0+)
	Value           float64 // The quota configuration value. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 1
}

func (res *DescribeClientQuotasResponse) Write(w io.Writer) error {
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

	// Entries (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.entriesEncoder, res.Entries); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, res.entriesEncoder, res.Entries); err != nil {
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
func (res *DescribeClientQuotasResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeClientQuotasResponse.Read: response or its body is nil")
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

	// Entries (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		entries, err := protocol.ReadNullableCompactArray(r, res.entriesDecoder)
		if err != nil {
			return err
		}
		res.Entries = entries
	} else {
		entries, err := protocol.ReadNullableArray(r, res.entriesDecoder)
		if err != nil {
			return err
		}
		res.Entries = entries
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

func (res *DescribeClientQuotasResponse) entriesEncoder(w io.Writer, value DescribeClientQuotasResponseEntrie) error {
	// Entity (versions: 0+)
	if value.Entity == nil {
		return fmt.Errorf("DescribeClientQuotasResponseEntrie.Entity must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.entityEncoder, value.Entity); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.entityEncoder, *value.Entity); err != nil {
			return err
		}
	}

	// Values (versions: 0+)
	if value.Values == nil {
		return fmt.Errorf("DescribeClientQuotasResponseEntrie.Values must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.valuesEncoder, value.Values); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.valuesEncoder, *value.Values); err != nil {
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

func (res *DescribeClientQuotasResponse) entriesDecoder(r io.Reader) (DescribeClientQuotasResponseEntrie, error) {
	describeclientquotasresponseentrie := DescribeClientQuotasResponseEntrie{}

	// Entity (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		entity, err := protocol.ReadNullableCompactArray(r, res.entityDecoder)
		if err != nil {
			return describeclientquotasresponseentrie, err
		}
		describeclientquotasresponseentrie.Entity = entity
	} else {
		entity, err := protocol.ReadArray(r, res.entityDecoder)
		if err != nil {
			return describeclientquotasresponseentrie, err
		}
		describeclientquotasresponseentrie.Entity = &entity
	}

	// Values (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		values, err := protocol.ReadNullableCompactArray(r, res.valuesDecoder)
		if err != nil {
			return describeclientquotasresponseentrie, err
		}
		describeclientquotasresponseentrie.Values = values
	} else {
		values, err := protocol.ReadArray(r, res.valuesDecoder)
		if err != nil {
			return describeclientquotasresponseentrie, err
		}
		describeclientquotasresponseentrie.Values = &values
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeclientquotasresponseentrie, err
		}
		describeclientquotasresponseentrie.rawTaggedFields = &rawTaggedFields
	}

	return describeclientquotasresponseentrie, nil
}

func (res *DescribeClientQuotasResponse) entityEncoder(w io.Writer, value DescribeClientQuotasResponseEntrieEntity) error {
	// EntityType (versions: 0+)
	if value.EntityType == nil {
		return fmt.Errorf("DescribeClientQuotasResponseEntrieEntity.EntityType must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.EntityType); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.EntityType); err != nil {
			return err
		}
	}

	// EntityName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.EntityName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.EntityName); err != nil {
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

func (res *DescribeClientQuotasResponse) entityDecoder(r io.Reader) (DescribeClientQuotasResponseEntrieEntity, error) {
	describeclientquotasresponseentrieentity := DescribeClientQuotasResponseEntrieEntity{}

	// EntityType (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		entitytype, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeclientquotasresponseentrieentity, err
		}
		describeclientquotasresponseentrieentity.EntityType = &entitytype
	} else {
		entitytype, err := protocol.ReadString(r)
		if err != nil {
			return describeclientquotasresponseentrieentity, err
		}
		describeclientquotasresponseentrieentity.EntityType = &entitytype
	}

	// EntityName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		entityname, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return describeclientquotasresponseentrieentity, err
		}
		describeclientquotasresponseentrieentity.EntityName = entityname
	} else {
		entityname, err := protocol.ReadNullableString(r)
		if err != nil {
			return describeclientquotasresponseentrieentity, err
		}
		describeclientquotasresponseentrieentity.EntityName = entityname
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeclientquotasresponseentrieentity, err
		}
		describeclientquotasresponseentrieentity.rawTaggedFields = &rawTaggedFields
	}

	return describeclientquotasresponseentrieentity, nil
}

func (res *DescribeClientQuotasResponse) valuesEncoder(w io.Writer, value DescribeClientQuotasResponseEntrieValue) error {
	// Key (versions: 0+)
	if value.Key == nil {
		return fmt.Errorf("DescribeClientQuotasResponseEntrieValue.Key must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Key); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Key); err != nil {
			return err
		}
	}

	// Value (versions: 0+)
	if err := protocol.WriteFloat64(w, value.Value); err != nil {
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

func (res *DescribeClientQuotasResponse) valuesDecoder(r io.Reader) (DescribeClientQuotasResponseEntrieValue, error) {
	describeclientquotasresponseentrievalue := DescribeClientQuotasResponseEntrieValue{}

	// Key (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		key, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeclientquotasresponseentrievalue, err
		}
		describeclientquotasresponseentrievalue.Key = &key
	} else {
		key, err := protocol.ReadString(r)
		if err != nil {
			return describeclientquotasresponseentrievalue, err
		}
		describeclientquotasresponseentrievalue.Key = &key
	}

	// Value (versions: 0+)
	value, err := protocol.ReadFloat64(r)
	if err != nil {
		return describeclientquotasresponseentrievalue, err
	}
	describeclientquotasresponseentrievalue.Value = value

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeclientquotasresponseentrievalue, err
		}
		describeclientquotasresponseentrievalue.rawTaggedFields = &rawTaggedFields
	}

	return describeclientquotasresponseentrievalue, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeClientQuotasResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeClientQuotasResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.ErrorMessage != nil {
		fmt.Fprintf(w, "        ErrorMessage: %v\n", *res.ErrorMessage)
	} else {
		fmt.Fprintf(w, "        ErrorMessage: nil\n")
	}

	if res.Entries != nil {
		fmt.Fprintf(w, "        Entries:\n")
		for _, entries := range *res.Entries {
			fmt.Fprintf(w, "%s", entries.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Entries: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeClientQuotasResponseEntrie) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Entity != nil {
		fmt.Fprintf(w, "            Entity:\n")
		for _, entity := range *value.Entity {
			fmt.Fprintf(w, "%s", entity.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Entity: nil\n")
	}

	if value.Values != nil {
		fmt.Fprintf(w, "            Values:\n")
		for _, values := range *value.Values {
			fmt.Fprintf(w, "%s", values.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Values: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeClientQuotasResponseEntrieEntity) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.EntityType != nil {
		fmt.Fprintf(w, "                EntityType: %v\n", *value.EntityType)
	} else {
		fmt.Fprintf(w, "                EntityType: nil\n")
	}

	if value.EntityName != nil {
		fmt.Fprintf(w, "                EntityName: %v\n", *value.EntityName)
	} else {
		fmt.Fprintf(w, "                EntityName: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeClientQuotasResponseEntrieValue) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Key != nil {
		fmt.Fprintf(w, "                Key: %v\n", *value.Key)
	} else {
		fmt.Fprintf(w, "                Key: nil\n")
	}

	fmt.Fprintf(w, "                Value: %v\n", value.Value)

	return w.String()
}
