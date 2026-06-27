package alterclientquotas

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AlterClientQuotasResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                              // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	Entries         *[]AlterClientQuotasResponseEntrie // The quota configuration entries to alter. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterClientQuotasResponseEntrie struct {
	ErrorCode       int16                                    // The error code, or `0` if the quota alteration succeeded. (versions: 0+)
	ErrorMessage    *string                                  // The error message, or `null` if the quota alteration succeeded. (versions: 0+, nullable: 0+)
	Entity          *[]AlterClientQuotasResponseEntrieEntity // The quota entity to alter. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterClientQuotasResponseEntrieEntity struct {
	EntityType      *string // The entity type. (versions: 0+)
	EntityName      *string // The name of the entity, or null if the default. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 1
}

func (res *AlterClientQuotasResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// Entries (versions: 0+)
	if res.Entries == nil {
		return fmt.Errorf("AlterClientQuotasResponse.Entries must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.entriesEncoder, res.Entries); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.entriesEncoder, *res.Entries); err != nil {
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
func (res *AlterClientQuotasResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("AlterClientQuotasResponse.Read: response or its body is nil")
	}

	*res = AlterClientQuotasResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// Entries (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		entries, err := protocol.ReadCompactArray(r, res.entriesDecoder)
		if err != nil {
			return err
		}
		res.Entries = &entries
	} else {
		entries, err := protocol.ReadArray(r, res.entriesDecoder)
		if err != nil {
			return err
		}
		res.Entries = &entries
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

func (res *AlterClientQuotasResponse) entriesEncoder(w io.Writer, value AlterClientQuotasResponseEntrie) error {
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

	// Entity (versions: 0+)
	if value.Entity == nil {
		return fmt.Errorf("AlterClientQuotasResponseEntrie.Entity must not be nil in version %d", res.ApiVersion)
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

func (res *AlterClientQuotasResponse) entriesDecoder(r io.Reader) (AlterClientQuotasResponseEntrie, error) {
	alterclientquotasresponseentrie := AlterClientQuotasResponseEntrie{}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return alterclientquotasresponseentrie, err
	}
	alterclientquotasresponseentrie.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return alterclientquotasresponseentrie, err
		}
		alterclientquotasresponseentrie.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return alterclientquotasresponseentrie, err
		}
		alterclientquotasresponseentrie.ErrorMessage = errormessage
	}

	// Entity (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		entity, err := protocol.ReadCompactArray(r, res.entityDecoder)
		if err != nil {
			return alterclientquotasresponseentrie, err
		}
		alterclientquotasresponseentrie.Entity = &entity
	} else {
		entity, err := protocol.ReadArray(r, res.entityDecoder)
		if err != nil {
			return alterclientquotasresponseentrie, err
		}
		alterclientquotasresponseentrie.Entity = &entity
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterclientquotasresponseentrie, err
		}
		alterclientquotasresponseentrie.rawTaggedFields = &rawTaggedFields
	}

	return alterclientquotasresponseentrie, nil
}

func (res *AlterClientQuotasResponse) entityEncoder(w io.Writer, value AlterClientQuotasResponseEntrieEntity) error {
	// EntityType (versions: 0+)
	if value.EntityType == nil {
		return fmt.Errorf("AlterClientQuotasResponseEntrieEntity.EntityType must not be nil in version %d", res.ApiVersion)
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

func (res *AlterClientQuotasResponse) entityDecoder(r io.Reader) (AlterClientQuotasResponseEntrieEntity, error) {
	alterclientquotasresponseentrieentity := AlterClientQuotasResponseEntrieEntity{}

	// EntityType (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		entitytype, err := protocol.ReadCompactString(r)
		if err != nil {
			return alterclientquotasresponseentrieentity, err
		}
		alterclientquotasresponseentrieentity.EntityType = &entitytype
	} else {
		entitytype, err := protocol.ReadString(r)
		if err != nil {
			return alterclientquotasresponseentrieentity, err
		}
		alterclientquotasresponseentrieentity.EntityType = &entitytype
	}

	// EntityName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		entityname, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return alterclientquotasresponseentrieentity, err
		}
		alterclientquotasresponseentrieentity.EntityName = entityname
	} else {
		entityname, err := protocol.ReadNullableString(r)
		if err != nil {
			return alterclientquotasresponseentrieentity, err
		}
		alterclientquotasresponseentrieentity.EntityName = entityname
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterclientquotasresponseentrieentity, err
		}
		alterclientquotasresponseentrieentity.rawTaggedFields = &rawTaggedFields
	}

	return alterclientquotasresponseentrieentity, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *AlterClientQuotasResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- AlterClientQuotasResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

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
func (value *AlterClientQuotasResponseEntrie) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	if value.Entity != nil {
		fmt.Fprintf(w, "            Entity:\n")
		for _, entity := range *value.Entity {
			fmt.Fprintf(w, "%s", entity.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Entity: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterClientQuotasResponseEntrieEntity) PrettyPrint() string {
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
