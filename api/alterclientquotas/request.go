package alterclientquotas

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AlterClientQuotasRequest struct {
	ApiVersion      int16
	Entries         *[]AlterClientQuotasRequestEntrie // The quota configuration entries to alter. (versions: 0+)
	ValidateOnly    bool                              // Whether the alteration should be validated, but not performed. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterClientQuotasRequestEntrie struct {
	Entity          *[]AlterClientQuotasRequestEntrieEntity // The quota entity to alter. (versions: 0+)
	Ops             *[]AlterClientQuotasRequestEntrieOp     // An individual quota configuration entry to alter. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterClientQuotasRequestEntrieEntity struct {
	EntityType      *string // The entity type. (versions: 0+)
	EntityName      *string // The name of the entity, or null if the default. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterClientQuotasRequestEntrieOp struct {
	Key             *string // The quota configuration key. (versions: 0+)
	Value           float64 // The value to set, otherwise ignored if the value is to be removed. (versions: 0+)
	Remove          bool    // Whether the quota configuration value should be removed, otherwise set. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 1
}

func (req *AlterClientQuotasRequest) Write(w io.Writer) error {
	// Entries (versions: 0+)
	if req.Entries == nil {
		return fmt.Errorf("AlterClientQuotasRequest.Entries must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.entriesEncoder, req.Entries); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.entriesEncoder, *req.Entries); err != nil {
			return err
		}
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
func (req *AlterClientQuotasRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("AlterClientQuotasRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Entries (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		entries, err := protocol.ReadNullableCompactArray(r, req.entriesDecoder)
		if err != nil {
			return err
		}
		req.Entries = entries
	} else {
		entries, err := protocol.ReadArray(r, req.entriesDecoder)
		if err != nil {
			return err
		}
		req.Entries = &entries
	}

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

func (req *AlterClientQuotasRequest) entriesEncoder(w io.Writer, value AlterClientQuotasRequestEntrie) error {
	// Entity (versions: 0+)
	if value.Entity == nil {
		return fmt.Errorf("AlterClientQuotasRequestEntrie.Entity must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.entityEncoder, value.Entity); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.entityEncoder, *value.Entity); err != nil {
			return err
		}
	}

	// Ops (versions: 0+)
	if value.Ops == nil {
		return fmt.Errorf("AlterClientQuotasRequestEntrie.Ops must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.opsEncoder, value.Ops); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.opsEncoder, *value.Ops); err != nil {
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

func (req *AlterClientQuotasRequest) entriesDecoder(r io.Reader) (AlterClientQuotasRequestEntrie, error) {
	alterclientquotasrequestentrie := AlterClientQuotasRequestEntrie{}

	// Entity (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		entity, err := protocol.ReadNullableCompactArray(r, req.entityDecoder)
		if err != nil {
			return alterclientquotasrequestentrie, err
		}
		alterclientquotasrequestentrie.Entity = entity
	} else {
		entity, err := protocol.ReadArray(r, req.entityDecoder)
		if err != nil {
			return alterclientquotasrequestentrie, err
		}
		alterclientquotasrequestentrie.Entity = &entity
	}

	// Ops (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		ops, err := protocol.ReadNullableCompactArray(r, req.opsDecoder)
		if err != nil {
			return alterclientquotasrequestentrie, err
		}
		alterclientquotasrequestentrie.Ops = ops
	} else {
		ops, err := protocol.ReadArray(r, req.opsDecoder)
		if err != nil {
			return alterclientquotasrequestentrie, err
		}
		alterclientquotasrequestentrie.Ops = &ops
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterclientquotasrequestentrie, err
		}
		alterclientquotasrequestentrie.rawTaggedFields = &rawTaggedFields
	}

	return alterclientquotasrequestentrie, nil
}

func (req *AlterClientQuotasRequest) entityEncoder(w io.Writer, value AlterClientQuotasRequestEntrieEntity) error {
	// EntityType (versions: 0+)
	if value.EntityType == nil {
		return fmt.Errorf("AlterClientQuotasRequestEntrieEntity.EntityType must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.EntityType); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.EntityType); err != nil {
			return err
		}
	}

	// EntityName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.EntityName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.EntityName); err != nil {
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

func (req *AlterClientQuotasRequest) entityDecoder(r io.Reader) (AlterClientQuotasRequestEntrieEntity, error) {
	alterclientquotasrequestentrieentity := AlterClientQuotasRequestEntrieEntity{}

	// EntityType (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		entitytype, err := protocol.ReadCompactString(r)
		if err != nil {
			return alterclientquotasrequestentrieentity, err
		}
		alterclientquotasrequestentrieentity.EntityType = &entitytype
	} else {
		entitytype, err := protocol.ReadString(r)
		if err != nil {
			return alterclientquotasrequestentrieentity, err
		}
		alterclientquotasrequestentrieentity.EntityType = &entitytype
	}

	// EntityName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		entityname, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return alterclientquotasrequestentrieentity, err
		}
		alterclientquotasrequestentrieentity.EntityName = entityname
	} else {
		entityname, err := protocol.ReadNullableString(r)
		if err != nil {
			return alterclientquotasrequestentrieentity, err
		}
		alterclientquotasrequestentrieentity.EntityName = entityname
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterclientquotasrequestentrieentity, err
		}
		alterclientquotasrequestentrieentity.rawTaggedFields = &rawTaggedFields
	}

	return alterclientquotasrequestentrieentity, nil
}

func (req *AlterClientQuotasRequest) opsEncoder(w io.Writer, value AlterClientQuotasRequestEntrieOp) error {
	// Key (versions: 0+)
	if value.Key == nil {
		return fmt.Errorf("AlterClientQuotasRequestEntrieOp.Key must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
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

	// Remove (versions: 0+)
	if err := protocol.WriteBool(w, value.Remove); err != nil {
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

func (req *AlterClientQuotasRequest) opsDecoder(r io.Reader) (AlterClientQuotasRequestEntrieOp, error) {
	alterclientquotasrequestentrieop := AlterClientQuotasRequestEntrieOp{}

	// Key (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		key, err := protocol.ReadCompactString(r)
		if err != nil {
			return alterclientquotasrequestentrieop, err
		}
		alterclientquotasrequestentrieop.Key = &key
	} else {
		key, err := protocol.ReadString(r)
		if err != nil {
			return alterclientquotasrequestentrieop, err
		}
		alterclientquotasrequestentrieop.Key = &key
	}

	// Value (versions: 0+)
	value, err := protocol.ReadFloat64(r)
	if err != nil {
		return alterclientquotasrequestentrieop, err
	}
	alterclientquotasrequestentrieop.Value = value

	// Remove (versions: 0+)
	remove, err := protocol.ReadBool(r)
	if err != nil {
		return alterclientquotasrequestentrieop, err
	}
	alterclientquotasrequestentrieop.Remove = remove

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterclientquotasrequestentrieop, err
		}
		alterclientquotasrequestentrieop.rawTaggedFields = &rawTaggedFields
	}

	return alterclientquotasrequestentrieop, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *AlterClientQuotasRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> AlterClientQuotasRequest:\n")

	if req.Entries != nil {
		fmt.Fprintf(w, "        Entries:\n")
		for _, entries := range *req.Entries {
			fmt.Fprintf(w, "%s", entries.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Entries: nil\n")
	}

	fmt.Fprintf(w, "        ValidateOnly: %v\n", req.ValidateOnly)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterClientQuotasRequestEntrie) PrettyPrint() string {
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

	if value.Ops != nil {
		fmt.Fprintf(w, "            Ops:\n")
		for _, ops := range *value.Ops {
			fmt.Fprintf(w, "%s", ops.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Ops: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterClientQuotasRequestEntrieEntity) PrettyPrint() string {
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
func (value *AlterClientQuotasRequestEntrieOp) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Key != nil {
		fmt.Fprintf(w, "                Key: %v\n", *value.Key)
	} else {
		fmt.Fprintf(w, "                Key: nil\n")
	}

	fmt.Fprintf(w, "                Value: %v\n", value.Value)
	fmt.Fprintf(w, "                Remove: %v\n", value.Remove)

	return w.String()
}
