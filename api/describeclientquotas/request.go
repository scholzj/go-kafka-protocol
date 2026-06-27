package describeclientquotas

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeClientQuotasRequest struct {
	ApiVersion      int16
	Components      *[]DescribeClientQuotasRequestComponent // Filter components to apply to quota entities. (versions: 0+)
	Strict          bool                                    // Whether the match is strict, i.e. should exclude entities with unspecified entity types. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeClientQuotasRequestComponent struct {
	EntityType      *string // The entity type that the filter component applies to. (versions: 0+)
	MatchType       int8    // How to match the entity {0 = exact name, 1 = default name, 2 = any specified name}. (versions: 0+)
	Match           *string // The string to match against, or null if unused for the match type. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 1
}

func (req *DescribeClientQuotasRequest) Write(w io.Writer) error {
	// Components (versions: 0+)
	if req.Components == nil {
		return fmt.Errorf("DescribeClientQuotasRequest.Components must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.componentsEncoder, req.Components); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.componentsEncoder, *req.Components); err != nil {
			return err
		}
	}

	// Strict (versions: 0+)
	if err := protocol.WriteBool(w, req.Strict); err != nil {
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
func (req *DescribeClientQuotasRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeClientQuotasRequest.Read: request or its body is nil")
	}

	*req = DescribeClientQuotasRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Components (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		components, err := protocol.ReadCompactArray(r, req.componentsDecoder)
		if err != nil {
			return err
		}
		req.Components = &components
	} else {
		components, err := protocol.ReadArray(r, req.componentsDecoder)
		if err != nil {
			return err
		}
		req.Components = &components
	}

	// Strict (versions: 0+)
	strict, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	req.Strict = strict

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

func (req *DescribeClientQuotasRequest) componentsEncoder(w io.Writer, value DescribeClientQuotasRequestComponent) error {
	// EntityType (versions: 0+)
	if value.EntityType == nil {
		return fmt.Errorf("DescribeClientQuotasRequestComponent.EntityType must not be nil in version %d", req.ApiVersion)
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

	// MatchType (versions: 0+)
	if err := protocol.WriteInt8(w, value.MatchType); err != nil {
		return err
	}

	// Match (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Match); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.Match); err != nil {
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

func (req *DescribeClientQuotasRequest) componentsDecoder(r io.Reader) (DescribeClientQuotasRequestComponent, error) {
	describeclientquotasrequestcomponent := DescribeClientQuotasRequestComponent{}

	// EntityType (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		entitytype, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeclientquotasrequestcomponent, err
		}
		describeclientquotasrequestcomponent.EntityType = &entitytype
	} else {
		entitytype, err := protocol.ReadString(r)
		if err != nil {
			return describeclientquotasrequestcomponent, err
		}
		describeclientquotasrequestcomponent.EntityType = &entitytype
	}

	// MatchType (versions: 0+)
	matchtype, err := protocol.ReadInt8(r)
	if err != nil {
		return describeclientquotasrequestcomponent, err
	}
	describeclientquotasrequestcomponent.MatchType = matchtype

	// Match (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		match, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return describeclientquotasrequestcomponent, err
		}
		describeclientquotasrequestcomponent.Match = match
	} else {
		match, err := protocol.ReadNullableString(r)
		if err != nil {
			return describeclientquotasrequestcomponent, err
		}
		describeclientquotasrequestcomponent.Match = match
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeclientquotasrequestcomponent, err
		}
		describeclientquotasrequestcomponent.rawTaggedFields = &rawTaggedFields
	}

	return describeclientquotasrequestcomponent, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DescribeClientQuotasRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeClientQuotasRequest:\n")

	if req.Components != nil {
		fmt.Fprintf(w, "        Components:\n")
		for _, components := range *req.Components {
			fmt.Fprintf(w, "%s", components.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Components: nil\n")
	}

	fmt.Fprintf(w, "        Strict: %v\n", req.Strict)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeClientQuotasRequestComponent) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.EntityType != nil {
		fmt.Fprintf(w, "            EntityType: %v\n", *value.EntityType)
	} else {
		fmt.Fprintf(w, "            EntityType: nil\n")
	}

	fmt.Fprintf(w, "            MatchType: %v\n", value.MatchType)

	if value.Match != nil {
		fmt.Fprintf(w, "            Match: %v\n", *value.Match)
	} else {
		fmt.Fprintf(w, "            Match: nil\n")
	}

	return w.String()
}
