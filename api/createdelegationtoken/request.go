package createdelegationtoken

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type CreateDelegationTokenRequest struct {
	ApiVersion         int16
	OwnerPrincipalType *string                                // The principal type of the owner of the token. If it's null it defaults to the token request principal. (versions: 3+, nullable: 3+)
	OwnerPrincipalName *string                                // The principal name of the owner of the token. If it's null it defaults to the token request principal. (versions: 3+, nullable: 3+)
	Renewers           *[]CreateDelegationTokenRequestRenewer // A list of those who are allowed to renew this token before it expires. (versions: 0+)
	MaxLifetimeMs      int64                                  // The maximum lifetime of the token in milliseconds, or -1 to use the server side default. (versions: 0+)
	rawTaggedFields    *[]protocol.TaggedField
}

type CreateDelegationTokenRequestRenewer struct {
	PrincipalType   *string // The type of the Kafka principal. (versions: 0+)
	PrincipalName   *string // The name of the Kafka principal. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *CreateDelegationTokenRequest) Write(w io.Writer) error {
	// OwnerPrincipalType (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, req.OwnerPrincipalType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, req.OwnerPrincipalType); err != nil {
				return err
			}
		}
	}

	// OwnerPrincipalName (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, req.OwnerPrincipalName); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, req.OwnerPrincipalName); err != nil {
				return err
			}
		}
	}

	// Renewers (versions: 0+)
	if req.Renewers == nil {
		return fmt.Errorf("CreateDelegationTokenRequest.Renewers must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.renewersEncoder, req.Renewers); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.renewersEncoder, *req.Renewers); err != nil {
			return err
		}
	}

	// MaxLifetimeMs (versions: 0+)
	if err := protocol.WriteInt64(w, req.MaxLifetimeMs); err != nil {
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
func (req *CreateDelegationTokenRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("CreateDelegationTokenRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// OwnerPrincipalType (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			ownerprincipaltype, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			req.OwnerPrincipalType = ownerprincipaltype
		} else {
			ownerprincipaltype, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			req.OwnerPrincipalType = ownerprincipaltype
		}
	}

	// OwnerPrincipalName (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			ownerprincipalname, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			req.OwnerPrincipalName = ownerprincipalname
		} else {
			ownerprincipalname, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			req.OwnerPrincipalName = ownerprincipalname
		}
	}

	// Renewers (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		renewers, err := protocol.ReadNullableCompactArray(r, req.renewersDecoder)
		if err != nil {
			return err
		}
		req.Renewers = renewers
	} else {
		renewers, err := protocol.ReadArray(r, req.renewersDecoder)
		if err != nil {
			return err
		}
		req.Renewers = &renewers
	}

	// MaxLifetimeMs (versions: 0+)
	maxlifetimems, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	req.MaxLifetimeMs = maxlifetimems

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

func (req *CreateDelegationTokenRequest) renewersEncoder(w io.Writer, value CreateDelegationTokenRequestRenewer) error {
	// PrincipalType (versions: 0+)
	if value.PrincipalType == nil {
		return fmt.Errorf("CreateDelegationTokenRequestRenewer.PrincipalType must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.PrincipalType); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.PrincipalType); err != nil {
			return err
		}
	}

	// PrincipalName (versions: 0+)
	if value.PrincipalName == nil {
		return fmt.Errorf("CreateDelegationTokenRequestRenewer.PrincipalName must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.PrincipalName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.PrincipalName); err != nil {
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

func (req *CreateDelegationTokenRequest) renewersDecoder(r io.Reader) (CreateDelegationTokenRequestRenewer, error) {
	createdelegationtokenrequestrenewer := CreateDelegationTokenRequestRenewer{}

	// PrincipalType (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		principaltype, err := protocol.ReadCompactString(r)
		if err != nil {
			return createdelegationtokenrequestrenewer, err
		}
		createdelegationtokenrequestrenewer.PrincipalType = &principaltype
	} else {
		principaltype, err := protocol.ReadString(r)
		if err != nil {
			return createdelegationtokenrequestrenewer, err
		}
		createdelegationtokenrequestrenewer.PrincipalType = &principaltype
	}

	// PrincipalName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		principalname, err := protocol.ReadCompactString(r)
		if err != nil {
			return createdelegationtokenrequestrenewer, err
		}
		createdelegationtokenrequestrenewer.PrincipalName = &principalname
	} else {
		principalname, err := protocol.ReadString(r)
		if err != nil {
			return createdelegationtokenrequestrenewer, err
		}
		createdelegationtokenrequestrenewer.PrincipalName = &principalname
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return createdelegationtokenrequestrenewer, err
		}
		createdelegationtokenrequestrenewer.rawTaggedFields = &rawTaggedFields
	}

	return createdelegationtokenrequestrenewer, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *CreateDelegationTokenRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> CreateDelegationTokenRequest:\n")

	if req.OwnerPrincipalType != nil {
		fmt.Fprintf(w, "        OwnerPrincipalType: %v\n", *req.OwnerPrincipalType)
	} else {
		fmt.Fprintf(w, "        OwnerPrincipalType: nil\n")
	}

	if req.OwnerPrincipalName != nil {
		fmt.Fprintf(w, "        OwnerPrincipalName: %v\n", *req.OwnerPrincipalName)
	} else {
		fmt.Fprintf(w, "        OwnerPrincipalName: nil\n")
	}

	if req.Renewers != nil {
		fmt.Fprintf(w, "        Renewers:\n")
		for _, renewers := range *req.Renewers {
			fmt.Fprintf(w, "%s", renewers.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Renewers: nil\n")
	}

	fmt.Fprintf(w, "        MaxLifetimeMs: %v\n", req.MaxLifetimeMs)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *CreateDelegationTokenRequestRenewer) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.PrincipalType != nil {
		fmt.Fprintf(w, "            PrincipalType: %v\n", *value.PrincipalType)
	} else {
		fmt.Fprintf(w, "            PrincipalType: nil\n")
	}

	if value.PrincipalName != nil {
		fmt.Fprintf(w, "            PrincipalName: %v\n", *value.PrincipalName)
	} else {
		fmt.Fprintf(w, "            PrincipalName: nil\n")
	}

	return w.String()
}
