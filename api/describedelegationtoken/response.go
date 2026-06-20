package describedelegationtoken

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeDelegationTokenResponse struct {
	ApiVersion      int16
	ErrorCode       int16                                   // The error code, or 0 if there was no error. (versions: 0+)
	Tokens          *[]DescribeDelegationTokenResponseToken // The tokens. (versions: 0+)
	ThrottleTimeMs  int32                                   // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeDelegationTokenResponseToken struct {
	PrincipalType               *string                                        // The token principal type. (versions: 0+)
	PrincipalName               *string                                        // The token principal name. (versions: 0+)
	TokenRequesterPrincipalType *string                                        // The principal type of the requester of the token. (versions: 3+)
	TokenRequesterPrincipalName *string                                        // The principal type of the requester of the token. (versions: 3+)
	IssueTimestamp              int64                                          // The token issue timestamp in milliseconds. (versions: 0+)
	ExpiryTimestamp             int64                                          // The token expiry timestamp in milliseconds. (versions: 0+)
	MaxTimestamp                int64                                          // The token maximum timestamp length in milliseconds. (versions: 0+)
	TokenId                     *string                                        // The token ID. (versions: 0+)
	Hmac                        *[]byte                                        // The token HMAC. (versions: 0+)
	Renewers                    *[]DescribeDelegationTokenResponseTokenRenewer // Those who are able to renew this token before it expires. (versions: 0+)
	rawTaggedFields             *[]protocol.TaggedField
}

type DescribeDelegationTokenResponseTokenRenewer struct {
	PrincipalType   *string // The renewer principal type. (versions: 0+)
	PrincipalName   *string // The renewer principal name. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (res *DescribeDelegationTokenResponse) Write(w io.Writer) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// Tokens (versions: 0+)
	if res.Tokens == nil {
		return fmt.Errorf("DescribeDelegationTokenResponse.Tokens must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.tokensEncoder, res.Tokens); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.tokensEncoder, *res.Tokens); err != nil {
			return err
		}
	}

	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
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
func (res *DescribeDelegationTokenResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeDelegationTokenResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// Tokens (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		tokens, err := protocol.ReadNullableCompactArray(r, res.tokensDecoder)
		if err != nil {
			return err
		}
		res.Tokens = tokens
	} else {
		tokens, err := protocol.ReadArray(r, res.tokensDecoder)
		if err != nil {
			return err
		}
		res.Tokens = &tokens
	}

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

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

func (res *DescribeDelegationTokenResponse) tokensEncoder(w io.Writer, value DescribeDelegationTokenResponseToken) error {
	// PrincipalType (versions: 0+)
	if value.PrincipalType == nil {
		return fmt.Errorf("DescribeDelegationTokenResponseToken.PrincipalType must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
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
		return fmt.Errorf("DescribeDelegationTokenResponseToken.PrincipalName must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.PrincipalName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.PrincipalName); err != nil {
			return err
		}
	}

	// TokenRequesterPrincipalType (versions: 3+)
	if res.ApiVersion >= 3 {
		if value.TokenRequesterPrincipalType == nil {
			return fmt.Errorf("DescribeDelegationTokenResponseToken.TokenRequesterPrincipalType must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.TokenRequesterPrincipalType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.TokenRequesterPrincipalType); err != nil {
				return err
			}
		}
	}

	// TokenRequesterPrincipalName (versions: 3+)
	if res.ApiVersion >= 3 {
		if value.TokenRequesterPrincipalName == nil {
			return fmt.Errorf("DescribeDelegationTokenResponseToken.TokenRequesterPrincipalName must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.TokenRequesterPrincipalName); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.TokenRequesterPrincipalName); err != nil {
				return err
			}
		}
	}

	// IssueTimestamp (versions: 0+)
	if err := protocol.WriteInt64(w, value.IssueTimestamp); err != nil {
		return err
	}

	// ExpiryTimestamp (versions: 0+)
	if err := protocol.WriteInt64(w, value.ExpiryTimestamp); err != nil {
		return err
	}

	// MaxTimestamp (versions: 0+)
	if err := protocol.WriteInt64(w, value.MaxTimestamp); err != nil {
		return err
	}

	// TokenId (versions: 0+)
	if value.TokenId == nil {
		return fmt.Errorf("DescribeDelegationTokenResponseToken.TokenId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.TokenId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.TokenId); err != nil {
			return err
		}
	}

	// Hmac (versions: 0+)
	if value.Hmac == nil {
		return fmt.Errorf("DescribeDelegationTokenResponseToken.Hmac must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *value.Hmac); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *value.Hmac); err != nil {
			return err
		}
	}

	// Renewers (versions: 0+)
	if value.Renewers == nil {
		return fmt.Errorf("DescribeDelegationTokenResponseToken.Renewers must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.renewersEncoder, value.Renewers); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.renewersEncoder, *value.Renewers); err != nil {
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

func (res *DescribeDelegationTokenResponse) tokensDecoder(r io.Reader) (DescribeDelegationTokenResponseToken, error) {
	describedelegationtokenresponsetoken := DescribeDelegationTokenResponseToken{}

	// PrincipalType (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		principaltype, err := protocol.ReadCompactString(r)
		if err != nil {
			return describedelegationtokenresponsetoken, err
		}
		describedelegationtokenresponsetoken.PrincipalType = &principaltype
	} else {
		principaltype, err := protocol.ReadString(r)
		if err != nil {
			return describedelegationtokenresponsetoken, err
		}
		describedelegationtokenresponsetoken.PrincipalType = &principaltype
	}

	// PrincipalName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		principalname, err := protocol.ReadCompactString(r)
		if err != nil {
			return describedelegationtokenresponsetoken, err
		}
		describedelegationtokenresponsetoken.PrincipalName = &principalname
	} else {
		principalname, err := protocol.ReadString(r)
		if err != nil {
			return describedelegationtokenresponsetoken, err
		}
		describedelegationtokenresponsetoken.PrincipalName = &principalname
	}

	// TokenRequesterPrincipalType (versions: 3+)
	if res.ApiVersion >= 3 {
		if isResponseFlexible(res.ApiVersion) {
			tokenrequesterprincipaltype, err := protocol.ReadCompactString(r)
			if err != nil {
				return describedelegationtokenresponsetoken, err
			}
			describedelegationtokenresponsetoken.TokenRequesterPrincipalType = &tokenrequesterprincipaltype
		} else {
			tokenrequesterprincipaltype, err := protocol.ReadString(r)
			if err != nil {
				return describedelegationtokenresponsetoken, err
			}
			describedelegationtokenresponsetoken.TokenRequesterPrincipalType = &tokenrequesterprincipaltype
		}
	}

	// TokenRequesterPrincipalName (versions: 3+)
	if res.ApiVersion >= 3 {
		if isResponseFlexible(res.ApiVersion) {
			tokenrequesterprincipalname, err := protocol.ReadCompactString(r)
			if err != nil {
				return describedelegationtokenresponsetoken, err
			}
			describedelegationtokenresponsetoken.TokenRequesterPrincipalName = &tokenrequesterprincipalname
		} else {
			tokenrequesterprincipalname, err := protocol.ReadString(r)
			if err != nil {
				return describedelegationtokenresponsetoken, err
			}
			describedelegationtokenresponsetoken.TokenRequesterPrincipalName = &tokenrequesterprincipalname
		}
	}

	// IssueTimestamp (versions: 0+)
	issuetimestamp, err := protocol.ReadInt64(r)
	if err != nil {
		return describedelegationtokenresponsetoken, err
	}
	describedelegationtokenresponsetoken.IssueTimestamp = issuetimestamp

	// ExpiryTimestamp (versions: 0+)
	expirytimestamp, err := protocol.ReadInt64(r)
	if err != nil {
		return describedelegationtokenresponsetoken, err
	}
	describedelegationtokenresponsetoken.ExpiryTimestamp = expirytimestamp

	// MaxTimestamp (versions: 0+)
	maxtimestamp, err := protocol.ReadInt64(r)
	if err != nil {
		return describedelegationtokenresponsetoken, err
	}
	describedelegationtokenresponsetoken.MaxTimestamp = maxtimestamp

	// TokenId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		tokenid, err := protocol.ReadCompactString(r)
		if err != nil {
			return describedelegationtokenresponsetoken, err
		}
		describedelegationtokenresponsetoken.TokenId = &tokenid
	} else {
		tokenid, err := protocol.ReadString(r)
		if err != nil {
			return describedelegationtokenresponsetoken, err
		}
		describedelegationtokenresponsetoken.TokenId = &tokenid
	}

	// Hmac (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		hmac, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return describedelegationtokenresponsetoken, err
		}
		describedelegationtokenresponsetoken.Hmac = &hmac
	} else {
		hmac, err := protocol.ReadBytes(r)
		if err != nil {
			return describedelegationtokenresponsetoken, err
		}
		describedelegationtokenresponsetoken.Hmac = &hmac
	}

	// Renewers (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		renewers, err := protocol.ReadNullableCompactArray(r, res.renewersDecoder)
		if err != nil {
			return describedelegationtokenresponsetoken, err
		}
		describedelegationtokenresponsetoken.Renewers = renewers
	} else {
		renewers, err := protocol.ReadArray(r, res.renewersDecoder)
		if err != nil {
			return describedelegationtokenresponsetoken, err
		}
		describedelegationtokenresponsetoken.Renewers = &renewers
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describedelegationtokenresponsetoken, err
		}
		describedelegationtokenresponsetoken.rawTaggedFields = &rawTaggedFields
	}

	return describedelegationtokenresponsetoken, nil
}

func (res *DescribeDelegationTokenResponse) renewersEncoder(w io.Writer, value DescribeDelegationTokenResponseTokenRenewer) error {
	// PrincipalType (versions: 0+)
	if value.PrincipalType == nil {
		return fmt.Errorf("DescribeDelegationTokenResponseTokenRenewer.PrincipalType must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
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
		return fmt.Errorf("DescribeDelegationTokenResponseTokenRenewer.PrincipalName must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.PrincipalName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.PrincipalName); err != nil {
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

func (res *DescribeDelegationTokenResponse) renewersDecoder(r io.Reader) (DescribeDelegationTokenResponseTokenRenewer, error) {
	describedelegationtokenresponsetokenrenewer := DescribeDelegationTokenResponseTokenRenewer{}

	// PrincipalType (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		principaltype, err := protocol.ReadCompactString(r)
		if err != nil {
			return describedelegationtokenresponsetokenrenewer, err
		}
		describedelegationtokenresponsetokenrenewer.PrincipalType = &principaltype
	} else {
		principaltype, err := protocol.ReadString(r)
		if err != nil {
			return describedelegationtokenresponsetokenrenewer, err
		}
		describedelegationtokenresponsetokenrenewer.PrincipalType = &principaltype
	}

	// PrincipalName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		principalname, err := protocol.ReadCompactString(r)
		if err != nil {
			return describedelegationtokenresponsetokenrenewer, err
		}
		describedelegationtokenresponsetokenrenewer.PrincipalName = &principalname
	} else {
		principalname, err := protocol.ReadString(r)
		if err != nil {
			return describedelegationtokenresponsetokenrenewer, err
		}
		describedelegationtokenresponsetokenrenewer.PrincipalName = &principalname
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describedelegationtokenresponsetokenrenewer, err
		}
		describedelegationtokenresponsetokenrenewer.rawTaggedFields = &rawTaggedFields
	}

	return describedelegationtokenresponsetokenrenewer, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeDelegationTokenResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeDelegationTokenResponse:\n")
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.Tokens != nil {
		fmt.Fprintf(w, "        Tokens:\n")
		for _, tokens := range *res.Tokens {
			fmt.Fprintf(w, "%s", tokens.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Tokens: nil\n")
	}

	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeDelegationTokenResponseToken) PrettyPrint() string {
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

	if value.TokenRequesterPrincipalType != nil {
		fmt.Fprintf(w, "            TokenRequesterPrincipalType: %v\n", *value.TokenRequesterPrincipalType)
	} else {
		fmt.Fprintf(w, "            TokenRequesterPrincipalType: nil\n")
	}

	if value.TokenRequesterPrincipalName != nil {
		fmt.Fprintf(w, "            TokenRequesterPrincipalName: %v\n", *value.TokenRequesterPrincipalName)
	} else {
		fmt.Fprintf(w, "            TokenRequesterPrincipalName: nil\n")
	}

	fmt.Fprintf(w, "            IssueTimestamp: %v\n", value.IssueTimestamp)
	fmt.Fprintf(w, "            ExpiryTimestamp: %v\n", value.ExpiryTimestamp)
	fmt.Fprintf(w, "            MaxTimestamp: %v\n", value.MaxTimestamp)

	if value.TokenId != nil {
		fmt.Fprintf(w, "            TokenId: %v\n", *value.TokenId)
	} else {
		fmt.Fprintf(w, "            TokenId: nil\n")
	}

	if value.Hmac != nil {
		fmt.Fprintf(w, "            Hmac: <%d bytes>\n", len(*value.Hmac))
	} else {
		fmt.Fprintf(w, "            Hmac: nil\n")
	}

	if value.Renewers != nil {
		fmt.Fprintf(w, "            Renewers:\n")
		for _, renewers := range *value.Renewers {
			fmt.Fprintf(w, "%s", renewers.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Renewers: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeDelegationTokenResponseTokenRenewer) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.PrincipalType != nil {
		fmt.Fprintf(w, "                PrincipalType: %v\n", *value.PrincipalType)
	} else {
		fmt.Fprintf(w, "                PrincipalType: nil\n")
	}

	if value.PrincipalName != nil {
		fmt.Fprintf(w, "                PrincipalName: %v\n", *value.PrincipalName)
	} else {
		fmt.Fprintf(w, "                PrincipalName: nil\n")
	}

	return w.String()
}
