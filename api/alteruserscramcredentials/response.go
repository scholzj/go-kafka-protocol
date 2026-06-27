package alteruserscramcredentials

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AlterUserScramCredentialsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                      // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	Results         *[]AlterUserScramCredentialsResponseResult // The results for deletions and alterations, one per affected user. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterUserScramCredentialsResponseResult struct {
	User            *string // The user name. (versions: 0+)
	ErrorCode       int16   // The error code. (versions: 0+)
	ErrorMessage    *string // The error message, if any. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *AlterUserScramCredentialsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// Results (versions: 0+)
	if res.Results == nil {
		return fmt.Errorf("AlterUserScramCredentialsResponse.Results must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.resultsEncoder, res.Results); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.resultsEncoder, *res.Results); err != nil {
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
func (res *AlterUserScramCredentialsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("AlterUserScramCredentialsResponse.Read: response or its body is nil")
	}

	*res = AlterUserScramCredentialsResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// Results (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		results, err := protocol.ReadCompactArray(r, res.resultsDecoder)
		if err != nil {
			return err
		}
		res.Results = &results
	} else {
		results, err := protocol.ReadArray(r, res.resultsDecoder)
		if err != nil {
			return err
		}
		res.Results = &results
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

func (res *AlterUserScramCredentialsResponse) resultsEncoder(w io.Writer, value AlterUserScramCredentialsResponseResult) error {
	// User (versions: 0+)
	if value.User == nil {
		return fmt.Errorf("AlterUserScramCredentialsResponseResult.User must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.User); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.User); err != nil {
			return err
		}
	}

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

func (res *AlterUserScramCredentialsResponse) resultsDecoder(r io.Reader) (AlterUserScramCredentialsResponseResult, error) {
	alteruserscramcredentialsresponseresult := AlterUserScramCredentialsResponseResult{}

	// User (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		user, err := protocol.ReadCompactString(r)
		if err != nil {
			return alteruserscramcredentialsresponseresult, err
		}
		alteruserscramcredentialsresponseresult.User = &user
	} else {
		user, err := protocol.ReadString(r)
		if err != nil {
			return alteruserscramcredentialsresponseresult, err
		}
		alteruserscramcredentialsresponseresult.User = &user
	}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return alteruserscramcredentialsresponseresult, err
	}
	alteruserscramcredentialsresponseresult.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return alteruserscramcredentialsresponseresult, err
		}
		alteruserscramcredentialsresponseresult.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return alteruserscramcredentialsresponseresult, err
		}
		alteruserscramcredentialsresponseresult.ErrorMessage = errormessage
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alteruserscramcredentialsresponseresult, err
		}
		alteruserscramcredentialsresponseresult.rawTaggedFields = &rawTaggedFields
	}

	return alteruserscramcredentialsresponseresult, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *AlterUserScramCredentialsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- AlterUserScramCredentialsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	if res.Results != nil {
		fmt.Fprintf(w, "        Results:\n")
		for _, results := range *res.Results {
			fmt.Fprintf(w, "%s", results.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Results: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterUserScramCredentialsResponseResult) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.User != nil {
		fmt.Fprintf(w, "            User: %v\n", *value.User)
	} else {
		fmt.Fprintf(w, "            User: nil\n")
	}

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	return w.String()
}
