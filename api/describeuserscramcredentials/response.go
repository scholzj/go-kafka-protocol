package describeuserscramcredentials

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeUserScramCredentialsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                         // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                                         // The message-level error code, 0 except for user authorization or infrastructure issues. (versions: 0+)
	ErrorMessage    *string                                       // The message-level error message, if any. (versions: 0+, nullable: 0+)
	Results         *[]DescribeUserScramCredentialsResponseResult // The results for descriptions, one per user. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeUserScramCredentialsResponseResult struct {
	User            *string                                                     // The user name. (versions: 0+)
	ErrorCode       int16                                                       // The user-level error code. (versions: 0+)
	ErrorMessage    *string                                                     // The user-level error message, if any. (versions: 0+, nullable: 0+)
	CredentialInfos *[]DescribeUserScramCredentialsResponseResultCredentialInfo // The mechanism and related information associated with the user's SCRAM credentials. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeUserScramCredentialsResponseResultCredentialInfo struct {
	Mechanism       int8  // The SCRAM mechanism. (versions: 0+)
	Iterations      int32 // The number of iterations used in the SCRAM credential. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *DescribeUserScramCredentialsResponse) Write(w io.Writer) error {
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

	// Results (versions: 0+)
	if res.Results == nil {
		return fmt.Errorf("DescribeUserScramCredentialsResponse.Results must not be nil in version %d", res.ApiVersion)
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
func (res *DescribeUserScramCredentialsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeUserScramCredentialsResponse.Read: response or its body is nil")
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

	// Results (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		results, err := protocol.ReadNullableCompactArray(r, res.resultsDecoder)
		if err != nil {
			return err
		}
		res.Results = results
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

func (res *DescribeUserScramCredentialsResponse) resultsEncoder(w io.Writer, value DescribeUserScramCredentialsResponseResult) error {
	// User (versions: 0+)
	if value.User == nil {
		return fmt.Errorf("DescribeUserScramCredentialsResponseResult.User must not be nil in version %d", res.ApiVersion)
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

	// CredentialInfos (versions: 0+)
	if value.CredentialInfos == nil {
		return fmt.Errorf("DescribeUserScramCredentialsResponseResult.CredentialInfos must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.credentialInfosEncoder, value.CredentialInfos); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.credentialInfosEncoder, *value.CredentialInfos); err != nil {
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

func (res *DescribeUserScramCredentialsResponse) resultsDecoder(r io.Reader) (DescribeUserScramCredentialsResponseResult, error) {
	describeuserscramcredentialsresponseresult := DescribeUserScramCredentialsResponseResult{}

	// User (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		user, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeuserscramcredentialsresponseresult, err
		}
		describeuserscramcredentialsresponseresult.User = &user
	} else {
		user, err := protocol.ReadString(r)
		if err != nil {
			return describeuserscramcredentialsresponseresult, err
		}
		describeuserscramcredentialsresponseresult.User = &user
	}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return describeuserscramcredentialsresponseresult, err
	}
	describeuserscramcredentialsresponseresult.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return describeuserscramcredentialsresponseresult, err
		}
		describeuserscramcredentialsresponseresult.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return describeuserscramcredentialsresponseresult, err
		}
		describeuserscramcredentialsresponseresult.ErrorMessage = errormessage
	}

	// CredentialInfos (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		credentialinfos, err := protocol.ReadNullableCompactArray(r, res.credentialInfosDecoder)
		if err != nil {
			return describeuserscramcredentialsresponseresult, err
		}
		describeuserscramcredentialsresponseresult.CredentialInfos = credentialinfos
	} else {
		credentialinfos, err := protocol.ReadArray(r, res.credentialInfosDecoder)
		if err != nil {
			return describeuserscramcredentialsresponseresult, err
		}
		describeuserscramcredentialsresponseresult.CredentialInfos = &credentialinfos
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeuserscramcredentialsresponseresult, err
		}
		describeuserscramcredentialsresponseresult.rawTaggedFields = &rawTaggedFields
	}

	return describeuserscramcredentialsresponseresult, nil
}

func (res *DescribeUserScramCredentialsResponse) credentialInfosEncoder(w io.Writer, value DescribeUserScramCredentialsResponseResultCredentialInfo) error {
	// Mechanism (versions: 0+)
	if err := protocol.WriteInt8(w, value.Mechanism); err != nil {
		return err
	}

	// Iterations (versions: 0+)
	if err := protocol.WriteInt32(w, value.Iterations); err != nil {
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

func (res *DescribeUserScramCredentialsResponse) credentialInfosDecoder(r io.Reader) (DescribeUserScramCredentialsResponseResultCredentialInfo, error) {
	describeuserscramcredentialsresponseresultcredentialinfo := DescribeUserScramCredentialsResponseResultCredentialInfo{}

	// Mechanism (versions: 0+)
	mechanism, err := protocol.ReadInt8(r)
	if err != nil {
		return describeuserscramcredentialsresponseresultcredentialinfo, err
	}
	describeuserscramcredentialsresponseresultcredentialinfo.Mechanism = mechanism

	// Iterations (versions: 0+)
	iterations, err := protocol.ReadInt32(r)
	if err != nil {
		return describeuserscramcredentialsresponseresultcredentialinfo, err
	}
	describeuserscramcredentialsresponseresultcredentialinfo.Iterations = iterations

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeuserscramcredentialsresponseresultcredentialinfo, err
		}
		describeuserscramcredentialsresponseresultcredentialinfo.rawTaggedFields = &rawTaggedFields
	}

	return describeuserscramcredentialsresponseresultcredentialinfo, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeUserScramCredentialsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeUserScramCredentialsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.ErrorMessage != nil {
		fmt.Fprintf(w, "        ErrorMessage: %v\n", *res.ErrorMessage)
	} else {
		fmt.Fprintf(w, "        ErrorMessage: nil\n")
	}

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
func (value *DescribeUserScramCredentialsResponseResult) PrettyPrint() string {
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

	if value.CredentialInfos != nil {
		fmt.Fprintf(w, "            CredentialInfos:\n")
		for _, credentialinfos := range *value.CredentialInfos {
			fmt.Fprintf(w, "%s", credentialinfos.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            CredentialInfos: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeUserScramCredentialsResponseResultCredentialInfo) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Mechanism: %v\n", value.Mechanism)
	fmt.Fprintf(w, "                Iterations: %v\n", value.Iterations)

	return w.String()
}
