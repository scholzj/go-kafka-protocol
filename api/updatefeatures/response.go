package updatefeatures

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type UpdateFeaturesResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                           // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                           // The top-level error code, or `0` if there was no top-level error. (versions: 0+)
	ErrorMessage    *string                         // The top-level error message, or `null` if there was no top-level error. (versions: 0+, nullable: 0+)
	Results         *[]UpdateFeaturesResponseResult // Results for each feature update. (versions: 0-1)
	rawTaggedFields *[]protocol.TaggedField
}

type UpdateFeaturesResponseResult struct {
	Feature         *string // The name of the finalized feature. (versions: 0+)
	ErrorCode       int16   // The feature update error code or `0` if the feature update succeeded. (versions: 0+)
	ErrorMessage    *string // The feature update error, or `null` if the feature update succeeded. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *UpdateFeaturesResponse) Write(w io.Writer) error {
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

	// Results (versions: 0-1)
	if res.ApiVersion <= 1 {
		if res.Results == nil {
			return fmt.Errorf("UpdateFeaturesResponse.Results must not be nil in version %d", res.ApiVersion)
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
func (res *UpdateFeaturesResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("UpdateFeaturesResponse.Read: response or its body is nil")
	}

	*res = UpdateFeaturesResponse{}

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

	// Results (versions: 0-1)
	if res.ApiVersion <= 1 {
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

func (res *UpdateFeaturesResponse) resultsEncoder(w io.Writer, value UpdateFeaturesResponseResult) error {
	// Feature (versions: 0+)
	if value.Feature == nil {
		return fmt.Errorf("UpdateFeaturesResponseResult.Feature must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Feature); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Feature); err != nil {
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

func (res *UpdateFeaturesResponse) resultsDecoder(r io.Reader) (UpdateFeaturesResponseResult, error) {
	updatefeaturesresponseresult := UpdateFeaturesResponseResult{}

	// Feature (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		feature, err := protocol.ReadCompactString(r)
		if err != nil {
			return updatefeaturesresponseresult, err
		}
		updatefeaturesresponseresult.Feature = &feature
	} else {
		feature, err := protocol.ReadString(r)
		if err != nil {
			return updatefeaturesresponseresult, err
		}
		updatefeaturesresponseresult.Feature = &feature
	}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return updatefeaturesresponseresult, err
	}
	updatefeaturesresponseresult.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return updatefeaturesresponseresult, err
		}
		updatefeaturesresponseresult.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return updatefeaturesresponseresult, err
		}
		updatefeaturesresponseresult.ErrorMessage = errormessage
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return updatefeaturesresponseresult, err
		}
		updatefeaturesresponseresult.rawTaggedFields = &rawTaggedFields
	}

	return updatefeaturesresponseresult, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *UpdateFeaturesResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- UpdateFeaturesResponse:\n")
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
func (value *UpdateFeaturesResponseResult) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Feature != nil {
		fmt.Fprintf(w, "            Feature: %v\n", *value.Feature)
	} else {
		fmt.Fprintf(w, "            Feature: nil\n")
	}

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	return w.String()
}
