package saslauthenticate

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type SaslAuthenticateResponse struct {
	ApiVersion        int16
	ErrorCode         int16   // The error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage      *string // The error message, or null if there was no error. (versions: 0+, nullable: 0+)
	AuthBytes         *[]byte // The SASL authentication bytes from the server, as defined by the SASL mechanism. (versions: 0+)
	SessionLifetimeMs int64   // Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur. (versions: 1+)
	rawTaggedFields   *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (res *SaslAuthenticateResponse) Write(w io.Writer) error {
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

	// AuthBytes (versions: 0+)
	if res.AuthBytes == nil {
		return fmt.Errorf("SaslAuthenticateResponse.AuthBytes must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *res.AuthBytes); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *res.AuthBytes); err != nil {
			return err
		}
	}

	// SessionLifetimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt64(w, res.SessionLifetimeMs); err != nil {
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
func (res *SaslAuthenticateResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("SaslAuthenticateResponse.Read: response or its body is nil")
	}

	*res = SaslAuthenticateResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

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

	// AuthBytes (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		authbytes, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return err
		}
		res.AuthBytes = &authbytes
	} else {
		authbytes, err := protocol.ReadBytes(r)
		if err != nil {
			return err
		}
		res.AuthBytes = &authbytes
	}

	// SessionLifetimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		sessionlifetimems, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		res.SessionLifetimeMs = sessionlifetimems
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

//goland:noinspection GoUnhandledErrorResult
func (res *SaslAuthenticateResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- SaslAuthenticateResponse:\n")
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.ErrorMessage != nil {
		fmt.Fprintf(w, "        ErrorMessage: %v\n", *res.ErrorMessage)
	} else {
		fmt.Fprintf(w, "        ErrorMessage: nil\n")
	}

	if res.AuthBytes != nil {
		fmt.Fprintf(w, "        AuthBytes: <%d bytes>\n", len(*res.AuthBytes))
	} else {
		fmt.Fprintf(w, "        AuthBytes: nil\n")
	}

	fmt.Fprintf(w, "        SessionLifetimeMs: %v\n", res.SessionLifetimeMs)

	return w.String()
}
