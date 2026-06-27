package saslhandshake

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type SaslHandshakeResponse struct {
	ApiVersion      int16
	ErrorCode       int16     // The error code, or 0 if there was no error. (versions: 0+)
	Mechanisms      *[]string // The mechanisms enabled in the server. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func (res *SaslHandshakeResponse) Write(w io.Writer) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// Mechanisms (versions: 0+)
	if res.Mechanisms == nil {
		return fmt.Errorf("SaslHandshakeResponse.Mechanisms must not be nil in version %d", res.ApiVersion)
	}
	if err := protocol.WriteArray(w, protocol.WriteString, *res.Mechanisms); err != nil {
		return err
	}

	return nil
}

// TODO: pass version and bytes only
func (res *SaslHandshakeResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("SaslHandshakeResponse.Read: response or its body is nil")
	}

	*res = SaslHandshakeResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// Mechanisms (versions: 0+)
	mechanisms, err := protocol.ReadArray(r, protocol.ReadString)
	if err != nil {
		return err
	}
	res.Mechanisms = &mechanisms

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *SaslHandshakeResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- SaslHandshakeResponse:\n")
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.Mechanisms != nil {
		fmt.Fprintf(w, "        Mechanisms: %v\n", *res.Mechanisms)
	} else {
		fmt.Fprintf(w, "        Mechanisms: nil\n")
	}

	return w.String()
}
