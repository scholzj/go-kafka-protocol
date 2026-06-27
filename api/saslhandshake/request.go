package saslhandshake

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type SaslHandshakeRequest struct {
	ApiVersion      int16
	Mechanism       *string // The SASL mechanism chosen by the client. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func (req *SaslHandshakeRequest) Write(w io.Writer) error {
	// Mechanism (versions: 0+)
	if req.Mechanism == nil {
		return fmt.Errorf("SaslHandshakeRequest.Mechanism must not be nil in version %d", req.ApiVersion)
	}
	if err := protocol.WriteString(w, *req.Mechanism); err != nil {
		return err
	}

	return nil
}

// TODO: pass version and bytes only
func (req *SaslHandshakeRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("SaslHandshakeRequest.Read: request or its body is nil")
	}

	*req = SaslHandshakeRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Mechanism (versions: 0+)
	mechanism, err := protocol.ReadString(r)
	if err != nil {
		return err
	}
	req.Mechanism = &mechanism

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *SaslHandshakeRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> SaslHandshakeRequest:\n")

	if req.Mechanism != nil {
		fmt.Fprintf(w, "        Mechanism: %v\n", *req.Mechanism)
	} else {
		fmt.Fprintf(w, "        Mechanism: nil\n")
	}

	return w.String()
}
