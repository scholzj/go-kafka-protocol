package brokerheartbeat

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type BrokerHeartbeatResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32 // Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16 // The error code, or 0 if there was no error. (versions: 0+)
	IsCaughtUp      bool  // True if the broker has approximately caught up with the latest metadata. (versions: 0+)
	IsFenced        bool  // True if the broker is fenced. (versions: 0+)
	ShouldShutDown  bool  // True if the broker should proceed with its shutdown. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *BrokerHeartbeatResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// IsCaughtUp (versions: 0+)
	if err := protocol.WriteBool(w, res.IsCaughtUp); err != nil {
		return err
	}

	// IsFenced (versions: 0+)
	if err := protocol.WriteBool(w, res.IsFenced); err != nil {
		return err
	}

	// ShouldShutDown (versions: 0+)
	if err := protocol.WriteBool(w, res.ShouldShutDown); err != nil {
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
func (res *BrokerHeartbeatResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("BrokerHeartbeatResponse.Read: response or its body is nil")
	}

	*res = BrokerHeartbeatResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	res.IsFenced = true

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

	// IsCaughtUp (versions: 0+)
	iscaughtup, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	res.IsCaughtUp = iscaughtup

	// IsFenced (versions: 0+)
	isfenced, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	res.IsFenced = isfenced

	// ShouldShutDown (versions: 0+)
	shouldshutdown, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	res.ShouldShutDown = shouldshutdown

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
func (res *BrokerHeartbeatResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- BrokerHeartbeatResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
	fmt.Fprintf(w, "        IsCaughtUp: %v\n", res.IsCaughtUp)
	fmt.Fprintf(w, "        IsFenced: %v\n", res.IsFenced)
	fmt.Fprintf(w, "        ShouldShutDown: %v\n", res.ShouldShutDown)

	return w.String()
}
