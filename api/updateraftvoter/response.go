package updateraftvoter

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type UpdateRaftVoterResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                 // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                                 // The error code, or 0 if there was no error. (versions: 0+)
	CurrentLeader   *UpdateRaftVoterResponseCurrentLeader // tag 0: Details of the current Raft cluster leader. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type UpdateRaftVoterResponseCurrentLeader struct {
	LeaderId        int32   // The replica id of the current leader or -1 if the leader is unknown. (versions: 0+)
	LeaderEpoch     int32   // The latest known leader epoch. (versions: 0+)
	Host            *string // The node's hostname. (versions: 0+)
	Port            int32   // The node's port. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *UpdateRaftVoterResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		taggedFields, err := res.taggedFieldsEncoder()
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (res *UpdateRaftVoterResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("UpdateRaftVoterResponse.Read: response or its body is nil")
	}

	*res = UpdateRaftVoterResponse{}

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

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.ReadTaggedFields(r, res.taggedFieldsDecoder); err != nil {
			return err
		}
	}

	return nil
}

func (res *UpdateRaftVoterResponse) currentLeaderEncoder(w io.Writer, value UpdateRaftVoterResponseCurrentLeader) error {
	// LeaderId (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderId); err != nil {
		return err
	}

	// LeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
		return err
	}

	// Host (versions: 0+)
	if value.Host == nil {
		return fmt.Errorf("UpdateRaftVoterResponseCurrentLeader.Host must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Host); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Host); err != nil {
			return err
		}
	}

	// Port (versions: 0+)
	if err := protocol.WriteInt32(w, value.Port); err != nil {
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

func (res *UpdateRaftVoterResponse) currentLeaderDecoder(r io.Reader) (UpdateRaftVoterResponseCurrentLeader, error) {
	updateraftvoterresponsecurrentleader := UpdateRaftVoterResponseCurrentLeader{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	updateraftvoterresponsecurrentleader.LeaderId = -1
	updateraftvoterresponsecurrentleader.LeaderEpoch = -1

	// LeaderId (versions: 0+)
	leaderid, err := protocol.ReadInt32(r)
	if err != nil {
		return updateraftvoterresponsecurrentleader, err
	}
	updateraftvoterresponsecurrentleader.LeaderId = leaderid

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return updateraftvoterresponsecurrentleader, err
	}
	updateraftvoterresponsecurrentleader.LeaderEpoch = leaderepoch

	// Host (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return updateraftvoterresponsecurrentleader, err
		}
		updateraftvoterresponsecurrentleader.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return updateraftvoterresponsecurrentleader, err
		}
		updateraftvoterresponsecurrentleader.Host = &host
	}

	// Port (versions: 0+)
	port, err := protocol.ReadInt32(r)
	if err != nil {
		return updateraftvoterresponsecurrentleader, err
	}
	updateraftvoterresponsecurrentleader.Port = port

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return updateraftvoterresponsecurrentleader, err
		}
		updateraftvoterresponsecurrentleader.rawTaggedFields = &rawTaggedFields
	}

	return updateraftvoterresponsecurrentleader, nil
}

func (res *UpdateRaftVoterResponse) taggedFieldsEncoder() ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if res.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*res.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 1+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	if res.CurrentLeader != nil {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := res.currentLeaderEncoder(buf, *res.CurrentLeader); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})
	}

	// We append any raw tagged fields to the end of the array
	if res.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *res.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (res *UpdateRaftVoterResponse) taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {
	known := false

	switch tag {
	case 0:
		// CurrentLeader
		known = true
		currentleaderVal, err := res.currentLeaderDecoder(r)
		if err != nil {
			return err
		}
		res.CurrentLeader = &currentleaderVal
	}

	if !known {
		// Keep the raw bytes (r is bounded to this tag's length by ReadTaggedFields)
		field, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if res.rawTaggedFields == nil {
			rawTaggedFields := make([]protocol.TaggedField, 0)
			res.rawTaggedFields = &rawTaggedFields
		}
		*res.rawTaggedFields = append(*res.rawTaggedFields, protocol.TaggedField{Tag: tag, Field: field})
	}

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *UpdateRaftVoterResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- UpdateRaftVoterResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	fmt.Fprintf(w, "        CurrentLeader:\n")
	if res.CurrentLeader != nil {
		fmt.Fprintf(w, "%s", res.CurrentLeader.PrettyPrint())
	} else {
		fmt.Fprintf(w, "            nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *UpdateRaftVoterResponseCurrentLeader) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "            LeaderEpoch: %v\n", value.LeaderEpoch)

	if value.Host != nil {
		fmt.Fprintf(w, "            Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "            Host: nil\n")
	}

	fmt.Fprintf(w, "            Port: %v\n", value.Port)

	return w.String()
}
