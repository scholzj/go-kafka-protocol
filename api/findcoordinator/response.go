package findcoordinator

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type FindCoordinatorResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                 // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ErrorCode       int16                                 // The error code, or 0 if there was no error.
	ErrorMessage    *string                               // The error message, or null if there was no error.
	NodeId          int32                                 // The node id.
	Host            *string                               // The host name.
	Port            int32                                 // The port.
	Coordinators    *[]FindCoordinatorResponseCoordinator // Each coordinator result in the response.
	rawTaggedFields *[]protocol.TaggedField
}

type FindCoordinatorResponseCoordinator struct {
	Key             *string // The coordinator key.
	NodeId          int32   // The node id.
	Host            *string // The host name.
	Port            int32   // The port.
	ErrorCode       int16   // The error code, or 0 if there was no error.
	ErrorMessage    *string // The error message, or null if there was no error.
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (res *FindCoordinatorResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// ErrorCode (versions: 0-3)
	if res.ApiVersion <= 3 {
		if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
			return err
		}
	}

	// ErrorMessage (versions: 1-3)
	if res.ApiVersion >= 1 && res.ApiVersion <= 3 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *res.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *res.ErrorMessage); err != nil {
				return err
			}
		}
	}

	// NodeId (versions: 0-3)
	if res.ApiVersion <= 3 {
		if err := protocol.WriteInt32(w, res.NodeId); err != nil {
			return err
		}
	}

	// Host (versions: 0-3)
	if res.ApiVersion <= 3 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *res.Host); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *res.Host); err != nil {
				return err
			}
		}
	}

	// Port (versions: 0-3)
	if res.ApiVersion <= 3 {
		if err := protocol.WriteInt32(w, res.Port); err != nil {
			return err
		}
	}

	// Coordinators (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.coordinatorsEncoder, res.Coordinators); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, res.coordinatorsEncoder, *res.Coordinators); err != nil {
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
func (res *FindCoordinatorResponse) Read(response protocol.Response) error {
	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	var err error

	// ThrottleTimeMs (versions: 1+)
	if response.ApiVersion >= 1 {
		throttletimems, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttletimems
	}

	// ErrorCode (versions: 0-3)
	if response.ApiVersion <= 3 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		res.ErrorCode = errorcode
	}

	// ErrorMessage (versions: 1-3)
	if response.ApiVersion >= 1 && response.ApiVersion <= 3 {
		if isRequestFlexible(res.ApiVersion) {
			errormessage, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			res.ErrorMessage = &errormessage
		} else {
			errormessage, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			res.ErrorMessage = &errormessage
		}
	}

	// NodeId (versions: 0-3)
	if response.ApiVersion <= 3 {
		nodeid, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.NodeId = nodeid
	}

	// Host (versions: 0-3)
	if response.ApiVersion <= 3 {
		if isRequestFlexible(res.ApiVersion) {
			host, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			res.Host = &host
		} else {
			host, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			res.Host = &host
		}
	}

	// Port (versions: 0-3)
	if response.ApiVersion <= 3 {
		port, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.Port = port
	}

	// Coordinators (versions: 4+)
	if response.ApiVersion >= 4 {
		if isRequestFlexible(res.ApiVersion) {
			coordinators, err := protocol.ReadNullableCompactArray(r, res.coordinatorsDecoder)
			if err != nil {
				return err
			}
			res.Coordinators = coordinators
		} else {
			coordinators, err := protocol.ReadArray(r, res.coordinatorsDecoder)
			if err != nil {
				return err
			}
			res.Coordinators = &coordinators
		}
	}

	if isResponseFlexible(res.ApiVersion) {
		// Decode tagged fields
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		res.rawTaggedFields = &rawTaggedFields
		if err != nil {
			return err
		}
	}

	return nil
}

func (res *FindCoordinatorResponse) coordinatorsEncoder(w io.Writer, value FindCoordinatorResponseCoordinator) error {
	// Key (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.Key); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.Key); err != nil {
				return err
			}
		}
	}

	// NodeId (versions: 4+)
	if res.ApiVersion >= 4 {
		if err := protocol.WriteInt32(w, value.NodeId); err != nil {
			return err
		}
	}

	// Host (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.Host); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.Host); err != nil {
				return err
			}
		}
	}

	// Port (versions: 4+)
	if res.ApiVersion >= 4 {
		if err := protocol.WriteInt32(w, value.Port); err != nil {
			return err
		}
	}

	// ErrorCode (versions: 4+)
	if res.ApiVersion >= 4 {
		if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
			return err
		}
	}

	// ErrorMessage (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.ErrorMessage); err != nil {
				return err
			}
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

func (res *FindCoordinatorResponse) coordinatorsDecoder(r io.Reader) (FindCoordinatorResponseCoordinator, error) {
	findcoordinatorresponsecoordinator := FindCoordinatorResponseCoordinator{}
	var err error

	// Key (versions: 4+)
	if res.ApiVersion >= 4 {
		if isRequestFlexible(res.ApiVersion) {
			key, err := protocol.ReadCompactString(r)
			if err != nil {
				return findcoordinatorresponsecoordinator, err
			}
			findcoordinatorresponsecoordinator.Key = &key
		} else {
			key, err := protocol.ReadString(r)
			if err != nil {
				return findcoordinatorresponsecoordinator, err
			}
			findcoordinatorresponsecoordinator.Key = &key
		}
	}

	// NodeId (versions: 4+)
	if res.ApiVersion >= 4 {
		nodeid, err := protocol.ReadInt32(r)
		if err != nil {
			return findcoordinatorresponsecoordinator, err
		}
		findcoordinatorresponsecoordinator.NodeId = nodeid
	}

	// Host (versions: 4+)
	if res.ApiVersion >= 4 {
		if isRequestFlexible(res.ApiVersion) {
			host, err := protocol.ReadCompactString(r)
			if err != nil {
				return findcoordinatorresponsecoordinator, err
			}
			findcoordinatorresponsecoordinator.Host = &host
		} else {
			host, err := protocol.ReadString(r)
			if err != nil {
				return findcoordinatorresponsecoordinator, err
			}
			findcoordinatorresponsecoordinator.Host = &host
		}
	}

	// Port (versions: 4+)
	if res.ApiVersion >= 4 {
		port, err := protocol.ReadInt32(r)
		if err != nil {
			return findcoordinatorresponsecoordinator, err
		}
		findcoordinatorresponsecoordinator.Port = port
	}

	// ErrorCode (versions: 4+)
	if res.ApiVersion >= 4 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return findcoordinatorresponsecoordinator, err
		}
		findcoordinatorresponsecoordinator.ErrorCode = errorcode
	}

	// ErrorMessage (versions: 4+)
	if res.ApiVersion >= 4 {
		if isRequestFlexible(res.ApiVersion) {
			errormessage, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return findcoordinatorresponsecoordinator, err
			}
			findcoordinatorresponsecoordinator.ErrorMessage = errormessage
		} else {
			errormessage, err := protocol.ReadNullableString(r)
			if err != nil {
				return findcoordinatorresponsecoordinator, err
			}
			findcoordinatorresponsecoordinator.ErrorMessage = errormessage
		}
	}

	// Tagged fields
	if isRequestFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return findcoordinatorresponsecoordinator, err
		}
		findcoordinatorresponsecoordinator.rawTaggedFields = &rawTaggedFields
	}

	return findcoordinatorresponsecoordinator, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *FindCoordinatorResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- FindCoordinatorResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
	if res.ErrorMessage != nil {
		fmt.Fprintf(w, "        ErrorMessage: %v\n", *res.ErrorMessage)
	} else {
		fmt.Fprintf(w, "        ErrorMessage: nil\n")
	}
	fmt.Fprintf(w, "        NodeId: %v\n", res.NodeId)
	if res.Host != nil {
		fmt.Fprintf(w, "        Host: %v\n", *res.Host)
	} else {
		fmt.Fprintf(w, "        Host: nil\n")
	}
	fmt.Fprintf(w, "        Port: %v\n", res.Port)
	if res.Coordinators != nil {
		fmt.Fprintf(w, "        Coordinators:\n")
		for _, coordinators := range *res.Coordinators {
			fmt.Fprintf(w, "%s", coordinators.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Coordinators: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FindCoordinatorResponseCoordinator) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Key != nil {
		fmt.Fprintf(w, "            Key: %v\n", *value.Key)
	} else {
		fmt.Fprintf(w, "            Key: nil\n")
	}
	fmt.Fprintf(w, "            NodeId: %v\n", value.NodeId)
	if value.Host != nil {
		fmt.Fprintf(w, "            Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "            Host: nil\n")
	}
	fmt.Fprintf(w, "            Port: %v\n", value.Port)
	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)
	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	return w.String()
}
