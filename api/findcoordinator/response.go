package findcoordinator

import (
	"bytes"
	"fmt"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

type FindCoordinatorResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32
	ErrorCode       int16
	ErrorMessage    *string
	NodeId          int32
	Host            *string
	Port            int32
	Coordinators    *[]FindCoordinatorResponseCoordinators
	rawTaggedFields []protocol.TaggedField // For unknown tags
}

type FindCoordinatorResponseCoordinators struct {
	Key             *string
	NodeId          int32
	Host            *string
	Port            int32
	ErrorCode       int16
	ErrorMessage    *string
	rawTaggedFields []protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (res *FindCoordinatorResponse) Write(w io.Writer) error {
	// ThrottleTime
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// ErrorCode
	if res.ApiVersion <= 3 {
		if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
			return err
		}
	}

	// ErrorMessage
	if res.ApiVersion >= 1 && res.ApiVersion <= 3 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, res.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, res.ErrorMessage); err != nil {
				return err
			}
		}
	}

	// NodeId
	if res.ApiVersion <= 3 {
		if err := protocol.WriteInt32(w, res.NodeId); err != nil {
			return err
		}
	}

	// Host
	if res.ApiVersion <= 3 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, res.Host); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *res.Host); err != nil {
				return err
			}
		}
	}

	// Port
	if res.ApiVersion <= 3 {
		if err := protocol.WriteInt32(w, res.Port); err != nil {
			return err
		}
	}

	// Coordinators
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
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteRawTaggedFields(w, res.rawTaggedFields); err != nil {
				return err
			}
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (res *FindCoordinatorResponse) Read(response protocol.Response) error {
	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTime
	if response.ApiVersion >= 1 {
		throttleTimeMs, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttleTimeMs
	}

	// ErrorCode
	if response.ApiVersion <= 3 {
		errorCode, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		res.ErrorCode = errorCode
	}

	// ErrorMessage
	if response.ApiVersion >= 1 && response.ApiVersion <= 3 {
		if isResponseFlexible(res.ApiVersion) {
			errorMessage, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			res.ErrorMessage = errorMessage
		} else {
			errorMessage, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			res.ErrorMessage = errorMessage
		}
	}

	// NodeId
	if response.ApiVersion <= 3 {
		nodeId, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.NodeId = nodeId
	}

	// Host
	if response.ApiVersion <= 3 {
		if isResponseFlexible(res.ApiVersion) {
			host, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			res.Host = host
		} else {
			host, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			res.Host = &host
		}
	}

	// Port
	if response.ApiVersion <= 3 {
		port, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.Port = port
	}

	// Coordinators
	if response.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
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

	if isResponseFlexible(response.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		res.rawTaggedFields = rawTaggedFields
	}

	return nil
}

func (res *FindCoordinatorResponse) coordinatorsEncoder(w io.Writer, value FindCoordinatorResponseCoordinators) error {
	// Key
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Key); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Key); err != nil {
			return err
		}
	}

	// NodeId
	if err := protocol.WriteInt32(w, value.NodeId); err != nil {
		return err
	}

	// Host
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Host); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Host); err != nil {
			return err
		}
	}

	// Port
	if err := protocol.WriteInt32(w, value.Port); err != nil {
		return err
	}

	// ErrorCode
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage
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
		if err := protocol.WriteRawTaggedFields(w, value.rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *FindCoordinatorResponse) coordinatorsDecoder(r io.Reader) (FindCoordinatorResponseCoordinators, error) {
	coordinators := FindCoordinatorResponseCoordinators{}

	// Key
	if isResponseFlexible(res.ApiVersion) {
		key, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return coordinators, err
		}
		coordinators.Key = key
	} else {
		key, err := protocol.ReadString(r)
		if err != nil {
			return coordinators, err
		}
		coordinators.Key = &key
	}

	// NodeId
	nodeId, err := protocol.ReadInt32(r)
	if err != nil {
		return coordinators, err
	}
	coordinators.NodeId = nodeId

	// Host
	if isResponseFlexible(res.ApiVersion) {
		host, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return coordinators, err
		}
		coordinators.Host = host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return coordinators, err
		}
		coordinators.Host = &host
	}

	// Port
	port, err := protocol.ReadInt32(r)
	if err != nil {
		return coordinators, err
	}
	coordinators.Port = port

	// ErrorCode
	errorCode, err := protocol.ReadInt16(r)
	if err != nil {
		return coordinators, err
	}
	coordinators.ErrorCode = errorCode

	// ErrorMessage
	if isResponseFlexible(res.ApiVersion) {
		errorMessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return coordinators, err
		}
		coordinators.ErrorMessage = errorMessage
	} else {
		errorMessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return coordinators, err
		}
		coordinators.ErrorMessage = errorMessage
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return coordinators, err
		}
		coordinators.rawTaggedFields = rawTaggedFields
	}

	return coordinators, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *FindCoordinatorResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "<- FindCoordinatorResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %d\n", res.ThrottleTimeMs)
	if res.ApiVersion <= 3 {
		fmt.Fprintf(w, "        ErrorCode: %d\n", res.ErrorCode)
		fmt.Fprintf(w, "        ErrorMessage: %s\n", *res.ErrorMessage)
		fmt.Fprintf(w, "        NodeId: %d\n", res.NodeId)
		fmt.Fprintf(w, "        Host: %s\n", *res.Host)
		fmt.Fprintf(w, "        Port: %d\n", res.Port)
	} else {
		if res.Coordinators != nil {
			fmt.Fprintf(w, "        Coordinators:\n")
			for _, coordinator := range *res.Coordinators {
				fmt.Fprintf(w, "                Key: %s; NodeId: %d; Host: %s; Port: %d; ErrorCode: %d; ErrorMessage: %s\n", *coordinator.Key, coordinator.NodeId, *coordinator.Host, coordinator.Port, coordinator.ErrorCode, *coordinator.ErrorMessage)
			}
		} else {
			fmt.Fprintf(w, "        Coordinators: nil\n")
		}
	}

	return w.String()
}
