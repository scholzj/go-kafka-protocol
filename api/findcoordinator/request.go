package findcoordinator

import (
	"bytes"
	"fmt"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

type FindCoordinatorRequest struct {
	ApiVersion      int16
	Key             *string
	KeyType         int8
	CoordinatorKeys *[]*string
	rawTaggedFields []protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (req *FindCoordinatorRequest) Write(w io.Writer) error {
	// Key
	if req.ApiVersion >= 0 && req.ApiVersion <= 3 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteCompactString(w, *req.Key); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *req.Key); err != nil {
				return err
			}
		}
	}

	// KeyType
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, req.KeyType); err != nil {
			return err
		}
	}

	// Coordinator Keys
	if req.ApiVersion >= 4 {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteNullableCompactString, req.CoordinatorKeys); err != nil {
			return err
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteRawTaggedFields(w, req.rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (req *FindCoordinatorRequest) Read(request protocol.Request) error {
	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Key
	if req.ApiVersion >= 0 && req.ApiVersion <= 3 {
		if isRequestFlexible(req.ApiVersion) {
			key, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			req.Key = &key
		} else {
			key, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			req.Key = &key
		}
	}

	// KeyType
	if req.ApiVersion >= 1 {
		keyType, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		req.KeyType = keyType
	}

	// Coordinator Keys
	if req.ApiVersion >= 4 {
		coordinatorKeys, err := protocol.ReadNullableCompactArray(r, protocol.ReadNullableCompactString)
		if err != nil {
			return err
		}
		req.CoordinatorKeys = coordinatorKeys
	}

	// Tagged fields
	if isRequestFlexible(request.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		req.rawTaggedFields = rawTaggedFields
	}

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *FindCoordinatorRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "-> FindCoordinatorRequest:\n")
	fmt.Fprintf(w, "        KeyType: %d\n", req.KeyType)
	if req.ApiVersion <= 3 {
		fmt.Fprintf(w, "        Key: %s\n", *req.Key)
	} else {
		if req.CoordinatorKeys != nil {
			fmt.Fprintf(w, "        CoordinatorKeys:\n")
			for _, coordinatorKey := range *req.CoordinatorKeys {
				fmt.Fprintf(w, "                %s\n", *coordinatorKey)
			}
		} else {
			fmt.Fprintf(w, "        CoordinatorKeys: nil\n")
		}
	}

	return w.String()
}
