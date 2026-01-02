package findcoordinator

import (
	"bytes"
	"fmt"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

type FindCoordinatorRequest struct {
	ApiVersion      int16
	Key             *string   // The coordinator key.
	KeyType         int8      // The coordinator key type. (group, transaction, share).
	CoordinatorKeys *[]string // The coordinator keys.
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (req *FindCoordinatorRequest) Write(w io.Writer) error {
	// Key (versions: 0-3)
	if req.ApiVersion <= 3 {
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

	// KeyType (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, req.KeyType); err != nil {
			return err
		}
	}

	// CoordinatorKeys (versions: 4+)
	if req.ApiVersion >= 4 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, req.CoordinatorKeys); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteString, *req.CoordinatorKeys); err != nil {
				return err
			}
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields := []protocol.TaggedField{}
		if req.rawTaggedFields != nil {
			rawTaggedFields = *req.rawTaggedFields
		}
		if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (req *FindCoordinatorRequest) Read(request protocol.Request) error {
	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	var err error

	// Key (versions: 0-3)
	if request.ApiVersion <= 3 {
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

	// KeyType (versions: 1+)
	if request.ApiVersion >= 1 {
		keytype, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		req.KeyType = keytype
	}

	// CoordinatorKeys (versions: 4+)
	if request.ApiVersion >= 4 {
		if isRequestFlexible(req.ApiVersion) {
			coordinatorkeys, err := protocol.ReadNullableCompactArray(r, protocol.ReadCompactString)
			if err != nil {
				return err
			}
			req.CoordinatorKeys = coordinatorkeys
		} else {
			coordinatorkeys, err := protocol.ReadArray(r, protocol.ReadString)
			if err != nil {
				return err
			}
			req.CoordinatorKeys = &coordinatorkeys
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		req.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *FindCoordinatorRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> FindCoordinatorRequest:\n")
	if req.Key != nil {
		fmt.Fprintf(w, "        Key: %v\n", *req.Key)
	} else {
		fmt.Fprintf(w, "        Key: nil\n")
	}
	fmt.Fprintf(w, "        KeyType: %v\n", req.KeyType)
	if req.CoordinatorKeys != nil {
		fmt.Fprintf(w, "        CoordinatorKeys: %v\n", *req.CoordinatorKeys)
	} else {
		fmt.Fprintf(w, "        CoordinatorKeys: nil\n")
	}

	return w.String()
}
