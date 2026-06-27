package describetransactions

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeTransactionsRequest struct {
	ApiVersion       int16
	TransactionalIds *[]string // Array of transactionalIds to include in describe results. If empty, then no results will be returned. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *DescribeTransactionsRequest) Write(w io.Writer) error {
	// TransactionalIds (versions: 0+)
	if req.TransactionalIds == nil {
		return fmt.Errorf("DescribeTransactionsRequest.TransactionalIds must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, req.TransactionalIds); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteString, *req.TransactionalIds); err != nil {
			return err
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
func (req *DescribeTransactionsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeTransactionsRequest.Read: request or its body is nil")
	}

	*req = DescribeTransactionsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// TransactionalIds (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		transactionalids, err := protocol.ReadCompactArray(r, protocol.ReadCompactString)
		if err != nil {
			return err
		}
		req.TransactionalIds = &transactionalids
	} else {
		transactionalids, err := protocol.ReadArray(r, protocol.ReadString)
		if err != nil {
			return err
		}
		req.TransactionalIds = &transactionalids
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		req.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DescribeTransactionsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeTransactionsRequest:\n")

	if req.TransactionalIds != nil {
		fmt.Fprintf(w, "        TransactionalIds: %v\n", *req.TransactionalIds)
	} else {
		fmt.Fprintf(w, "        TransactionalIds: nil\n")
	}

	return w.String()
}
