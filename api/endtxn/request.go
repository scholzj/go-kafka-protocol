package endtxn

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type EndTxnRequest struct {
	ApiVersion      int16
	TransactionalId *string // The ID of the transaction to end. (versions: 0+)
	ProducerId      int64   // The producer ID. (versions: 0+)
	ProducerEpoch   int16   // The current epoch associated with the producer. (versions: 0+)
	Committed       bool    // True if the transaction was committed, false if it was aborted. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (req *EndTxnRequest) Write(w io.Writer) error {
	// TransactionalId (versions: 0+)
	if req.TransactionalId == nil {
		return fmt.Errorf("EndTxnRequest.TransactionalId must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *req.TransactionalId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *req.TransactionalId); err != nil {
			return err
		}
	}

	// ProducerId (versions: 0+)
	if err := protocol.WriteInt64(w, req.ProducerId); err != nil {
		return err
	}

	// ProducerEpoch (versions: 0+)
	if err := protocol.WriteInt16(w, req.ProducerEpoch); err != nil {
		return err
	}

	// Committed (versions: 0+)
	if err := protocol.WriteBool(w, req.Committed); err != nil {
		return err
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
func (req *EndTxnRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("EndTxnRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// TransactionalId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		transactionalid, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		req.TransactionalId = &transactionalid
	} else {
		transactionalid, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		req.TransactionalId = &transactionalid
	}

	// ProducerId (versions: 0+)
	producerid, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	req.ProducerId = producerid

	// ProducerEpoch (versions: 0+)
	producerepoch, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	req.ProducerEpoch = producerepoch

	// Committed (versions: 0+)
	committed, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	req.Committed = committed

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
func (req *EndTxnRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> EndTxnRequest:\n")

	if req.TransactionalId != nil {
		fmt.Fprintf(w, "        TransactionalId: %v\n", *req.TransactionalId)
	} else {
		fmt.Fprintf(w, "        TransactionalId: nil\n")
	}

	fmt.Fprintf(w, "        ProducerId: %v\n", req.ProducerId)
	fmt.Fprintf(w, "        ProducerEpoch: %v\n", req.ProducerEpoch)
	fmt.Fprintf(w, "        Committed: %v\n", req.Committed)

	return w.String()
}
