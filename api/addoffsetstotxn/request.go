package addoffsetstotxn

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AddOffsetsToTxnRequest struct {
	ApiVersion      int16
	TransactionalId *string // The transactional id corresponding to the transaction. (versions: 0+)
	ProducerId      int64   // Current producer id in use by the transactional id. (versions: 0+)
	ProducerEpoch   int16   // Current epoch associated with the producer id. (versions: 0+)
	GroupId         *string // The unique group identifier. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (req *AddOffsetsToTxnRequest) Write(w io.Writer) error {
	// TransactionalId (versions: 0+)
	if req.TransactionalId == nil {
		return fmt.Errorf("AddOffsetsToTxnRequest.TransactionalId must not be nil in version %d", req.ApiVersion)
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

	// GroupId (versions: 0+)
	if req.GroupId == nil {
		return fmt.Errorf("AddOffsetsToTxnRequest.GroupId must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *req.GroupId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *req.GroupId); err != nil {
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
func (req *AddOffsetsToTxnRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("AddOffsetsToTxnRequest.Read: request or its body is nil")
	}

	*req = AddOffsetsToTxnRequest{}

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

	// GroupId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		groupid, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		req.GroupId = &groupid
	} else {
		groupid, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		req.GroupId = &groupid
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
func (req *AddOffsetsToTxnRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> AddOffsetsToTxnRequest:\n")

	if req.TransactionalId != nil {
		fmt.Fprintf(w, "        TransactionalId: %v\n", *req.TransactionalId)
	} else {
		fmt.Fprintf(w, "        TransactionalId: nil\n")
	}

	fmt.Fprintf(w, "        ProducerId: %v\n", req.ProducerId)
	fmt.Fprintf(w, "        ProducerEpoch: %v\n", req.ProducerEpoch)

	if req.GroupId != nil {
		fmt.Fprintf(w, "        GroupId: %v\n", *req.GroupId)
	} else {
		fmt.Fprintf(w, "        GroupId: nil\n")
	}

	return w.String()
}
