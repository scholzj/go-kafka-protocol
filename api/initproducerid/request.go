package initproducerid

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type InitProducerIdRequest struct {
	ApiVersion           int16
	TransactionalId      *string // The transactional id, or null if the producer is not transactional. (versions: 0+, nullable: 0+)
	TransactionTimeoutMs int32   // The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined. (versions: 0+)
	ProducerId           int64   // The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration. (versions: 3+)
	ProducerEpoch        int16   // The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match. (versions: 3+)
	Enable2Pc            bool    // True if the client wants to enable two-phase commit (2PC) protocol for transactions. (versions: 6+)
	KeepPreparedTxn      bool    // True if the client wants to keep the currently ongoing transaction instead of aborting it. (versions: 6+)
	rawTaggedFields      *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *InitProducerIdRequest) Write(w io.Writer) error {
	// TransactionalId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, req.TransactionalId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, req.TransactionalId); err != nil {
			return err
		}
	}

	// TransactionTimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.TransactionTimeoutMs); err != nil {
		return err
	}

	// ProducerId (versions: 3+)
	if req.ApiVersion >= 3 {
		if err := protocol.WriteInt64(w, req.ProducerId); err != nil {
			return err
		}
	}

	// ProducerEpoch (versions: 3+)
	if req.ApiVersion >= 3 {
		if err := protocol.WriteInt16(w, req.ProducerEpoch); err != nil {
			return err
		}
	}

	// Enable2Pc (versions: 6+)
	if req.ApiVersion >= 6 {
		if err := protocol.WriteBool(w, req.Enable2Pc); err != nil {
			return err
		}
	}

	// KeepPreparedTxn (versions: 6+)
	if req.ApiVersion >= 6 {
		if err := protocol.WriteBool(w, req.KeepPreparedTxn); err != nil {
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
func (req *InitProducerIdRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("InitProducerIdRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// TransactionalId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		transactionalid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.TransactionalId = transactionalid
	} else {
		transactionalid, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		req.TransactionalId = transactionalid
	}

	// TransactionTimeoutMs (versions: 0+)
	transactiontimeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.TransactionTimeoutMs = transactiontimeoutms

	// ProducerId (versions: 3+)
	if req.ApiVersion >= 3 {
		producerid, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		req.ProducerId = producerid
	}

	// ProducerEpoch (versions: 3+)
	if req.ApiVersion >= 3 {
		producerepoch, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		req.ProducerEpoch = producerepoch
	}

	// Enable2Pc (versions: 6+)
	if req.ApiVersion >= 6 {
		enable2pc, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.Enable2Pc = enable2pc
	}

	// KeepPreparedTxn (versions: 6+)
	if req.ApiVersion >= 6 {
		keeppreparedtxn, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.KeepPreparedTxn = keeppreparedtxn
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
func (req *InitProducerIdRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> InitProducerIdRequest:\n")

	if req.TransactionalId != nil {
		fmt.Fprintf(w, "        TransactionalId: %v\n", *req.TransactionalId)
	} else {
		fmt.Fprintf(w, "        TransactionalId: nil\n")
	}

	fmt.Fprintf(w, "        TransactionTimeoutMs: %v\n", req.TransactionTimeoutMs)
	fmt.Fprintf(w, "        ProducerId: %v\n", req.ProducerId)
	fmt.Fprintf(w, "        ProducerEpoch: %v\n", req.ProducerEpoch)
	fmt.Fprintf(w, "        Enable2Pc: %v\n", req.Enable2Pc)
	fmt.Fprintf(w, "        KeepPreparedTxn: %v\n", req.KeepPreparedTxn)

	return w.String()
}
