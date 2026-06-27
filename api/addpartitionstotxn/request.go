package addpartitionstotxn

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AddPartitionsToTxnRequest struct {
	ApiVersion                int16
	Transactions              *[]AddPartitionsToTxnRequestTransaction     // List of transactions to add partitions to. (versions: 4+)
	V3AndBelowTransactionalId *string                                     // The transactional id corresponding to the transaction. (versions: 0-3)
	V3AndBelowProducerId      int64                                       // Current producer id in use by the transactional id. (versions: 0-3)
	V3AndBelowProducerEpoch   int16                                       // Current epoch associated with the producer id. (versions: 0-3)
	V3AndBelowTopics          *[]AddPartitionsToTxnRequestV3AndBelowTopic // The partitions to add to the transaction. (versions: 0-3)
	rawTaggedFields           *[]protocol.TaggedField
}

type AddPartitionsToTxnRequestTransaction struct {
	TransactionalId *string                                      // The transactional id corresponding to the transaction. (versions: 4+)
	ProducerId      int64                                        // Current producer id in use by the transactional id. (versions: 4+)
	ProducerEpoch   int16                                        // Current epoch associated with the producer id. (versions: 4+)
	VerifyOnly      bool                                         // Boolean to signify if we want to check if the partition is in the transaction rather than add it. (versions: 4+)
	Topics          *[]AddPartitionsToTxnRequestTransactionTopic // The partitions to add to the transaction. (versions: 4+)
	rawTaggedFields *[]protocol.TaggedField
}

type AddPartitionsToTxnRequestTransactionTopic struct {
	Name            *string  // The name of the topic. (versions: 0+)
	Partitions      *[]int32 // The partition indexes to add to the transaction. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AddPartitionsToTxnRequestV3AndBelowTopic struct {
	Name            *string  // The name of the topic. (versions: 0+)
	Partitions      *[]int32 // The partition indexes to add to the transaction. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (req *AddPartitionsToTxnRequest) Write(w io.Writer) error {
	// Transactions (versions: 4+)
	if req.ApiVersion >= 4 {
		if req.Transactions == nil {
			return fmt.Errorf("AddPartitionsToTxnRequest.Transactions must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, req.transactionsEncoder, req.Transactions); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, req.transactionsEncoder, *req.Transactions); err != nil {
				return err
			}
		}
	}

	// V3AndBelowTransactionalId (versions: 0-3)
	if req.ApiVersion <= 3 {
		if req.V3AndBelowTransactionalId == nil {
			return fmt.Errorf("AddPartitionsToTxnRequest.V3AndBelowTransactionalId must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteCompactString(w, *req.V3AndBelowTransactionalId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *req.V3AndBelowTransactionalId); err != nil {
				return err
			}
		}
	}

	// V3AndBelowProducerId (versions: 0-3)
	if req.ApiVersion <= 3 {
		if err := protocol.WriteInt64(w, req.V3AndBelowProducerId); err != nil {
			return err
		}
	}

	// V3AndBelowProducerEpoch (versions: 0-3)
	if req.ApiVersion <= 3 {
		if err := protocol.WriteInt16(w, req.V3AndBelowProducerEpoch); err != nil {
			return err
		}
	}

	// V3AndBelowTopics (versions: 0-3)
	if req.ApiVersion <= 3 {
		if req.V3AndBelowTopics == nil {
			return fmt.Errorf("AddPartitionsToTxnRequest.V3AndBelowTopics must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, req.v3AndBelowTopicsEncoder, req.V3AndBelowTopics); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, req.v3AndBelowTopicsEncoder, *req.V3AndBelowTopics); err != nil {
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
func (req *AddPartitionsToTxnRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("AddPartitionsToTxnRequest.Read: request or its body is nil")
	}

	*req = AddPartitionsToTxnRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Transactions (versions: 4+)
	if req.ApiVersion >= 4 {
		if isRequestFlexible(req.ApiVersion) {
			transactions, err := protocol.ReadCompactArray(r, req.transactionsDecoder)
			if err != nil {
				return err
			}
			req.Transactions = &transactions
		} else {
			transactions, err := protocol.ReadArray(r, req.transactionsDecoder)
			if err != nil {
				return err
			}
			req.Transactions = &transactions
		}
	}

	// V3AndBelowTransactionalId (versions: 0-3)
	if req.ApiVersion <= 3 {
		if isRequestFlexible(req.ApiVersion) {
			v3andbelowtransactionalid, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			req.V3AndBelowTransactionalId = &v3andbelowtransactionalid
		} else {
			v3andbelowtransactionalid, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			req.V3AndBelowTransactionalId = &v3andbelowtransactionalid
		}
	}

	// V3AndBelowProducerId (versions: 0-3)
	if req.ApiVersion <= 3 {
		v3andbelowproducerid, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		req.V3AndBelowProducerId = v3andbelowproducerid
	}

	// V3AndBelowProducerEpoch (versions: 0-3)
	if req.ApiVersion <= 3 {
		v3andbelowproducerepoch, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		req.V3AndBelowProducerEpoch = v3andbelowproducerepoch
	}

	// V3AndBelowTopics (versions: 0-3)
	if req.ApiVersion <= 3 {
		if isRequestFlexible(req.ApiVersion) {
			v3andbelowtopics, err := protocol.ReadCompactArray(r, req.v3AndBelowTopicsDecoder)
			if err != nil {
				return err
			}
			req.V3AndBelowTopics = &v3andbelowtopics
		} else {
			v3andbelowtopics, err := protocol.ReadArray(r, req.v3AndBelowTopicsDecoder)
			if err != nil {
				return err
			}
			req.V3AndBelowTopics = &v3andbelowtopics
		}
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

func (req *AddPartitionsToTxnRequest) transactionsEncoder(w io.Writer, value AddPartitionsToTxnRequestTransaction) error {
	// TransactionalId (versions: 4+)
	if req.ApiVersion >= 4 {
		if value.TransactionalId == nil {
			return fmt.Errorf("AddPartitionsToTxnRequestTransaction.TransactionalId must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.TransactionalId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.TransactionalId); err != nil {
				return err
			}
		}
	}

	// ProducerId (versions: 4+)
	if req.ApiVersion >= 4 {
		if err := protocol.WriteInt64(w, value.ProducerId); err != nil {
			return err
		}
	}

	// ProducerEpoch (versions: 4+)
	if req.ApiVersion >= 4 {
		if err := protocol.WriteInt16(w, value.ProducerEpoch); err != nil {
			return err
		}
	}

	// VerifyOnly (versions: 4+)
	if req.ApiVersion >= 4 {
		if err := protocol.WriteBool(w, value.VerifyOnly); err != nil {
			return err
		}
	}

	// Topics (versions: 4+)
	if req.ApiVersion >= 4 {
		if value.Topics == nil {
			return fmt.Errorf("AddPartitionsToTxnRequestTransaction.Topics must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, value.Topics); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, req.topicsEncoder, *value.Topics); err != nil {
				return err
			}
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
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

func (req *AddPartitionsToTxnRequest) transactionsDecoder(r io.Reader) (AddPartitionsToTxnRequestTransaction, error) {
	addpartitionstotxnrequesttransaction := AddPartitionsToTxnRequestTransaction{}

	// TransactionalId (versions: 4+)
	if req.ApiVersion >= 4 {
		if isRequestFlexible(req.ApiVersion) {
			transactionalid, err := protocol.ReadCompactString(r)
			if err != nil {
				return addpartitionstotxnrequesttransaction, err
			}
			addpartitionstotxnrequesttransaction.TransactionalId = &transactionalid
		} else {
			transactionalid, err := protocol.ReadString(r)
			if err != nil {
				return addpartitionstotxnrequesttransaction, err
			}
			addpartitionstotxnrequesttransaction.TransactionalId = &transactionalid
		}
	}

	// ProducerId (versions: 4+)
	if req.ApiVersion >= 4 {
		producerid, err := protocol.ReadInt64(r)
		if err != nil {
			return addpartitionstotxnrequesttransaction, err
		}
		addpartitionstotxnrequesttransaction.ProducerId = producerid
	}

	// ProducerEpoch (versions: 4+)
	if req.ApiVersion >= 4 {
		producerepoch, err := protocol.ReadInt16(r)
		if err != nil {
			return addpartitionstotxnrequesttransaction, err
		}
		addpartitionstotxnrequesttransaction.ProducerEpoch = producerepoch
	}

	// VerifyOnly (versions: 4+)
	if req.ApiVersion >= 4 {
		verifyonly, err := protocol.ReadBool(r)
		if err != nil {
			return addpartitionstotxnrequesttransaction, err
		}
		addpartitionstotxnrequesttransaction.VerifyOnly = verifyonly
	}

	// Topics (versions: 4+)
	if req.ApiVersion >= 4 {
		if isRequestFlexible(req.ApiVersion) {
			topics, err := protocol.ReadCompactArray(r, req.topicsDecoder)
			if err != nil {
				return addpartitionstotxnrequesttransaction, err
			}
			addpartitionstotxnrequesttransaction.Topics = &topics
		} else {
			topics, err := protocol.ReadArray(r, req.topicsDecoder)
			if err != nil {
				return addpartitionstotxnrequesttransaction, err
			}
			addpartitionstotxnrequesttransaction.Topics = &topics
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return addpartitionstotxnrequesttransaction, err
		}
		addpartitionstotxnrequesttransaction.rawTaggedFields = &rawTaggedFields
	}

	return addpartitionstotxnrequesttransaction, nil
}

func (req *AddPartitionsToTxnRequest) topicsEncoder(w io.Writer, value AddPartitionsToTxnRequestTransactionTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("AddPartitionsToTxnRequestTransactionTopic.Name must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("AddPartitionsToTxnRequestTransactionTopic.Partitions must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
			return err
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
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

func (req *AddPartitionsToTxnRequest) topicsDecoder(r io.Reader) (AddPartitionsToTxnRequestTransactionTopic, error) {
	addpartitionstotxnrequesttransactiontopic := AddPartitionsToTxnRequestTransactionTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return addpartitionstotxnrequesttransactiontopic, err
		}
		addpartitionstotxnrequesttransactiontopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return addpartitionstotxnrequesttransactiontopic, err
		}
		addpartitionstotxnrequesttransactiontopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return addpartitionstotxnrequesttransactiontopic, err
		}
		addpartitionstotxnrequesttransactiontopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return addpartitionstotxnrequesttransactiontopic, err
		}
		addpartitionstotxnrequesttransactiontopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return addpartitionstotxnrequesttransactiontopic, err
		}
		addpartitionstotxnrequesttransactiontopic.rawTaggedFields = &rawTaggedFields
	}

	return addpartitionstotxnrequesttransactiontopic, nil
}

func (req *AddPartitionsToTxnRequest) v3AndBelowTopicsEncoder(w io.Writer, value AddPartitionsToTxnRequestV3AndBelowTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("AddPartitionsToTxnRequestV3AndBelowTopic.Name must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("AddPartitionsToTxnRequestV3AndBelowTopic.Partitions must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
			return err
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
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

func (req *AddPartitionsToTxnRequest) v3AndBelowTopicsDecoder(r io.Reader) (AddPartitionsToTxnRequestV3AndBelowTopic, error) {
	addpartitionstotxnrequestv3andbelowtopic := AddPartitionsToTxnRequestV3AndBelowTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return addpartitionstotxnrequestv3andbelowtopic, err
		}
		addpartitionstotxnrequestv3andbelowtopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return addpartitionstotxnrequestv3andbelowtopic, err
		}
		addpartitionstotxnrequestv3andbelowtopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return addpartitionstotxnrequestv3andbelowtopic, err
		}
		addpartitionstotxnrequestv3andbelowtopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return addpartitionstotxnrequestv3andbelowtopic, err
		}
		addpartitionstotxnrequestv3andbelowtopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return addpartitionstotxnrequestv3andbelowtopic, err
		}
		addpartitionstotxnrequestv3andbelowtopic.rawTaggedFields = &rawTaggedFields
	}

	return addpartitionstotxnrequestv3andbelowtopic, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *AddPartitionsToTxnRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> AddPartitionsToTxnRequest:\n")

	if req.Transactions != nil {
		fmt.Fprintf(w, "        Transactions:\n")
		for _, transactions := range *req.Transactions {
			fmt.Fprintf(w, "%s", transactions.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Transactions: nil\n")
	}

	if req.V3AndBelowTransactionalId != nil {
		fmt.Fprintf(w, "        V3AndBelowTransactionalId: %v\n", *req.V3AndBelowTransactionalId)
	} else {
		fmt.Fprintf(w, "        V3AndBelowTransactionalId: nil\n")
	}

	fmt.Fprintf(w, "        V3AndBelowProducerId: %v\n", req.V3AndBelowProducerId)
	fmt.Fprintf(w, "        V3AndBelowProducerEpoch: %v\n", req.V3AndBelowProducerEpoch)

	if req.V3AndBelowTopics != nil {
		fmt.Fprintf(w, "        V3AndBelowTopics:\n")
		for _, v3andbelowtopics := range *req.V3AndBelowTopics {
			fmt.Fprintf(w, "%s", v3andbelowtopics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        V3AndBelowTopics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AddPartitionsToTxnRequestTransaction) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TransactionalId != nil {
		fmt.Fprintf(w, "            TransactionalId: %v\n", *value.TransactionalId)
	} else {
		fmt.Fprintf(w, "            TransactionalId: nil\n")
	}

	fmt.Fprintf(w, "            ProducerId: %v\n", value.ProducerId)
	fmt.Fprintf(w, "            ProducerEpoch: %v\n", value.ProducerEpoch)
	fmt.Fprintf(w, "            VerifyOnly: %v\n", value.VerifyOnly)

	if value.Topics != nil {
		fmt.Fprintf(w, "            Topics:\n")
		for _, topics := range *value.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AddPartitionsToTxnRequestTransactionTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AddPartitionsToTxnRequestV3AndBelowTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "            Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "            Partitions: nil\n")
	}

	return w.String()
}
