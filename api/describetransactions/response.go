package describetransactions

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeTransactionsResponse struct {
	ApiVersion        int16
	ThrottleTimeMs    int32                                           // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	TransactionStates *[]DescribeTransactionsResponseTransactionState // The current state of the transaction. (versions: 0+)
	rawTaggedFields   *[]protocol.TaggedField
}

type DescribeTransactionsResponseTransactionState struct {
	ErrorCode              int16                                                // The error code. (versions: 0+)
	TransactionalId        *string                                              // The transactional id. (versions: 0+)
	TransactionState       *string                                              // The current transaction state of the producer. (versions: 0+)
	TransactionTimeoutMs   int32                                                // The timeout in milliseconds for the transaction. (versions: 0+)
	TransactionStartTimeMs int64                                                // The start time of the transaction in milliseconds. (versions: 0+)
	ProducerId             int64                                                // The current producer id associated with the transaction. (versions: 0+)
	ProducerEpoch          int16                                                // The current epoch associated with the producer id. (versions: 0+)
	Topics                 *[]DescribeTransactionsResponseTransactionStateTopic // The set of partitions included in the current transaction (if active). When a transaction is preparing to commit or abort, this will include only partitions which do not have markers. (versions: 0+)
	rawTaggedFields        *[]protocol.TaggedField
}

type DescribeTransactionsResponseTransactionStateTopic struct {
	Topic           *string  // The topic name. (versions: 0+)
	Partitions      *[]int32 // The partition ids included in the current transaction. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *DescribeTransactionsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// TransactionStates (versions: 0+)
	if res.TransactionStates == nil {
		return fmt.Errorf("DescribeTransactionsResponse.TransactionStates must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.transactionStatesEncoder, res.TransactionStates); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.transactionStatesEncoder, *res.TransactionStates); err != nil {
			return err
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
func (res *DescribeTransactionsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeTransactionsResponse.Read: response or its body is nil")
	}

	*res = DescribeTransactionsResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// TransactionStates (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		transactionstates, err := protocol.ReadCompactArray(r, res.transactionStatesDecoder)
		if err != nil {
			return err
		}
		res.TransactionStates = &transactionstates
	} else {
		transactionstates, err := protocol.ReadArray(r, res.transactionStatesDecoder)
		if err != nil {
			return err
		}
		res.TransactionStates = &transactionstates
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		res.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

func (res *DescribeTransactionsResponse) transactionStatesEncoder(w io.Writer, value DescribeTransactionsResponseTransactionState) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// TransactionalId (versions: 0+)
	if value.TransactionalId == nil {
		return fmt.Errorf("DescribeTransactionsResponseTransactionState.TransactionalId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.TransactionalId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.TransactionalId); err != nil {
			return err
		}
	}

	// TransactionState (versions: 0+)
	if value.TransactionState == nil {
		return fmt.Errorf("DescribeTransactionsResponseTransactionState.TransactionState must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.TransactionState); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.TransactionState); err != nil {
			return err
		}
	}

	// TransactionTimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, value.TransactionTimeoutMs); err != nil {
		return err
	}

	// TransactionStartTimeMs (versions: 0+)
	if err := protocol.WriteInt64(w, value.TransactionStartTimeMs); err != nil {
		return err
	}

	// ProducerId (versions: 0+)
	if err := protocol.WriteInt64(w, value.ProducerId); err != nil {
		return err
	}

	// ProducerEpoch (versions: 0+)
	if err := protocol.WriteInt16(w, value.ProducerEpoch); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if value.Topics == nil {
		return fmt.Errorf("DescribeTransactionsResponseTransactionState.Topics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.topicsEncoder, value.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicsEncoder, *value.Topics); err != nil {
			return err
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

func (res *DescribeTransactionsResponse) transactionStatesDecoder(r io.Reader) (DescribeTransactionsResponseTransactionState, error) {
	describetransactionsresponsetransactionstate := DescribeTransactionsResponseTransactionState{}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return describetransactionsresponsetransactionstate, err
	}
	describetransactionsresponsetransactionstate.ErrorCode = errorcode

	// TransactionalId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		transactionalid, err := protocol.ReadCompactString(r)
		if err != nil {
			return describetransactionsresponsetransactionstate, err
		}
		describetransactionsresponsetransactionstate.TransactionalId = &transactionalid
	} else {
		transactionalid, err := protocol.ReadString(r)
		if err != nil {
			return describetransactionsresponsetransactionstate, err
		}
		describetransactionsresponsetransactionstate.TransactionalId = &transactionalid
	}

	// TransactionState (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		transactionstate, err := protocol.ReadCompactString(r)
		if err != nil {
			return describetransactionsresponsetransactionstate, err
		}
		describetransactionsresponsetransactionstate.TransactionState = &transactionstate
	} else {
		transactionstate, err := protocol.ReadString(r)
		if err != nil {
			return describetransactionsresponsetransactionstate, err
		}
		describetransactionsresponsetransactionstate.TransactionState = &transactionstate
	}

	// TransactionTimeoutMs (versions: 0+)
	transactiontimeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return describetransactionsresponsetransactionstate, err
	}
	describetransactionsresponsetransactionstate.TransactionTimeoutMs = transactiontimeoutms

	// TransactionStartTimeMs (versions: 0+)
	transactionstarttimems, err := protocol.ReadInt64(r)
	if err != nil {
		return describetransactionsresponsetransactionstate, err
	}
	describetransactionsresponsetransactionstate.TransactionStartTimeMs = transactionstarttimems

	// ProducerId (versions: 0+)
	producerid, err := protocol.ReadInt64(r)
	if err != nil {
		return describetransactionsresponsetransactionstate, err
	}
	describetransactionsresponsetransactionstate.ProducerId = producerid

	// ProducerEpoch (versions: 0+)
	producerepoch, err := protocol.ReadInt16(r)
	if err != nil {
		return describetransactionsresponsetransactionstate, err
	}
	describetransactionsresponsetransactionstate.ProducerEpoch = producerepoch

	// Topics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topics, err := protocol.ReadCompactArray(r, res.topicsDecoder)
		if err != nil {
			return describetransactionsresponsetransactionstate, err
		}
		describetransactionsresponsetransactionstate.Topics = &topics
	} else {
		topics, err := protocol.ReadArray(r, res.topicsDecoder)
		if err != nil {
			return describetransactionsresponsetransactionstate, err
		}
		describetransactionsresponsetransactionstate.Topics = &topics
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describetransactionsresponsetransactionstate, err
		}
		describetransactionsresponsetransactionstate.rawTaggedFields = &rawTaggedFields
	}

	return describetransactionsresponsetransactionstate, nil
}

func (res *DescribeTransactionsResponse) topicsEncoder(w io.Writer, value DescribeTransactionsResponseTransactionStateTopic) error {
	// Topic (versions: 0+)
	if value.Topic == nil {
		return fmt.Errorf("DescribeTransactionsResponseTransactionStateTopic.Topic must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Topic); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Topic); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("DescribeTransactionsResponseTransactionStateTopic.Partitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
			return err
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

func (res *DescribeTransactionsResponse) topicsDecoder(r io.Reader) (DescribeTransactionsResponseTransactionStateTopic, error) {
	describetransactionsresponsetransactionstatetopic := DescribeTransactionsResponseTransactionStateTopic{}

	// Topic (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topic, err := protocol.ReadCompactString(r)
		if err != nil {
			return describetransactionsresponsetransactionstatetopic, err
		}
		describetransactionsresponsetransactionstatetopic.Topic = &topic
	} else {
		topic, err := protocol.ReadString(r)
		if err != nil {
			return describetransactionsresponsetransactionstatetopic, err
		}
		describetransactionsresponsetransactionstatetopic.Topic = &topic
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return describetransactionsresponsetransactionstatetopic, err
		}
		describetransactionsresponsetransactionstatetopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return describetransactionsresponsetransactionstatetopic, err
		}
		describetransactionsresponsetransactionstatetopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describetransactionsresponsetransactionstatetopic, err
		}
		describetransactionsresponsetransactionstatetopic.rawTaggedFields = &rawTaggedFields
	}

	return describetransactionsresponsetransactionstatetopic, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeTransactionsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeTransactionsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	if res.TransactionStates != nil {
		fmt.Fprintf(w, "        TransactionStates:\n")
		for _, transactionstates := range *res.TransactionStates {
			fmt.Fprintf(w, "%s", transactionstates.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        TransactionStates: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeTransactionsResponseTransactionState) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.TransactionalId != nil {
		fmt.Fprintf(w, "            TransactionalId: %v\n", *value.TransactionalId)
	} else {
		fmt.Fprintf(w, "            TransactionalId: nil\n")
	}

	if value.TransactionState != nil {
		fmt.Fprintf(w, "            TransactionState: %v\n", *value.TransactionState)
	} else {
		fmt.Fprintf(w, "            TransactionState: nil\n")
	}

	fmt.Fprintf(w, "            TransactionTimeoutMs: %v\n", value.TransactionTimeoutMs)
	fmt.Fprintf(w, "            TransactionStartTimeMs: %v\n", value.TransactionStartTimeMs)
	fmt.Fprintf(w, "            ProducerId: %v\n", value.ProducerId)
	fmt.Fprintf(w, "            ProducerEpoch: %v\n", value.ProducerEpoch)

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
func (value *DescribeTransactionsResponseTransactionStateTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Topic != nil {
		fmt.Fprintf(w, "                Topic: %v\n", *value.Topic)
	} else {
		fmt.Fprintf(w, "                Topic: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                Partitions: nil\n")
	}

	return w.String()
}
