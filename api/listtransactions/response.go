package listtransactions

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ListTransactionsResponse struct {
	ApiVersion          int16
	ThrottleTimeMs      int32                                       // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode           int16                                       // The error code, or 0 if there was no error. (versions: 0+)
	UnknownStateFilters *[]string                                   // Set of state filters provided in the request which were unknown to the transaction coordinator. (versions: 0+)
	TransactionStates   *[]ListTransactionsResponseTransactionState // The current state of the transaction for the transactional id. (versions: 0+)
	rawTaggedFields     *[]protocol.TaggedField
}

type ListTransactionsResponseTransactionState struct {
	TransactionalId  *string // The transactional id. (versions: 0+)
	ProducerId       int64   // The producer id. (versions: 0+)
	TransactionState *string // The current transaction state of the producer. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *ListTransactionsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// UnknownStateFilters (versions: 0+)
	if res.UnknownStateFilters == nil {
		return fmt.Errorf("ListTransactionsResponse.UnknownStateFilters must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, res.UnknownStateFilters); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteString, *res.UnknownStateFilters); err != nil {
			return err
		}
	}

	// TransactionStates (versions: 0+)
	if res.TransactionStates == nil {
		return fmt.Errorf("ListTransactionsResponse.TransactionStates must not be nil in version %d", res.ApiVersion)
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
func (res *ListTransactionsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("ListTransactionsResponse.Read: response or its body is nil")
	}

	*res = ListTransactionsResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// UnknownStateFilters (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		unknownstatefilters, err := protocol.ReadCompactArray(r, protocol.ReadCompactString)
		if err != nil {
			return err
		}
		res.UnknownStateFilters = &unknownstatefilters
	} else {
		unknownstatefilters, err := protocol.ReadArray(r, protocol.ReadString)
		if err != nil {
			return err
		}
		res.UnknownStateFilters = &unknownstatefilters
	}

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

func (res *ListTransactionsResponse) transactionStatesEncoder(w io.Writer, value ListTransactionsResponseTransactionState) error {
	// TransactionalId (versions: 0+)
	if value.TransactionalId == nil {
		return fmt.Errorf("ListTransactionsResponseTransactionState.TransactionalId must not be nil in version %d", res.ApiVersion)
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

	// ProducerId (versions: 0+)
	if err := protocol.WriteInt64(w, value.ProducerId); err != nil {
		return err
	}

	// TransactionState (versions: 0+)
	if value.TransactionState == nil {
		return fmt.Errorf("ListTransactionsResponseTransactionState.TransactionState must not be nil in version %d", res.ApiVersion)
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

func (res *ListTransactionsResponse) transactionStatesDecoder(r io.Reader) (ListTransactionsResponseTransactionState, error) {
	listtransactionsresponsetransactionstate := ListTransactionsResponseTransactionState{}

	// TransactionalId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		transactionalid, err := protocol.ReadCompactString(r)
		if err != nil {
			return listtransactionsresponsetransactionstate, err
		}
		listtransactionsresponsetransactionstate.TransactionalId = &transactionalid
	} else {
		transactionalid, err := protocol.ReadString(r)
		if err != nil {
			return listtransactionsresponsetransactionstate, err
		}
		listtransactionsresponsetransactionstate.TransactionalId = &transactionalid
	}

	// ProducerId (versions: 0+)
	producerid, err := protocol.ReadInt64(r)
	if err != nil {
		return listtransactionsresponsetransactionstate, err
	}
	listtransactionsresponsetransactionstate.ProducerId = producerid

	// TransactionState (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		transactionstate, err := protocol.ReadCompactString(r)
		if err != nil {
			return listtransactionsresponsetransactionstate, err
		}
		listtransactionsresponsetransactionstate.TransactionState = &transactionstate
	} else {
		transactionstate, err := protocol.ReadString(r)
		if err != nil {
			return listtransactionsresponsetransactionstate, err
		}
		listtransactionsresponsetransactionstate.TransactionState = &transactionstate
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return listtransactionsresponsetransactionstate, err
		}
		listtransactionsresponsetransactionstate.rawTaggedFields = &rawTaggedFields
	}

	return listtransactionsresponsetransactionstate, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ListTransactionsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ListTransactionsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.UnknownStateFilters != nil {
		fmt.Fprintf(w, "        UnknownStateFilters: %v\n", *res.UnknownStateFilters)
	} else {
		fmt.Fprintf(w, "        UnknownStateFilters: nil\n")
	}

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
func (value *ListTransactionsResponseTransactionState) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TransactionalId != nil {
		fmt.Fprintf(w, "            TransactionalId: %v\n", *value.TransactionalId)
	} else {
		fmt.Fprintf(w, "            TransactionalId: nil\n")
	}

	fmt.Fprintf(w, "            ProducerId: %v\n", value.ProducerId)

	if value.TransactionState != nil {
		fmt.Fprintf(w, "            TransactionState: %v\n", *value.TransactionState)
	} else {
		fmt.Fprintf(w, "            TransactionState: nil\n")
	}

	return w.String()
}
