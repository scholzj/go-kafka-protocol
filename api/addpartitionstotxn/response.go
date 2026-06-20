package addpartitionstotxn

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AddPartitionsToTxnResponse struct {
	ApiVersion               int16
	ThrottleTimeMs           int32                                                 // Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode                int16                                                 // The response top level error code. (versions: 4+)
	ResultsByTransaction     *[]AddPartitionsToTxnResponseResultsByTransaction     // Results categorized by transactional ID. (versions: 4+)
	ResultsByTopicV3AndBelow *[]AddPartitionsToTxnResponseResultsByTopicV3AndBelow // The results for each topic. (versions: 0-3)
	rawTaggedFields          *[]protocol.TaggedField
}

type AddPartitionsToTxnResponseResultsByTransaction struct {
	TransactionalId *string                                                      // The transactional id corresponding to the transaction. (versions: 4+)
	TopicResults    *[]AddPartitionsToTxnResponseResultsByTransactionTopicResult // The results for each topic. (versions: 4+)
	rawTaggedFields *[]protocol.TaggedField
}

type AddPartitionsToTxnResponseResultsByTransactionTopicResult struct {
	Name               *string                                                                        // The topic name. (versions: 0+)
	ResultsByPartition *[]AddPartitionsToTxnResponseResultsByTransactionTopicResultResultsByPartition // The results for each partition. (versions: 0+)
	rawTaggedFields    *[]protocol.TaggedField
}

type AddPartitionsToTxnResponseResultsByTransactionTopicResultResultsByPartition struct {
	PartitionIndex     int32 // The partition indexes. (versions: 0+)
	PartitionErrorCode int16 // The response error code. (versions: 0+)
	rawTaggedFields    *[]protocol.TaggedField
}

type AddPartitionsToTxnResponseResultsByTopicV3AndBelow struct {
	Name               *string                                                                 // The topic name. (versions: 0+)
	ResultsByPartition *[]AddPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartition // The results for each partition. (versions: 0+)
	rawTaggedFields    *[]protocol.TaggedField
}

type AddPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartition struct {
	PartitionIndex     int32 // The partition indexes. (versions: 0+)
	PartitionErrorCode int16 // The response error code. (versions: 0+)
	rawTaggedFields    *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (res *AddPartitionsToTxnResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 4+)
	if res.ApiVersion >= 4 {
		if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
			return err
		}
	}

	// ResultsByTransaction (versions: 4+)
	if res.ApiVersion >= 4 {
		if res.ResultsByTransaction == nil {
			return fmt.Errorf("AddPartitionsToTxnResponse.ResultsByTransaction must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.resultsByTransactionEncoder, res.ResultsByTransaction); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, res.resultsByTransactionEncoder, *res.ResultsByTransaction); err != nil {
				return err
			}
		}
	}

	// ResultsByTopicV3AndBelow (versions: 0-3)
	if res.ApiVersion <= 3 {
		if res.ResultsByTopicV3AndBelow == nil {
			return fmt.Errorf("AddPartitionsToTxnResponse.ResultsByTopicV3AndBelow must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.resultsByTopicV3AndBelowEncoder, res.ResultsByTopicV3AndBelow); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, res.resultsByTopicV3AndBelowEncoder, *res.ResultsByTopicV3AndBelow); err != nil {
				return err
			}
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
func (res *AddPartitionsToTxnResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("AddPartitionsToTxnResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// ErrorCode (versions: 4+)
	if res.ApiVersion >= 4 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		res.ErrorCode = errorcode
	}

	// ResultsByTransaction (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			resultsbytransaction, err := protocol.ReadNullableCompactArray(r, res.resultsByTransactionDecoder)
			if err != nil {
				return err
			}
			res.ResultsByTransaction = resultsbytransaction
		} else {
			resultsbytransaction, err := protocol.ReadArray(r, res.resultsByTransactionDecoder)
			if err != nil {
				return err
			}
			res.ResultsByTransaction = &resultsbytransaction
		}
	}

	// ResultsByTopicV3AndBelow (versions: 0-3)
	if res.ApiVersion <= 3 {
		if isResponseFlexible(res.ApiVersion) {
			resultsbytopicv3andbelow, err := protocol.ReadNullableCompactArray(r, res.resultsByTopicV3AndBelowDecoder)
			if err != nil {
				return err
			}
			res.ResultsByTopicV3AndBelow = resultsbytopicv3andbelow
		} else {
			resultsbytopicv3andbelow, err := protocol.ReadArray(r, res.resultsByTopicV3AndBelowDecoder)
			if err != nil {
				return err
			}
			res.ResultsByTopicV3AndBelow = &resultsbytopicv3andbelow
		}
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

func (res *AddPartitionsToTxnResponse) resultsByTransactionEncoder(w io.Writer, value AddPartitionsToTxnResponseResultsByTransaction) error {
	// TransactionalId (versions: 4+)
	if res.ApiVersion >= 4 {
		if value.TransactionalId == nil {
			return fmt.Errorf("AddPartitionsToTxnResponseResultsByTransaction.TransactionalId must not be nil in version %d", res.ApiVersion)
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
	}

	// TopicResults (versions: 4+)
	if res.ApiVersion >= 4 {
		if value.TopicResults == nil {
			return fmt.Errorf("AddPartitionsToTxnResponseResultsByTransaction.TopicResults must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, res.topicResultsEncoder, value.TopicResults); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, res.topicResultsEncoder, *value.TopicResults); err != nil {
				return err
			}
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

func (res *AddPartitionsToTxnResponse) resultsByTransactionDecoder(r io.Reader) (AddPartitionsToTxnResponseResultsByTransaction, error) {
	addpartitionstotxnresponseresultsbytransaction := AddPartitionsToTxnResponseResultsByTransaction{}

	// TransactionalId (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			transactionalid, err := protocol.ReadCompactString(r)
			if err != nil {
				return addpartitionstotxnresponseresultsbytransaction, err
			}
			addpartitionstotxnresponseresultsbytransaction.TransactionalId = &transactionalid
		} else {
			transactionalid, err := protocol.ReadString(r)
			if err != nil {
				return addpartitionstotxnresponseresultsbytransaction, err
			}
			addpartitionstotxnresponseresultsbytransaction.TransactionalId = &transactionalid
		}
	}

	// TopicResults (versions: 4+)
	if res.ApiVersion >= 4 {
		if isResponseFlexible(res.ApiVersion) {
			topicresults, err := protocol.ReadNullableCompactArray(r, res.topicResultsDecoder)
			if err != nil {
				return addpartitionstotxnresponseresultsbytransaction, err
			}
			addpartitionstotxnresponseresultsbytransaction.TopicResults = topicresults
		} else {
			topicresults, err := protocol.ReadArray(r, res.topicResultsDecoder)
			if err != nil {
				return addpartitionstotxnresponseresultsbytransaction, err
			}
			addpartitionstotxnresponseresultsbytransaction.TopicResults = &topicresults
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return addpartitionstotxnresponseresultsbytransaction, err
		}
		addpartitionstotxnresponseresultsbytransaction.rawTaggedFields = &rawTaggedFields
	}

	return addpartitionstotxnresponseresultsbytransaction, nil
}

func (res *AddPartitionsToTxnResponse) topicResultsEncoder(w io.Writer, value AddPartitionsToTxnResponseResultsByTransactionTopicResult) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("AddPartitionsToTxnResponseResultsByTransactionTopicResult.Name must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// ResultsByPartition (versions: 0+)
	if value.ResultsByPartition == nil {
		return fmt.Errorf("AddPartitionsToTxnResponseResultsByTransactionTopicResult.ResultsByPartition must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.resultsByPartitionEncoder, value.ResultsByPartition); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.resultsByPartitionEncoder, *value.ResultsByPartition); err != nil {
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

func (res *AddPartitionsToTxnResponse) topicResultsDecoder(r io.Reader) (AddPartitionsToTxnResponseResultsByTransactionTopicResult, error) {
	addpartitionstotxnresponseresultsbytransactiontopicresult := AddPartitionsToTxnResponseResultsByTransactionTopicResult{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return addpartitionstotxnresponseresultsbytransactiontopicresult, err
		}
		addpartitionstotxnresponseresultsbytransactiontopicresult.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return addpartitionstotxnresponseresultsbytransactiontopicresult, err
		}
		addpartitionstotxnresponseresultsbytransactiontopicresult.Name = &name
	}

	// ResultsByPartition (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		resultsbypartition, err := protocol.ReadNullableCompactArray(r, res.resultsByPartitionDecoder)
		if err != nil {
			return addpartitionstotxnresponseresultsbytransactiontopicresult, err
		}
		addpartitionstotxnresponseresultsbytransactiontopicresult.ResultsByPartition = resultsbypartition
	} else {
		resultsbypartition, err := protocol.ReadArray(r, res.resultsByPartitionDecoder)
		if err != nil {
			return addpartitionstotxnresponseresultsbytransactiontopicresult, err
		}
		addpartitionstotxnresponseresultsbytransactiontopicresult.ResultsByPartition = &resultsbypartition
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return addpartitionstotxnresponseresultsbytransactiontopicresult, err
		}
		addpartitionstotxnresponseresultsbytransactiontopicresult.rawTaggedFields = &rawTaggedFields
	}

	return addpartitionstotxnresponseresultsbytransactiontopicresult, nil
}

func (res *AddPartitionsToTxnResponse) resultsByPartitionEncoder(w io.Writer, value AddPartitionsToTxnResponseResultsByTransactionTopicResultResultsByPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// PartitionErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.PartitionErrorCode); err != nil {
		return err
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

func (res *AddPartitionsToTxnResponse) resultsByPartitionDecoder(r io.Reader) (AddPartitionsToTxnResponseResultsByTransactionTopicResultResultsByPartition, error) {
	addpartitionstotxnresponseresultsbytransactiontopicresultresultsbypartition := AddPartitionsToTxnResponseResultsByTransactionTopicResultResultsByPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return addpartitionstotxnresponseresultsbytransactiontopicresultresultsbypartition, err
	}
	addpartitionstotxnresponseresultsbytransactiontopicresultresultsbypartition.PartitionIndex = partitionindex

	// PartitionErrorCode (versions: 0+)
	partitionerrorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return addpartitionstotxnresponseresultsbytransactiontopicresultresultsbypartition, err
	}
	addpartitionstotxnresponseresultsbytransactiontopicresultresultsbypartition.PartitionErrorCode = partitionerrorcode

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return addpartitionstotxnresponseresultsbytransactiontopicresultresultsbypartition, err
		}
		addpartitionstotxnresponseresultsbytransactiontopicresultresultsbypartition.rawTaggedFields = &rawTaggedFields
	}

	return addpartitionstotxnresponseresultsbytransactiontopicresultresultsbypartition, nil
}

func (res *AddPartitionsToTxnResponse) resultsByTopicV3AndBelowEncoder(w io.Writer, value AddPartitionsToTxnResponseResultsByTopicV3AndBelow) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("AddPartitionsToTxnResponseResultsByTopicV3AndBelow.Name must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// ResultsByPartition (versions: 0+)
	if value.ResultsByPartition == nil {
		return fmt.Errorf("AddPartitionsToTxnResponseResultsByTopicV3AndBelow.ResultsByPartition must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.addPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartitionEncoder, value.ResultsByPartition); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.addPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartitionEncoder, *value.ResultsByPartition); err != nil {
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

func (res *AddPartitionsToTxnResponse) resultsByTopicV3AndBelowDecoder(r io.Reader) (AddPartitionsToTxnResponseResultsByTopicV3AndBelow, error) {
	addpartitionstotxnresponseresultsbytopicv3andbelow := AddPartitionsToTxnResponseResultsByTopicV3AndBelow{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return addpartitionstotxnresponseresultsbytopicv3andbelow, err
		}
		addpartitionstotxnresponseresultsbytopicv3andbelow.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return addpartitionstotxnresponseresultsbytopicv3andbelow, err
		}
		addpartitionstotxnresponseresultsbytopicv3andbelow.Name = &name
	}

	// ResultsByPartition (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		resultsbypartition, err := protocol.ReadNullableCompactArray(r, res.addPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartitionDecoder)
		if err != nil {
			return addpartitionstotxnresponseresultsbytopicv3andbelow, err
		}
		addpartitionstotxnresponseresultsbytopicv3andbelow.ResultsByPartition = resultsbypartition
	} else {
		resultsbypartition, err := protocol.ReadArray(r, res.addPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartitionDecoder)
		if err != nil {
			return addpartitionstotxnresponseresultsbytopicv3andbelow, err
		}
		addpartitionstotxnresponseresultsbytopicv3andbelow.ResultsByPartition = &resultsbypartition
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return addpartitionstotxnresponseresultsbytopicv3andbelow, err
		}
		addpartitionstotxnresponseresultsbytopicv3andbelow.rawTaggedFields = &rawTaggedFields
	}

	return addpartitionstotxnresponseresultsbytopicv3andbelow, nil
}

func (res *AddPartitionsToTxnResponse) addPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartitionEncoder(w io.Writer, value AddPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// PartitionErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.PartitionErrorCode); err != nil {
		return err
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

func (res *AddPartitionsToTxnResponse) addPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartitionDecoder(r io.Reader) (AddPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartition, error) {
	addpartitionstotxnresponseresultsbytopicv3andbelowresultsbypartition := AddPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return addpartitionstotxnresponseresultsbytopicv3andbelowresultsbypartition, err
	}
	addpartitionstotxnresponseresultsbytopicv3andbelowresultsbypartition.PartitionIndex = partitionindex

	// PartitionErrorCode (versions: 0+)
	partitionerrorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return addpartitionstotxnresponseresultsbytopicv3andbelowresultsbypartition, err
	}
	addpartitionstotxnresponseresultsbytopicv3andbelowresultsbypartition.PartitionErrorCode = partitionerrorcode

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return addpartitionstotxnresponseresultsbytopicv3andbelowresultsbypartition, err
		}
		addpartitionstotxnresponseresultsbytopicv3andbelowresultsbypartition.rawTaggedFields = &rawTaggedFields
	}

	return addpartitionstotxnresponseresultsbytopicv3andbelowresultsbypartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *AddPartitionsToTxnResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- AddPartitionsToTxnResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.ResultsByTransaction != nil {
		fmt.Fprintf(w, "        ResultsByTransaction:\n")
		for _, resultsbytransaction := range *res.ResultsByTransaction {
			fmt.Fprintf(w, "%s", resultsbytransaction.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        ResultsByTransaction: nil\n")
	}

	if res.ResultsByTopicV3AndBelow != nil {
		fmt.Fprintf(w, "        ResultsByTopicV3AndBelow:\n")
		for _, resultsbytopicv3andbelow := range *res.ResultsByTopicV3AndBelow {
			fmt.Fprintf(w, "%s", resultsbytopicv3andbelow.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        ResultsByTopicV3AndBelow: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AddPartitionsToTxnResponseResultsByTransaction) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TransactionalId != nil {
		fmt.Fprintf(w, "            TransactionalId: %v\n", *value.TransactionalId)
	} else {
		fmt.Fprintf(w, "            TransactionalId: nil\n")
	}

	if value.TopicResults != nil {
		fmt.Fprintf(w, "            TopicResults:\n")
		for _, topicresults := range *value.TopicResults {
			fmt.Fprintf(w, "%s", topicresults.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            TopicResults: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AddPartitionsToTxnResponseResultsByTransactionTopicResult) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

	if value.ResultsByPartition != nil {
		fmt.Fprintf(w, "                ResultsByPartition:\n")
		for _, resultsbypartition := range *value.ResultsByPartition {
			fmt.Fprintf(w, "%s", resultsbypartition.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                ResultsByPartition: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AddPartitionsToTxnResponseResultsByTransactionTopicResultResultsByPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                    PartitionErrorCode: %v\n", value.PartitionErrorCode)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AddPartitionsToTxnResponseResultsByTopicV3AndBelow) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	if value.ResultsByPartition != nil {
		fmt.Fprintf(w, "            ResultsByPartition:\n")
		for _, resultsbypartition := range *value.ResultsByPartition {
			fmt.Fprintf(w, "%s", resultsbypartition.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            ResultsByPartition: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AddPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                PartitionErrorCode: %v\n", value.PartitionErrorCode)

	return w.String()
}
