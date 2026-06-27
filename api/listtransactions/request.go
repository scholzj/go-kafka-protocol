package listtransactions

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ListTransactionsRequest struct {
	ApiVersion             int16
	StateFilters           *[]string // The transaction states to filter by: if empty, all transactions are returned; if non-empty, then only transactions matching one of the filtered states will be returned. (versions: 0+)
	ProducerIdFilters      *[]int64  // The producerIds to filter by: if empty, all transactions will be returned; if non-empty, only transactions which match one of the filtered producerIds will be returned. (versions: 0+)
	DurationFilter         int64     // Duration (in millis) to filter by: if < 0, all transactions will be returned; otherwise, only transactions running longer than this duration will be returned. (versions: 1+)
	TransactionalIdPattern *string   // The transactional ID regular expression pattern to filter by: if it is empty or null, all transactions are returned; Otherwise then only the transactions matching the given regular expression will be returned. (versions: 2+, nullable: 2+)
	rawTaggedFields        *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *ListTransactionsRequest) Write(w io.Writer) error {
	// StateFilters (versions: 0+)
	if req.StateFilters == nil {
		return fmt.Errorf("ListTransactionsRequest.StateFilters must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, req.StateFilters); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteString, *req.StateFilters); err != nil {
			return err
		}
	}

	// ProducerIdFilters (versions: 0+)
	if req.ProducerIdFilters == nil {
		return fmt.Errorf("ListTransactionsRequest.ProducerIdFilters must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt64, req.ProducerIdFilters); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt64, *req.ProducerIdFilters); err != nil {
			return err
		}
	}

	// DurationFilter (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt64(w, req.DurationFilter); err != nil {
			return err
		}
	}

	// TransactionalIdPattern (versions: 2+)
	if req.ApiVersion >= 2 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, req.TransactionalIdPattern); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, req.TransactionalIdPattern); err != nil {
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
func (req *ListTransactionsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("ListTransactionsRequest.Read: request or its body is nil")
	}

	*req = ListTransactionsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.DurationFilter = -1

	// StateFilters (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		statefilters, err := protocol.ReadCompactArray(r, protocol.ReadCompactString)
		if err != nil {
			return err
		}
		req.StateFilters = &statefilters
	} else {
		statefilters, err := protocol.ReadArray(r, protocol.ReadString)
		if err != nil {
			return err
		}
		req.StateFilters = &statefilters
	}

	// ProducerIdFilters (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		produceridfilters, err := protocol.ReadCompactArray(r, protocol.ReadInt64)
		if err != nil {
			return err
		}
		req.ProducerIdFilters = &produceridfilters
	} else {
		produceridfilters, err := protocol.ReadArray(r, protocol.ReadInt64)
		if err != nil {
			return err
		}
		req.ProducerIdFilters = &produceridfilters
	}

	// DurationFilter (versions: 1+)
	if req.ApiVersion >= 1 {
		durationfilter, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		req.DurationFilter = durationfilter
	}

	// TransactionalIdPattern (versions: 2+)
	if req.ApiVersion >= 2 {
		if isRequestFlexible(req.ApiVersion) {
			transactionalidpattern, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			req.TransactionalIdPattern = transactionalidpattern
		} else {
			transactionalidpattern, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			req.TransactionalIdPattern = transactionalidpattern
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

//goland:noinspection GoUnhandledErrorResult
func (req *ListTransactionsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ListTransactionsRequest:\n")

	if req.StateFilters != nil {
		fmt.Fprintf(w, "        StateFilters: %v\n", *req.StateFilters)
	} else {
		fmt.Fprintf(w, "        StateFilters: nil\n")
	}

	if req.ProducerIdFilters != nil {
		fmt.Fprintf(w, "        ProducerIdFilters: %v\n", *req.ProducerIdFilters)
	} else {
		fmt.Fprintf(w, "        ProducerIdFilters: nil\n")
	}

	fmt.Fprintf(w, "        DurationFilter: %v\n", req.DurationFilter)

	if req.TransactionalIdPattern != nil {
		fmt.Fprintf(w, "        TransactionalIdPattern: %v\n", *req.TransactionalIdPattern)
	} else {
		fmt.Fprintf(w, "        TransactionalIdPattern: nil\n")
	}

	return w.String()
}
