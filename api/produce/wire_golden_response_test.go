package produce

import (
	"bytes"
	"testing"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

// Golden wire-format vector for ProduceResponse v3 (non-flexible).
//
// v3 present fields, in order: Responses (array), then ThrottleTimeMs. A v3 partition response is
// Index, ErrorCode, BaseOffset, LogAppendTimeMs (LogStartOffset is v5+, the rest are v8+/v10+).
// Non-flexible -> INT32 array lengths, INT16 string lengths, no tagged-fields section.
//
//	Responses length = 1              -> 00 00 00 01
//	  Name "x"                        -> 00 01 'x'(0x78)
//	  PartitionResponses length = 1   -> 00 00 00 01
//	    Index = 5                     -> 00 00 00 05
//	    ErrorCode = 0                 -> 00 00
//	    BaseOffset = 100              -> 00 00 00 00 00 00 00 64
//	    LogAppendTimeMs = -1          -> FF FF FF FF FF FF FF FF
//	ThrottleTimeMs = 0                -> 00 00 00 00
func TestProduceResponseV3WireFormat(t *testing.T) {
	want := []byte{
		0x00, 0x00, 0x00, 0x01, // Responses length
		0x00, 0x01, 0x78, // Name "x"
		0x00, 0x00, 0x00, 0x01, // PartitionResponses length
		0x00, 0x00, 0x00, 0x05, // Index
		0x00, 0x00, // ErrorCode
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // BaseOffset = 100
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // LogAppendTimeMs = -1
		0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs
	}

	name := "x"
	in := &ProduceResponse{
		Responses: &[]ProduceResponseResponse{{
			Name: &name,
			PartitionResponses: &[]ProduceResponseResponsePartitionResponse{
				{Index: 5, ErrorCode: 0, BaseOffset: 100, LogAppendTimeMs: -1},
			},
		}},
		ThrottleTimeMs: 0,
	}
	in.ApiVersion = 3

	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("encoded = %x, want %x", buf.Bytes(), want)
	}

	out := &ProduceResponse{}
	resp := &protocol.Response{Body: bytes.NewBuffer(want)}
	resp.ApiVersion = 3
	if err := out.Read(resp); err != nil {
		t.Fatalf("read: %v", err)
	}
	if out.Responses == nil || len(*out.Responses) != 1 {
		t.Fatalf("Responses = %v, want 1 element", out.Responses)
	}
	r := (*out.Responses)[0]
	if r.Name == nil || *r.Name != "x" || r.PartitionResponses == nil || len(*r.PartitionResponses) != 1 {
		t.Fatalf("Responses[0] = %+v", r)
	}
	if p := (*r.PartitionResponses)[0]; p.Index != 5 || p.BaseOffset != 100 || p.LogAppendTimeMs != -1 {
		t.Errorf("partition = %+v, want {Index:5 BaseOffset:100 LogAppendTimeMs:-1}", p)
	}
}
