package fetch

import (
	"bytes"
	"testing"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

// Golden wire-format vector for FetchResponse v4 (non-flexible).
//
// v4 present fields, in order: ThrottleTimeMs (ErrorCode/SessionId are v7+ and absent), then
// Responses. A v4 topic response is Topic, Partitions. Non-flexible -> INT32 array lengths, INT16
// string lengths, no tagged-fields section.
//
//	ThrottleTimeMs = 10               -> 00 00 00 0A
//	Responses length = 1              -> 00 00 00 01
//	  Topic "t"                       -> 00 01 't'(0x74)
//	  Partitions length = 0           -> 00 00 00 00
func TestFetchResponseV4WireFormat(t *testing.T) {
	want := []byte{
		0x00, 0x00, 0x00, 0x0A, // ThrottleTimeMs = 10
		0x00, 0x00, 0x00, 0x01, // Responses length
		0x00, 0x01, 0x74, // Topic "t"
		0x00, 0x00, 0x00, 0x00, // Partitions length = 0
	}

	topic := "t"
	in := &FetchResponse{
		ThrottleTimeMs: 10,
		Responses: &[]FetchResponseResponse{
			{Topic: &topic, Partitions: &[]FetchResponseResponsePartition{}},
		},
	}
	in.ApiVersion = 4

	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("encoded = %x, want %x", buf.Bytes(), want)
	}

	out := &FetchResponse{}
	resp := &protocol.Response{Body: bytes.NewBuffer(want)}
	resp.ApiVersion = 4
	if err := out.Read(resp); err != nil {
		t.Fatalf("read: %v", err)
	}
	if out.ThrottleTimeMs != 10 {
		t.Errorf("ThrottleTimeMs = %d, want 10", out.ThrottleTimeMs)
	}
	if out.Responses == nil || len(*out.Responses) != 1 {
		t.Fatalf("Responses = %v, want 1 element", out.Responses)
	}
	if r := (*out.Responses)[0]; r.Topic == nil || *r.Topic != "t" {
		t.Errorf("Responses[0].Topic = %v, want \"t\"", r.Topic)
	}
}
