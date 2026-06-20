package metadata

import (
	"bytes"
	"testing"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

// Golden wire-format vector for MetadataResponse v0 (non-flexible).
//
// v0 present fields, in order: Brokers (array; v0 broker = NodeId, Host, Port - Rack is v1+), then
// Topics. Non-flexible -> INT32 array lengths, INT16 string lengths, no tagged-fields section.
//
//	Brokers length = 1                -> 00 00 00 01
//	  NodeId = 1                      -> 00 00 00 01
//	  Host "h"                        -> 00 01 'h'(0x68)
//	  Port = 9092 (0x2384)            -> 00 00 23 84
//	Topics length = 0                 -> 00 00 00 00
func TestMetadataResponseV0WireFormat(t *testing.T) {
	want := []byte{
		0x00, 0x00, 0x00, 0x01, // Brokers length
		0x00, 0x00, 0x00, 0x01, // NodeId
		0x00, 0x01, 0x68, // Host "h"
		0x00, 0x00, 0x23, 0x84, // Port 9092
		0x00, 0x00, 0x00, 0x00, // Topics length = 0
	}

	host := "h"
	in := &MetadataResponse{
		Brokers: &[]MetadataResponseBroker{{NodeId: 1, Host: &host, Port: 9092}},
		Topics:  &[]MetadataResponseTopic{},
	}
	in.ApiVersion = 0

	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("encoded = %x, want %x", buf.Bytes(), want)
	}

	out := &MetadataResponse{}
	resp := &protocol.Response{Body: bytes.NewBuffer(want)}
	resp.ApiVersion = 0
	if err := out.Read(resp); err != nil {
		t.Fatalf("read: %v", err)
	}
	if out.Brokers == nil || len(*out.Brokers) != 1 {
		t.Fatalf("Brokers = %v, want 1 element", out.Brokers)
	}
	if b := (*out.Brokers)[0]; b.NodeId != 1 || b.Host == nil || *b.Host != "h" || b.Port != 9092 {
		t.Errorf("Brokers[0] = %+v, want {NodeId:1 Host:h Port:9092}", b)
	}
}
