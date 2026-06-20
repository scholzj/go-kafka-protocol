package metadata

import (
	"bytes"
	"testing"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

// Golden wire-format vector for MetadataRequest v0 (non-flexible). Bytes derived by hand from the
// Kafka protocol definition, independent of the generator, to catch symmetric encode+decode bugs.
//
// v0 fields: Topics (array; TopicId is v10+ and absent, so each topic is just Name). Non-flexible,
// so the array uses an INT32 length and the string an INT16 length; there is no tagged-fields section.
//
//	Topics length = 1                 -> 00 00 00 01
//	  Topic[0].Name "a" (INT16 len)   -> 00 01 'a'(0x61)
func TestMetadataRequestV0WireFormat(t *testing.T) {
	want := []byte{
		0x00, 0x00, 0x00, 0x01, // Topics length
		0x00, 0x01, 0x61, // Name "a"
	}

	name := "a"
	in := &MetadataRequest{Topics: &[]MetadataRequestTopic{{Name: &name}}}
	in.ApiVersion = 0

	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("encoded = %x, want %x", buf.Bytes(), want)
	}

	out := &MetadataRequest{}
	req := &protocol.Request{Body: bytes.NewBuffer(want)}
	req.ApiVersion = 0
	if err := out.Read(req); err != nil {
		t.Fatalf("read: %v", err)
	}
	if out.Topics == nil || len(*out.Topics) != 1 {
		t.Fatalf("Topics = %v, want 1 element", out.Topics)
	}
	if got := (*out.Topics)[0]; got.Name == nil || *got.Name != "a" {
		t.Errorf("Topics[0].Name = %v, want \"a\"", got.Name)
	}
}
