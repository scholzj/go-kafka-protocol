package produce

import (
	"bytes"
	"testing"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

// Golden wire-format vector for ProduceRequest v3 (non-flexible). Bytes derived by hand from the
// Kafka protocol definition.
//
// v3 fields, in order: TransactionalId (nullable string), Acks (int16), TimeoutMs (int32),
// TopicData (array). Non-flexible -> INT32 array lengths, INT16 string lengths, no tagged fields.
//
//	TransactionalId = null            -> FF FF                 (INT16 -1)
//	Acks = 1                          -> 00 01
//	TimeoutMs = 100                   -> 00 00 00 64
//	TopicData length = 1              -> 00 00 00 01
//	  Name "x"                        -> 00 01 'x'(0x78)       (TopicId is v13+, absent)
//	  PartitionData length = 0        -> 00 00 00 00
func TestProduceRequestV3WireFormat(t *testing.T) {
	want := []byte{
		0xFF, 0xFF, // TransactionalId = null
		0x00, 0x01, // Acks = 1
		0x00, 0x00, 0x00, 0x64, // TimeoutMs = 100
		0x00, 0x00, 0x00, 0x01, // TopicData length = 1
		0x00, 0x01, 0x78, // TopicData[0].Name = "x"
		0x00, 0x00, 0x00, 0x00, // TopicData[0].PartitionData length = 0
	}

	topic := "x"
	in := &ProduceRequest{
		TransactionalId: nil,
		Acks:            1,
		TimeoutMs:       100,
		TopicData: &[]ProduceRequestTopicData{
			{Name: &topic, PartitionData: &[]ProduceRequestTopicDataPartitionData{}},
		},
	}
	in.ApiVersion = 3

	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("encoded = %x, want %x", buf.Bytes(), want)
	}

	out := &ProduceRequest{}
	req := &protocol.Request{Body: bytes.NewBuffer(want)}
	req.ApiVersion = 3
	if err := out.Read(req); err != nil {
		t.Fatalf("read: %v", err)
	}
	if out.TransactionalId != nil {
		t.Errorf("TransactionalId = %v, want nil", *out.TransactionalId)
	}
	if out.Acks != 1 || out.TimeoutMs != 100 {
		t.Errorf("Acks/TimeoutMs = %d/%d, want 1/100", out.Acks, out.TimeoutMs)
	}
	if out.TopicData == nil || len(*out.TopicData) != 1 {
		t.Fatalf("TopicData = %v, want 1 element", out.TopicData)
	}
	if td := (*out.TopicData)[0]; td.Name == nil || *td.Name != "x" {
		t.Errorf("TopicData[0].Name = %v, want \"x\"", td.Name)
	}
}
