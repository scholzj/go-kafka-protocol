package fetch

import (
	"bytes"
	"testing"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

// Golden wire-format vector for FetchRequest v4 (non-flexible). Bytes derived by hand from the Kafka
// protocol definition.
//
// v4 present, non-tagged fields, in order: ReplicaId (int32), MaxWaitMs (int32), MinBytes (int32),
// MaxBytes (int32), IsolationLevel (int8), Topics (array). ClusterId/ReplicaState are tagged (and
// v12+/v15+), so they are absent; non-flexible means INT32 array length and no tagged-fields section.
//
//	ReplicaId = 1        -> 00 00 00 01
//	MaxWaitMs = 2        -> 00 00 00 02
//	MinBytes = 3         -> 00 00 00 03
//	MaxBytes = 4         -> 00 00 00 04
//	IsolationLevel = 0   -> 00
//	Topics length = 0    -> 00 00 00 00
func TestFetchRequestV4WireFormat(t *testing.T) {
	want := []byte{
		0x00, 0x00, 0x00, 0x01, // ReplicaId
		0x00, 0x00, 0x00, 0x02, // MaxWaitMs
		0x00, 0x00, 0x00, 0x03, // MinBytes
		0x00, 0x00, 0x00, 0x04, // MaxBytes
		0x00,                   // IsolationLevel
		0x00, 0x00, 0x00, 0x00, // Topics length = 0
	}

	in := &FetchRequest{
		ReplicaId:      1,
		MaxWaitMs:      2,
		MinBytes:       3,
		MaxBytes:       4,
		IsolationLevel: 0,
		Topics:         &[]FetchRequestTopic{},
	}
	in.ApiVersion = 4

	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("encoded = %x, want %x", buf.Bytes(), want)
	}

	out := &FetchRequest{}
	req := &protocol.Request{Body: bytes.NewBuffer(want)}
	req.ApiVersion = 4
	if err := out.Read(req); err != nil {
		t.Fatalf("read: %v", err)
	}
	if out.ReplicaId != 1 || out.MaxWaitMs != 2 || out.MinBytes != 3 || out.MaxBytes != 4 {
		t.Errorf("scalars = %d/%d/%d/%d, want 1/2/3/4",
			out.ReplicaId, out.MaxWaitMs, out.MinBytes, out.MaxBytes)
	}
	if out.Topics == nil || len(*out.Topics) != 0 {
		t.Errorf("Topics = %v, want empty non-nil", out.Topics)
	}
}
