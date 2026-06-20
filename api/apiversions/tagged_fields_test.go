package apiversions

import (
	"bytes"
	"testing"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

// A response with tagged fields round-trips through Write/Read (v3 is the first flexible version).
func TestTaggedFieldsRoundtrip(t *testing.T) {
	in := &ApiVersionsResponse{
		ApiKeys:                &[]ApiVersionsResponseApiKey{}, // non-nullable array, must be non-nil
		FinalizedFeaturesEpoch: 42,                             // tag 1
		ZkMigrationReady:       true,                           // tag 3
	}
	in.ApiVersion = 3

	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}

	out := &ApiVersionsResponse{}
	resp := protocol.Response{Body: bytes.NewBuffer(buf.Bytes())}
	resp.ApiVersion = 3
	if err := out.Read(&resp); err != nil {
		t.Fatalf("read: %v", err)
	}

	if out.FinalizedFeaturesEpoch != 42 {
		t.Errorf("FinalizedFeaturesEpoch = %d, want 42", out.FinalizedFeaturesEpoch)
	}
	if !out.ZkMigrationReady {
		t.Errorf("ZkMigrationReady = false, want true")
	}
}

// An unknown tag must be preserved verbatim in rawTaggedFields, reading exactly the bytes the caller
// (ReadTaggedFields) bounded the reader to.
func TestUnknownTaggedFieldPreserved(t *testing.T) {
	res := &ApiVersionsResponse{}
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	if err := res.taggedFieldsDecoder(bytes.NewReader(payload), 99, uint64(len(payload))); err != nil {
		t.Fatalf("taggedFieldsDecoder: %v", err)
	}

	if res.rawTaggedFields == nil || len(*res.rawTaggedFields) != 1 {
		t.Fatalf("expected 1 raw tagged field, got %v", res.rawTaggedFields)
	}
	got := (*res.rawTaggedFields)[0]
	if got.Tag != 99 || !bytes.Equal(got.Field, payload) {
		t.Errorf("raw tagged field = {Tag:%d, Field:%x}, want {99, %x}", got.Tag, got.Field, payload)
	}
}
