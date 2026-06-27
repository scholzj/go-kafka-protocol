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

// #4: ReadTaggedFields calls the decoder once per tag, so unknown tags must accumulate. A known tag
// arriving between two unknown ones must not clear the raw tags preserved so far.
func TestUnknownTaggedFieldsAccumulate(t *testing.T) {
	res := &ApiVersionsResponse{}
	res.ApiVersion = 3 // tag 1 (FinalizedFeaturesEpoch) is only decoded in versions 3+

	// Unknown tag 50.
	if err := res.taggedFieldsDecoder(bytes.NewReader([]byte{0x01}), 50, 1); err != nil {
		t.Fatalf("tag 50: %v", err)
	}
	// Known tag 1 (FinalizedFeaturesEpoch) in between - must not wipe the preserved raw tags.
	var known bytes.Buffer
	if err := protocol.WriteInt64(&known, 7); err != nil {
		t.Fatal(err)
	}
	if err := res.taggedFieldsDecoder(bytes.NewReader(known.Bytes()), 1, uint64(known.Len())); err != nil {
		t.Fatalf("tag 1: %v", err)
	}
	// Unknown tag 60.
	if err := res.taggedFieldsDecoder(bytes.NewReader([]byte{0x02}), 60, 1); err != nil {
		t.Fatalf("tag 60: %v", err)
	}

	if res.FinalizedFeaturesEpoch != 7 {
		t.Errorf("known tag not decoded: FinalizedFeaturesEpoch = %d, want 7", res.FinalizedFeaturesEpoch)
	}
	if res.rawTaggedFields == nil || len(*res.rawTaggedFields) != 2 {
		t.Fatalf("expected 2 preserved raw tags, got %v", res.rawTaggedFields)
	}
	tags := *res.rawTaggedFields
	if tags[0].Tag != 50 || !bytes.Equal(tags[0].Field, []byte{0x01}) ||
		tags[1].Tag != 60 || !bytes.Equal(tags[1].Field, []byte{0x02}) {
		t.Errorf("preserved raw tags = %+v, want tags 50 then 60", tags)
	}
}

// A known tag number seen in a version outside its taggedVersions range must be preserved verbatim
// as a raw tag, not decoded into the field and then silently dropped when re-encoded.
func TestTaggedFieldOutsideVersionPreservedAsRaw(t *testing.T) {
	res := &ApiVersionsResponse{}
	res.ApiVersion = 0 // tag 1 (FinalizedFeaturesEpoch) only exists in v3+

	var payload bytes.Buffer
	if err := protocol.WriteInt64(&payload, 7); err != nil {
		t.Fatal(err)
	}
	if err := res.taggedFieldsDecoder(bytes.NewReader(payload.Bytes()), 1, uint64(payload.Len())); err != nil {
		t.Fatalf("taggedFieldsDecoder: %v", err)
	}

	if res.FinalizedFeaturesEpoch == 7 {
		t.Errorf("tag 1 must not be decoded at v0, but FinalizedFeaturesEpoch was set to 7")
	}
	if res.rawTaggedFields == nil || len(*res.rawTaggedFields) != 1 || (*res.rawTaggedFields)[0].Tag != 1 ||
		!bytes.Equal((*res.rawTaggedFields)[0].Field, payload.Bytes()) {
		t.Fatalf("tag 1 not preserved as a raw tag at v0: %v", res.rawTaggedFields)
	}
}

// Reading into a reused struct must not retain tagged values (or raw tags) from a previous read:
// a second message that omits a tag must leave the field at its default, not the earlier value.
func TestReadClearsStaleTaggedState(t *testing.T) {
	// First message carries tag 1 (epoch 42) and tag 3 (zkMigrationReady true).
	first := &ApiVersionsResponse{
		ApiKeys:                &[]ApiVersionsResponseApiKey{},
		FinalizedFeaturesEpoch: 42,
		ZkMigrationReady:       true,
	}
	first.ApiVersion = 3
	var fb bytes.Buffer
	if err := first.Write(&fb); err != nil {
		t.Fatalf("first write: %v", err)
	}

	// Second message leaves both at their defaults (epoch -1, zk false), so both tags are omitted.
	second := &ApiVersionsResponse{
		ApiKeys:                &[]ApiVersionsResponseApiKey{},
		FinalizedFeaturesEpoch: -1,
		ZkMigrationReady:       false,
	}
	second.ApiVersion = 3
	var sb bytes.Buffer
	if err := second.Write(&sb); err != nil {
		t.Fatalf("second write: %v", err)
	}

	// Reuse a single receiver for both reads.
	out := &ApiVersionsResponse{}

	r1 := protocol.Response{Body: bytes.NewBuffer(fb.Bytes())}
	r1.ApiVersion = 3
	if err := out.Read(&r1); err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if out.FinalizedFeaturesEpoch != 42 || !out.ZkMigrationReady {
		t.Fatalf("first read wrong: epoch=%d zk=%v", out.FinalizedFeaturesEpoch, out.ZkMigrationReady)
	}

	r2 := protocol.Response{Body: bytes.NewBuffer(sb.Bytes())}
	r2.ApiVersion = 3
	if err := out.Read(&r2); err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if out.FinalizedFeaturesEpoch != -1 {
		t.Errorf("stale FinalizedFeaturesEpoch retained across reads: got %d, want -1", out.FinalizedFeaturesEpoch)
	}
	if out.ZkMigrationReady {
		t.Errorf("stale ZkMigrationReady retained across reads: got true, want false")
	}
}
