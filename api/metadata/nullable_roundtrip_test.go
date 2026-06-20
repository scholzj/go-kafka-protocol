package metadata

import (
	"bytes"
	"testing"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

func strPtr(s string) *string { return &s }

func roundtripResponse(t *testing.T, v int16, in *MetadataResponse) *MetadataResponse {
	t.Helper()
	in.ApiVersion = v
	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatalf("v%d write: %v", v, err)
	}
	out := &MetadataResponse{}
	resp := protocol.Response{Body: bytes.NewBuffer(buf.Bytes())}
	resp.ApiVersion = v
	if err := out.Read(&resp); err != nil {
		t.Fatalf("v%d read: %v", v, err)
	}
	return out
}

func roundtripRequest(t *testing.T, v int16, in *MetadataRequest) *MetadataRequest {
	t.Helper()
	in.ApiVersion = v
	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatalf("v%d write: %v", v, err)
	}
	out := &MetadataRequest{}
	req := protocol.Request{Body: bytes.NewBuffer(buf.Bytes())}
	req.ApiVersion = v
	if err := out.Read(&req); err != nil {
		t.Fatalf("v%d read: %v", v, err)
	}
	return out
}

// v8 exercises the non-flexible (plain) encoders, v12 the flexible (compact) ones.
func TestNullableStringRoundtrip(t *testing.T) {
	for _, v := range []int16{8, 12} {
		emptyBrokers := []MetadataResponseBroker{}
		emptyTopics := []MetadataResponseTopic{}

		out := roundtripResponse(t, v, &MetadataResponse{Brokers: &emptyBrokers, Topics: &emptyTopics, ClusterId: nil})
		if out.ClusterId != nil {
			t.Errorf("v%d: nil ClusterId round-tripped to %q", v, *out.ClusterId)
		}

		out = roundtripResponse(t, v, &MetadataResponse{Brokers: &emptyBrokers, Topics: &emptyTopics, ClusterId: strPtr("my-cluster")})
		if out.ClusterId == nil || *out.ClusterId != "my-cluster" {
			t.Errorf("v%d: ClusterId value not preserved: %v", v, out.ClusterId)
		}
	}
}

func TestNullValidationOnEncode(t *testing.T) {
	// MetadataRequest.Topics is present from v0 but nullable only from v1: nil is illegal at v0.
	v0 := &MetadataRequest{Topics: nil}
	v0.ApiVersion = 0
	if err := v0.Write(&bytes.Buffer{}); err == nil {
		t.Errorf("v0: expected an error writing nil non-nullable Topics, got nil")
	}

	// From v1 the field is nullable, so nil is fine.
	v1 := &MetadataRequest{Topics: nil}
	v1.ApiVersion = 1
	if err := v1.Write(&bytes.Buffer{}); err != nil {
		t.Errorf("v1: nil Topics should be allowed, got %v", err)
	}

	// MetadataResponse.Brokers is never nullable: nil is always illegal (no more nil panic).
	resp := &MetadataResponse{Brokers: nil}
	resp.ApiVersion = 12
	if err := resp.Write(&bytes.Buffer{}); err == nil {
		t.Errorf("expected an error writing nil non-nullable Brokers, got nil")
	}
}

func TestNullableArrayRoundtrip(t *testing.T) {
	for _, v := range []int16{8, 12} {
		out := roundtripRequest(t, v, &MetadataRequest{Topics: nil})
		if out.Topics != nil {
			t.Errorf("v%d: nil Topics round-tripped to %v", v, *out.Topics)
		}

		topics := []MetadataRequestTopic{{Name: strPtr("t1")}}
		out = roundtripRequest(t, v, &MetadataRequest{Topics: &topics})
		if out.Topics == nil || len(*out.Topics) != 1 || *(*out.Topics)[0].Name != "t1" {
			t.Errorf("v%d: Topics value not preserved: %v", v, out.Topics)
		}
	}
}
