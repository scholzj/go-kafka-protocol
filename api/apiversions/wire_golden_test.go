package apiversions

import (
	"bytes"
	"testing"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

// Golden wire-format vectors derived by hand from the Kafka protocol spec. The generated round-trip
// tests only prove Write and Read agree with *each other*; these pin the actual bytes, so they also
// catch a symmetric mistake (e.g. the wrong length encoding on both sides).

func TestApiVersionsRequestV3WireFormat(t *testing.T) {
	// v3 is the first flexible version: compact strings + an (empty) tagged-fields section.
	//   ClientSoftwareName    "a" -> compact string uvarint(len+1)=0x02, 'a'=0x61
	//   ClientSoftwareVersion "b" -> 0x02, 'b'=0x62
	//   tagged fields (none)      -> uvarint(0) = 0x00
	want := []byte{0x02, 0x61, 0x02, 0x62, 0x00}

	name, version := "a", "b"
	in := &ApiVersionsRequest{ClientSoftwareName: &name, ClientSoftwareVersion: &version}
	in.ApiVersion = 3

	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("encoded = %x, want %x", buf.Bytes(), want)
	}

	out := &ApiVersionsRequest{}
	req := &protocol.Request{Body: bytes.NewBuffer(want)}
	req.ApiVersion = 3
	if err := out.Read(req); err != nil {
		t.Fatalf("read: %v", err)
	}
	if out.ClientSoftwareName == nil || *out.ClientSoftwareName != "a" {
		t.Errorf("ClientSoftwareName = %v, want \"a\"", out.ClientSoftwareName)
	}
	if out.ClientSoftwareVersion == nil || *out.ClientSoftwareVersion != "b" {
		t.Errorf("ClientSoftwareVersion = %v, want \"b\"", out.ClientSoftwareVersion)
	}
}

func TestApiVersionsResponseV0WireFormat(t *testing.T) {
	// v0 is NOT flexible: int16 ErrorCode + a non-compact (int32 length) array, no tagged section.
	//   ErrorCode = 0              -> 0x0000
	//   ApiKeys length = 1         -> 0x00000001
	//   element ApiKey/Min/Max=1,2,3 (int16 each, no per-element tagged section at v0)
	want := []byte{
		0x00, 0x00, // ErrorCode
		0x00, 0x00, 0x00, 0x01, // array length
		0x00, 0x01, 0x00, 0x02, 0x00, 0x03, // ApiKey, MinVersion, MaxVersion
	}

	in := &ApiVersionsResponse{
		ApiKeys: &[]ApiVersionsResponseApiKey{{ApiKey: 1, MinVersion: 2, MaxVersion: 3}},
	}
	in.ApiVersion = 0

	var buf bytes.Buffer
	if err := in.Write(&buf); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("encoded = %x, want %x", buf.Bytes(), want)
	}

	out := &ApiVersionsResponse{}
	resp := &protocol.Response{Body: bytes.NewBuffer(want)}
	resp.ApiVersion = 0
	if err := out.Read(resp); err != nil {
		t.Fatalf("read: %v", err)
	}
	if out.ApiKeys == nil || len(*out.ApiKeys) != 1 {
		t.Fatalf("ApiKeys = %v, want 1 element", out.ApiKeys)
	}
	if k := (*out.ApiKeys)[0]; k.ApiKey != 1 || k.MinVersion != 2 || k.MaxVersion != 3 {
		t.Errorf("ApiKeys[0] = %+v, want {ApiKey:1 MinVersion:2 MaxVersion:3}", k)
	}
}
