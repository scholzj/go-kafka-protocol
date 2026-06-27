package protocol

import (
	"bytes"
	"io"
	"testing"
)

// These tests guard the bounded-prealloc hardening: a corrupt or malicious length field must not
// cause a huge up-front allocation. Each decodes a tiny input that claims a vast length; with the
// fix they fail fast with an error, and without it they would attempt a multi-gigabyte allocation.

func TestReadNullableCompactArrayDoesNotOverAllocate(t *testing.T) {
	var buf bytes.Buffer
	// Compact arrays encode length+1; claim ~2 billion elements but provide no element data.
	if err := WriteUvarint(&buf, uint64(2_000_000_000)+1); err != nil {
		t.Fatal(err)
	}

	r := bytes.NewReader(buf.Bytes())
	_, err := ReadNullableCompactArray(r, func(r io.Reader) (int32, error) {
		return ReadInt32(r)
	})
	if err == nil {
		t.Fatal("expected an error decoding a truncated huge array, got nil")
	}
}

func TestReadArrayDoesNotOverAllocate(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteInt32(&buf, 1_000_000_000); err != nil { // claim 1 billion elements
		t.Fatal(err)
	}

	r := bytes.NewReader(buf.Bytes())
	_, err := ReadArray(r, func(r io.Reader) (int64, error) {
		return ReadInt64(r)
	})
	if err == nil {
		t.Fatal("expected an error decoding a truncated huge array, got nil")
	}
}

func TestReadCompactBytesDoesNotOverAllocate(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteUvarint(&buf, uint64(3_000_000_000)+1); err != nil { // ~3 GB claimed
		t.Fatal(err)
	}

	r := bytes.NewReader(buf.Bytes())
	if _, err := ReadCompactBytes(r); err == nil {
		t.Fatal("expected an error decoding truncated huge bytes, got nil")
	}
}

// Sanity check that the bounded path still decodes a normal value correctly.
func TestReadCompactBytesRoundTripStillWorks(t *testing.T) {
	want := []byte("hello world")
	var buf bytes.Buffer
	if err := WriteCompactBytes(&buf, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadCompactBytes(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("round trip = %q, want %q", got, want)
	}
}

// #5: a tagged-fields frame claiming an absurd tag count must fail fast instead of pre-allocating it.
func TestReadRawTaggedFieldsBoundedAllocation(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteUvarint(&buf, 1<<40); err != nil { // huge count, but no tags follow
		t.Fatal(err)
	}
	if _, err := ReadRawTaggedFields(&buf); err == nil {
		t.Fatal("expected an error reading a truncated, over-claimed tagged-fields section")
	}
}

// #5: an individual tag claiming an absurd length must fail fast instead of pre-allocating it.
func TestReadRawTaggedFieldBoundedAllocation(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteUvarint(&buf, 1); err != nil { // tag number
		t.Fatal(err)
	}
	if err := WriteUvarint(&buf, 1<<40); err != nil { // huge length, but no payload follows
		t.Fatal(err)
	}
	if _, err := ReadRawTaggedField(&buf); err == nil {
		t.Fatal("expected an error reading a truncated, over-claimed tag")
	}
}
