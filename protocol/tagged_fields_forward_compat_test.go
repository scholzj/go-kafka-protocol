package protocol

import (
	"bytes"
	"io"
	"testing"
)

// ReadTaggedFields must bound each tag's decoder to the tag's declared length. A decoder that reads
// fewer bytes than declared (e.g. an older schema that doesn't know about fields a newer peer added
// to a known tagged struct) must not consume into the following tag and desync the stream.
func TestReadTaggedFieldsBoundsDecoderToTagLength(t *testing.T) {
	var buf bytes.Buffer
	_ = WriteUvarint(&buf, 2) // two tagged fields

	// Tag 5 declares 3 bytes, but our decoder will only read the first one.
	_ = WriteUvarint(&buf, 5)
	_ = WriteUvarint(&buf, 3)
	buf.Write([]byte{0x01, 0xAA, 0xBB})

	// Tag 6 declares 1 byte.
	_ = WriteUvarint(&buf, 6)
	_ = WriteUvarint(&buf, 1)
	buf.Write([]byte{0x07})

	seen := map[uint64]byte{}
	err := ReadTaggedFields(bytes.NewReader(buf.Bytes()), func(r io.Reader, tag uint64, tagLength uint64) error {
		b := make([]byte, 1) // intentionally read only one byte regardless of tagLength
		if _, err := io.ReadFull(r, b); err != nil {
			return err
		}
		seen[tag] = b[0]
		return nil
	})
	if err != nil {
		t.Fatalf("ReadTaggedFields: %v", err)
	}

	if seen[5] != 0x01 {
		t.Errorf("tag 5: got %#x, want 0x01", seen[5])
	}
	// Without length bounding this would read 0xAA (the leftover of tag 5) and desync.
	if seen[6] != 0x07 {
		t.Errorf("tag 6: got %#x, want 0x07 - decoder desynced", seen[6])
	}
}
