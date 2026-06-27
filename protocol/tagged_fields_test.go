package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestWriteRawTaggedFields(t *testing.T) {
	tests := []struct {
		name   string
		fields []TaggedField
	}{
		{
			name:   "empty",
			fields: []TaggedField{},
		},
		{
			name: "single field",
			fields: []TaggedField{
				{Tag: 1, Field: []byte("test")},
			},
		},
		{
			name: "multiple fields",
			fields: []TaggedField{
				{Tag: 0, Field: []byte("field0")},
				{Tag: 1, Field: []byte("field1")},
				{Tag: 2, Field: []byte("field2")},
			},
		},
		{
			name: "empty field",
			fields: []TaggedField{
				{Tag: 1, Field: []byte{}},
			},
		},
		{
			name: "large tag numbers",
			fields: []TaggedField{
				{Tag: 100, Field: []byte("large tag")},
				{Tag: 1000, Field: []byte("very large tag")},
			},
		},
		{
			name: "binary data",
			fields: []TaggedField{
				{Tag: 1, Field: []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteRawTaggedFields(&buf, tt.fields)
			if err != nil {
				t.Fatalf("WriteRawTaggedFields failed: %v", err)
			}

			result, err := ReadRawTaggedFields(&buf)
			if err != nil {
				t.Fatalf("ReadRawTaggedFields failed: %v", err)
			}

			if len(result) != len(tt.fields) {
				t.Fatalf("TaggedFields length mismatch: expected %d, got %d", len(tt.fields), len(result))
			}

			for i := range tt.fields {
				if result[i].Tag != tt.fields[i].Tag {
					t.Errorf("TaggedFields[%d].Tag mismatch: expected %d, got %d", i, tt.fields[i].Tag, result[i].Tag)
				}

				if !bytes.Equal(result[i].Field, tt.fields[i].Field) {
					t.Errorf("TaggedFields[%d].Field mismatch: expected %v, got %v", i, tt.fields[i].Field, result[i].Field)
				}
			}
		})
	}
}

func TestReadRawTaggedField(t *testing.T) {
	tests := []struct {
		name  string
		field TaggedField
	}{
		{
			name:  "simple field",
			field: TaggedField{Tag: 1, Field: []byte("test")},
		},
		{
			name:  "empty field",
			field: TaggedField{Tag: 0, Field: []byte{}},
		},
		{
			name:  "large tag",
			field: TaggedField{Tag: 1000, Field: []byte("large tag")},
		},
		{
			name:  "binary data",
			field: TaggedField{Tag: 5, Field: []byte{0x00, 0x01, 0x02, 0xFF}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			// Write the field manually
			err := WriteUvarint(&buf, tt.field.Tag)
			if err != nil {
				t.Fatalf("WriteUvarint (tag) failed: %v", err)
			}

			err = WriteUvarint(&buf, uint64(len(tt.field.Field)))
			if err != nil {
				t.Fatalf("WriteUvarint (length) failed: %v", err)
			}

			_, err = buf.Write(tt.field.Field)
			if err != nil {
				t.Fatalf("Write (field) failed: %v", err)
			}

			result, err := ReadRawTaggedField(&buf)
			if err != nil {
				t.Fatalf("ReadRawTaggedField failed: %v", err)
			}

			if result.Tag != tt.field.Tag {
				t.Errorf("Tag mismatch: expected %d, got %d", tt.field.Tag, result.Tag)
			}

			if !bytes.Equal(result.Field, tt.field.Field) {
				t.Errorf("Field mismatch: expected %v, got %v", tt.field.Field, result.Field)
			}
		})
	}
}

func TestReadTaggedFields(t *testing.T) {
	// Create test tagged fields
	fields := []TaggedField{
		{Tag: 0, Field: []byte("field0")},
		{Tag: 1, Field: []byte("field1")},
		{Tag: 2, Field: []byte("field2")},
	}

	var buf bytes.Buffer
	err := WriteRawTaggedFields(&buf, fields)
	if err != nil {
		t.Fatalf("WriteRawTaggedFields failed: %v", err)
	}

	// Track what we read
	readFields := make(map[uint64][]byte)

	decoder := func(r io.Reader, tag uint64, length uint64) error {
		field := make([]byte, length)
		_, err := io.ReadFull(r, field)
		if err != nil {
			return err
		}
		readFields[tag] = field
		return nil
	}

	err = ReadTaggedFields(&buf, decoder)
	if err != nil {
		t.Fatalf("ReadTaggedFields failed: %v", err)
	}

	if len(readFields) != len(fields) {
		t.Fatalf("Read fields count mismatch: expected %d, got %d", len(fields), len(readFields))
	}

	for _, field := range fields {
		readField, exists := readFields[field.Tag]
		if !exists {
			t.Errorf("Tag %d not found in read fields", field.Tag)
			continue
		}

		if !bytes.Equal(readField, field.Field) {
			t.Errorf("Field for tag %d mismatch: expected %v, got %v", field.Tag, field.Field, readField)
		}
	}
}

func TestReadTaggedFieldsEmpty(t *testing.T) {
	fields := []TaggedField{}

	var buf bytes.Buffer
	err := WriteRawTaggedFields(&buf, fields)
	if err != nil {
		t.Fatalf("WriteRawTaggedFields failed: %v", err)
	}

	decoder := func(r io.Reader, tag uint64, length uint64) error {
		t.Errorf("Decoder should not be called for empty tagged fields")
		return nil
	}

	err = ReadTaggedFields(&buf, decoder)
	if err != nil {
		t.Fatalf("ReadTaggedFields failed: %v", err)
	}
}

func TestReadTaggedFieldsDecoderError(t *testing.T) {
	fields := []TaggedField{
		{Tag: 1, Field: []byte("test")},
	}

	var buf bytes.Buffer
	err := WriteRawTaggedFields(&buf, fields)
	if err != nil {
		t.Fatalf("WriteRawTaggedFields failed: %v", err)
	}

	expectedError := errors.New("decoder error")
	decoder := func(r io.Reader, tag uint64, length uint64) error {
		return expectedError
	}

	err = ReadTaggedFields(&buf, decoder)
	if err != expectedError {
		t.Errorf("ReadTaggedFields should return decoder error, got: %v", err)
	}
}

// #10: WriteRawTaggedFields must emit tags in ascending order, on a copy (caller slice untouched).
func TestWriteRawTaggedFieldsSortsWithoutMutating(t *testing.T) {
	fields := []TaggedField{{Tag: 5, Field: []byte{0x05}}, {Tag: 1, Field: []byte{0x01}}}

	var buf bytes.Buffer
	if err := WriteRawTaggedFields(&buf, fields); err != nil {
		t.Fatalf("write: %v", err)
	}
	if fields[0].Tag != 5 {
		t.Fatalf("caller slice was reordered: %+v", fields)
	}

	got, err := ReadRawTaggedFields(&buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2 || got[0].Tag != 1 || got[1].Tag != 5 {
		t.Fatalf("tags not written in ascending order: %+v", got)
	}
}

// #10: duplicate tags are invalid and must be rejected.
func TestWriteRawTaggedFieldsRejectsDuplicates(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteRawTaggedFields(&buf, []TaggedField{{Tag: 1}, {Tag: 1}}); err == nil {
		t.Fatal("expected an error for duplicate tagged field tags")
	}
}

// #7: tagged fields must arrive in strictly ascending order with no duplicates; the readers reject
// anything else rather than tolerating malformed wire data.
func TestReadRawTaggedFieldsRejectsOutOfOrder(t *testing.T) {
	var buf bytes.Buffer
	mustU(t, &buf, 2) // two tags
	mustU(t, &buf, 5) // tag 5 ...
	mustU(t, &buf, 0)
	mustU(t, &buf, 1) // ... then tag 1 (descending)
	mustU(t, &buf, 0)
	if _, err := ReadRawTaggedFields(&buf); err == nil {
		t.Fatal("ReadRawTaggedFields accepted out-of-order tags")
	}
}

func TestReadRawTaggedFieldsRejectsDuplicate(t *testing.T) {
	var buf bytes.Buffer
	mustU(t, &buf, 2)
	mustU(t, &buf, 3)
	mustU(t, &buf, 0)
	mustU(t, &buf, 3) // duplicate tag
	mustU(t, &buf, 0)
	if _, err := ReadRawTaggedFields(&buf); err == nil {
		t.Fatal("ReadRawTaggedFields accepted a duplicate tag")
	}
}

func TestReadTaggedFieldsRejectsOutOfOrder(t *testing.T) {
	var buf bytes.Buffer
	mustU(t, &buf, 2)
	mustU(t, &buf, 7)
	mustU(t, &buf, 0)
	mustU(t, &buf, 2) // descending
	mustU(t, &buf, 0)
	noop := func(io.Reader, uint64, uint64) error { return nil }
	if err := ReadTaggedFields(&buf, noop); err == nil {
		t.Fatal("ReadTaggedFields accepted out-of-order tags")
	}
}

func mustU(t *testing.T, buf *bytes.Buffer, v uint64) {
	t.Helper()
	if err := WriteUvarint(buf, v); err != nil {
		t.Fatal(err)
	}
}

// #6: a tagged-fields count near the uint64 max must not be truncated by an int conversion (which
// would overflow on a 32-bit build); the readers loop on the uint64 directly and simply fail fast
// when the claimed tags are not actually present.
func TestReadTaggedFieldsHugeCountFailsFast(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteUvarint(&buf, 1<<33); err != nil { // a count that overflows a 32-bit int
		t.Fatal(err)
	}
	noop := func(io.Reader, uint64, uint64) error { return nil }
	if err := ReadTaggedFields(&buf, noop); err == nil {
		t.Fatal("expected an error for an over-claimed tagged-fields count")
	}

	var rbuf bytes.Buffer
	if err := WriteUvarint(&rbuf, 1<<33); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRawTaggedFields(&rbuf); err == nil {
		t.Fatal("expected an error for an over-claimed raw tagged-fields count")
	}
}
