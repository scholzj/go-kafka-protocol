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
