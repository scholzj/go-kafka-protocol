package protocol

import (
	"io"
)

////////////////////
// Tagged fields methods
////////////////////

type TaggedField struct {
	Tag   uint64
	Field []byte
}

func WriteRawTaggedFields(w io.Writer, fields []TaggedField) error {
	err := WriteUvarint(w, uint64(len(fields)))
	if err != nil {
		return err
	}

	for _, field := range fields {
		err := WriteUvarint(w, field.Tag)
		if err != nil {
			return err
		}

		err = WriteUvarint(w, uint64(len(field.Field)))
		if err != nil {
			return err
		}

		_, err = w.Write(field.Field)
		if err != nil {
			return err
		}
	}

	return nil
}

func ReadRawTaggedFields(r io.Reader) ([]TaggedField, error) {
	// Find the number of tags
	l, err := ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	// Read each tag and store it in the slice of slices
	//(we need to partially decode them to know how many bytes are the raw tags)
	rawTaggedFields := make([]TaggedField, 0, l)
	for i := 0; i < int(l); i++ {
		taggedField, err := ReadRawTaggedField(r)
		if err != nil {
			return nil, err
		}
		rawTaggedFields = append(rawTaggedFields, taggedField)
	}

	return rawTaggedFields, nil
}

func ReadRawTaggedField(r io.Reader) (TaggedField, error) {
	taggedField := TaggedField{}

	// Read the tag number first
	tag, err := ReadUvarint(r)
	if err != nil {
		return taggedField, err
	}
	taggedField.Tag = tag

	// Read the tag length
	tagLength, err := ReadUvarint(r)
	if err != nil {
		return taggedField, err
	}

	rawTaggedField := make([]byte, tagLength)
	_, err = r.Read(rawTaggedField)
	taggedField.Field = rawTaggedField

	return taggedField, nil
}

type TaggedFieldsDecoder func(io.Reader, uint64, uint64) error

func ReadTaggedFields(r io.Reader, decoder TaggedFieldsDecoder) error {
	// Find the number of tags
	l, err := ReadUvarint(r)
	if err != nil {
		return err
	}

	for i := 0; i < int(l); i++ {
		// Read the tag number first
		tag, err := ReadUvarint(r)
		if err != nil {
			return err
		}

		// Read the tag length
		tagLength, err := ReadUvarint(r)
		if err != nil {
			return err
		}

		// Use the decoded to decode the fields
		err = decoder(r, tag, tagLength)
		if err != nil {
			return err
		}
	}

	return nil
}
