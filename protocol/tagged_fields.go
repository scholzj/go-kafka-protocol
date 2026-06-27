package protocol

import (
	"bytes"
	"fmt"
	"io"
	"sort"
)

////////////////////
// Tagged fields methods
////////////////////

type TaggedField struct {
	Tag   uint64
	Field []byte
}

// WriteRawTaggedFields writes a tagged-fields section. Kafka requires the tags to be serialised in
// strictly ascending tag order with no duplicates, so the fields are sorted (a stable sort, on a
// copy, leaving the caller's slice untouched) and checked for duplicate tags before writing.
func WriteRawTaggedFields(w io.Writer, fields []TaggedField) error {
	if len(fields) > 1 {
		sorted := make([]TaggedField, len(fields))
		copy(sorted, fields)
		sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Tag < sorted[j].Tag })
		for i := 1; i < len(sorted); i++ {
			if sorted[i].Tag == sorted[i-1].Tag {
				return fmt.Errorf("duplicate tagged field tag %d", sorted[i].Tag)
			}
		}
		fields = sorted
	}

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
	// preallocLen bounds the initial capacity so a corrupt/huge tag count cannot force a giant
	// allocation before any tag bytes have been read.
	// Loop on the uint64 count directly: converting it to int for the loop bound would overflow on a
	// 32-bit build and silently truncate the number of tags parsed.
	rawTaggedFields := make([]TaggedField, 0, preallocLen(int(l)))
	var prevTag uint64
	for i := uint64(0); i < l; i++ {
		taggedField, err := ReadRawTaggedField(r)
		if err != nil {
			return nil, err
		}
		// Kafka requires tagged fields to arrive in strictly ascending tag order with no duplicates;
		// reject anything else rather than silently tolerating malformed wire data.
		if i > 0 && taggedField.Tag <= prevTag {
			return nil, fmt.Errorf("tagged fields out of order or duplicated: tag %d after %d", taggedField.Tag, prevTag)
		}
		prevTag = taggedField.Tag
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

	// readBytesLimited reads exactly tagLength bytes without pre-allocating the full declared
	// length up front, so a corrupt/huge length cannot force a giant allocation before the bytes
	// are actually available.
	rawTaggedField, err := readBytesLimited(r, int(tagLength))
	if err != nil {
		return taggedField, err
	}
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

	// Loop on the uint64 count directly so a large count cannot overflow int on a 32-bit build.
	var prevTag uint64
	for i := uint64(0); i < l; i++ {
		// Read the tag number first
		tag, err := ReadUvarint(r)
		if err != nil {
			return err
		}

		// Kafka requires tagged fields in strictly ascending tag order with no duplicates.
		if i > 0 && tag <= prevTag {
			return fmt.Errorf("tagged fields out of order or duplicated: tag %d after %d", tag, prevTag)
		}
		prevTag = tag

		// Read the tag length
		tagLength, err := ReadUvarint(r)
		if err != nil {
			return err
		}

		// Read exactly tagLength bytes for this tag and decode from a reader bounded to them.
		// This keeps any unknown trailing bytes (e.g. fields a newer peer added to a known
		// tagged struct) confined to the field, so they cannot desync the rest of the message.
		// readBytesLimited bounds the up-front allocation so a corrupt/huge length cannot force a
		// giant allocation before the bytes are available.
		field, err := readBytesLimited(r, int(tagLength))
		if err != nil {
			return err
		}

		err = decoder(bytes.NewReader(field), tag, tagLength)
		if err != nil {
			return err
		}
	}

	return nil
}
