package protocol

import (
	"bytes"
	"fmt"
	"io"
)

////////////////////
// Structs to decode the headers
////////////////////

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      *string
}

type Request struct {
	RequestHeader
	Size int32
	Body *bytes.Buffer
}

type ResponseHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      *string
}

type Response struct {
	ResponseHeader
	Size int32
	Body *bytes.Buffer
}

////////////////////
// Methods to decode and encode requests and responses
////////////////////

// 4 bytes -> size (not included in size)
// 2 bytes -> apiKey
// 2 bytes -> apiVersion
// 4 bytes -> correlationId
// 2 + ? bytes -> Client ID
// ? bytes tagged fields, but should be always 1?

func (r *Request) Write(w io.Writer) error {
	buf := bytes.NewBuffer(make([]byte, 0))

	err := WriteInt16(buf, r.ApiKey)
	if err != nil {
		return err
	}

	err = WriteInt16(buf, r.ApiVersion)
	if err != nil {
		return err
	}

	err = WriteInt32(buf, r.CorrelationId)
	if err != nil {
		return err
	}

	err = WriteNullableString(buf, r.ClientId)
	if err != nil {
		return err
	}

	// TODO: Check header version
	err = WriteRawTaggedFields(buf, make([]TaggedField, 0))
	if err != nil {
		return err
	}

	_, err = io.Copy(buf, r.Body)
	//_, err = buf.Write(r.Body.Bytes())
	if err != nil {
		return err
	}

	err = WriteInt32(w, int32(buf.Len()))
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(w)
	if err != nil {
		return err
	}

	return nil
}

func ReadRequest(r io.Reader) (Request, error) {
	request := Request{}

	size, err := ReadInt32(r)
	if err != nil {
		fmt.Println("Failed to decode request size", err)
		return request, err
	}
	request.Size = size

	fmt.Printf("Reading request size is %d bytes\n", size)

	requestReader := io.LimitReader(r, int64(request.Size))

	apiKey, err := ReadInt16(requestReader)
	if err != nil {
		fmt.Println("Failed to decode apiKey", err)
		return request, err
	}
	request.ApiKey = apiKey

	apiVersion, err := ReadInt16(requestReader)
	if err != nil {
		fmt.Println("Failed to decode apiVersion", err)
		return request, err
	}
	request.ApiVersion = apiVersion

	correlationId, err := ReadInt32(requestReader)
	if err != nil {
		fmt.Println("Failed to decode correlationID", err)
		return request, err
	}
	request.CorrelationId = correlationId

	// Decode client ID
	clientId, err := ReadNullableString(requestReader)
	if err != nil {
		fmt.Println("Failed to decode clientId", err)
		return request, err
	}
	request.ClientId = clientId

	// TODO: Check header version
	// Decode tagged fields
	_, err = ReadRawTaggedFields(requestReader)
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
		return request, err
	}

	// Read the body
	request.Body, err = readBody(requestReader)
	if err != nil {
		fmt.Println("Failed to read the body", err)
		return request, err
	}

	return request, nil
}

func DecodeRequest(b []byte) (Request, error) {
	request := Request{}
	offset := 0

	size, c, err := DecodeInt32(b[offset:])
	//c, err := binary.Decode(b, binary.BigEndian, &request.Size)
	if err != nil {
		fmt.Println("Failed to decode request size", err)
		return request, err
	}
	request.Size = size
	offset += c
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	apiKey, c, err := DecodeInt16(b[offset:])
	if err != nil {
		fmt.Println("Failed to decode apiKey", err)
		return request, err
	}
	request.ApiKey = apiKey
	offset += c
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	apiVersion, c, err := DecodeInt16(b[offset:])
	if err != nil {
		fmt.Println("Failed to decode apiVersion", err)
		return request, err
	}
	request.ApiVersion = apiVersion
	offset += c
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	correlationId, c, err := DecodeInt32(b[offset:])
	if err != nil {
		fmt.Println("Failed to decode correlationID", err)
		return request, err
	}
	request.CorrelationId = correlationId
	offset += c
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	// Decode client ID
	clientId, c, err := DecodeNullableString(b[offset:])
	if err != nil {
		fmt.Println("Failed to decode clientId", err)
		return request, err
	}
	offset += c
	request.ClientId = clientId
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	// TODO: Check header version
	// Decode tagged fields
	_, c, err = DecodeUvarint(b[offset:])
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
		return request, err
	}
	offset += c
	fmt.Printf("Tagged fields length: %d\n", c)
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	request.Body = bytes.NewBuffer(b[offset : request.Size+4]) // We add 4 here because the first 4 b are the size

	return request, nil
}

func (r *Response) Write(w io.Writer) error {
	buf := bytes.NewBuffer(make([]byte, 0))

	err := WriteInt32(buf, r.CorrelationId)
	if err != nil {
		return err
	}

	// TODO: Check header version
	if r.ApiKey != 18 {
		err = WriteRawTaggedFields(buf, make([]TaggedField, 0))
		if err != nil {
			return err
		}
	}

	_, err = buf.Write(r.Body.Bytes())
	if err != nil {
		return err
	}

	err = WriteInt32(w, int32(buf.Len()))
	if err != nil {
		return err
	}

	_, err = buf.WriteTo(w)
	if err != nil {
		return err
	}

	return nil
}

func ReadResponse(r io.Reader, correlations map[int32]RequestHeader) (Response, error) {
	var err error
	response := Response{}

	response.Size, err = ReadInt32(r)
	if err != nil {
		fmt.Println("Failed to decode response size", err)
		return response, err
	}

	responseReader := io.LimitReader(r, int64(response.Size))

	response.CorrelationId, err = ReadInt32(responseReader)
	if err != nil {
		fmt.Println("Failed to decode correlationID", err)
		return response, err
	}

	requestHeader, ok := correlations[response.CorrelationId]
	if !ok {
		return response, fmt.Errorf("no correlation found for correlationId %d", response.CorrelationId)
	}

	response.ApiKey = requestHeader.ApiKey
	response.ApiVersion = requestHeader.ApiVersion
	response.ClientId = requestHeader.ClientId

	// TODO: Check response version header
	if response.ApiKey != 18 {
		// Decode tagged fields
		_, err := ReadRawTaggedFields(responseReader)
		if err != nil {
			fmt.Println("Failed to decode tagged fields", err)
			return response, err
		}
	}

	// Read the body
	response.Body, err = readBody(responseReader)
	if err != nil {
		fmt.Println("Failed to read the body", err)
		return response, err
	}

	return response, nil
}

func DecodeResponse(b []byte, correlations map[int32]RequestHeader) (Response, error) {
	response := Response{}
	offset := 0

	size, c, err := DecodeInt32(b[offset:])
	if err != nil {
		fmt.Println("Failed to decode response size", err)
		return response, err
	}
	response.Size = size
	offset += c
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	correlationId, c, err := DecodeInt32(b[offset:])
	if err != nil {
		fmt.Println("Failed to decode correlationID", err)
		return response, err
	}
	offset += c

	requestHeader, ok := correlations[correlationId]
	if !ok {
		return response, fmt.Errorf("no correlation found for correlationId %d", correlationId)
	}

	response.ApiKey = requestHeader.ApiKey
	response.ApiVersion = requestHeader.ApiVersion
	response.CorrelationId = correlationId
	response.ClientId = requestHeader.ClientId

	// TODO: Check header version
	if response.ApiKey != 18 {
		// Decode tagged fields
		l, c, err := DecodeUvarint(b[offset:])
		if err != nil {
			fmt.Println("Failed to decode tagged fields", err)
			return response, err
		}
		offset += c
		fmt.Printf("Tagged fields length: %d b, %d records\n", c, l)
		fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)
	}

	response.Body = bytes.NewBuffer(b[offset : response.Size+4]) // We add 4 here because the first 4 b are the size

	return response, nil
}

func readBody(r io.Reader) (*bytes.Buffer, error) {
	body := bytes.NewBuffer(make([]byte, 0))
	_, err := io.Copy(body, r)
	return body, err
}
