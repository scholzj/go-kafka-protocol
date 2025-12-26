package protocol

import (
	"bytes"
	"fmt"
	"io"

	"github.com/scholzj/go-kafka-protocol/apis"
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

	if apis.RequestHeaderVersion(r.ApiKey, r.ApiVersion) >= 2 {
		err = WriteRawTaggedFields(buf, make([]TaggedField, 0))
		if err != nil {
			return err
		}
	}

	_, err = io.Copy(buf, r.Body)
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
		if err == io.EOF {
			return request, err
		} else {
			return request, err
		}
	}
	request.Size = size

	requestReader := io.LimitReader(r, int64(request.Size))

	apiKey, err := ReadInt16(requestReader)
	if err != nil {
		return request, err
	}
	request.ApiKey = apiKey

	apiVersion, err := ReadInt16(requestReader)
	if err != nil {
		return request, err
	}
	request.ApiVersion = apiVersion

	correlationId, err := ReadInt32(requestReader)
	if err != nil {
		return request, err
	}
	request.CorrelationId = correlationId

	// Decode client ID
	clientId, err := ReadNullableString(requestReader)
	if err != nil {
		return request, err
	}
	request.ClientId = clientId

	// Decode tagged fields
	if apis.RequestHeaderVersion(request.ApiKey, request.ApiVersion) >= 2 {
		_, err = ReadRawTaggedFields(requestReader)
		if err != nil {
			return request, err
		}
	}

	// Read the body
	request.Body, err = readBody(requestReader)
	if err != nil {
		return request, err
	}

	return request, nil
}

func (r *Response) Write(w io.Writer) error {
	buf := bytes.NewBuffer(make([]byte, 0))

	err := WriteInt32(buf, r.CorrelationId)
	if err != nil {
		return err
	}

	if apis.ResponseHeaderVersion(r.ApiKey, r.ApiVersion) >= 1 {
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
		if err == io.EOF {
			return response, err
		} else {
			return response, err
		}
	}

	responseReader := io.LimitReader(r, int64(response.Size))

	response.CorrelationId, err = ReadInt32(responseReader)
	if err != nil {
		return response, err
	}

	requestHeader, ok := correlations[response.CorrelationId]
	if !ok {
		return response, fmt.Errorf("no correlation found for correlationId %d", response.CorrelationId)
	}

	response.ApiKey = requestHeader.ApiKey
	response.ApiVersion = requestHeader.ApiVersion
	response.ClientId = requestHeader.ClientId

	// Decode tagged fields
	if apis.ResponseHeaderVersion(response.ApiKey, response.ApiVersion) >= 1 {
		_, err := ReadRawTaggedFields(responseReader)
		if err != nil {
			return response, err
		}
	}

	// Read the body
	response.Body, err = readBody(responseReader)
	if err != nil {
		return response, err
	}

	return response, nil
}

func readBody(r io.Reader) (*bytes.Buffer, error) {
	body := bytes.NewBuffer(make([]byte, 0))
	_, err := io.Copy(body, r)
	return body, err
}
