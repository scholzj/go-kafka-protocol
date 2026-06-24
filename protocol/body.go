package protocol

import "io"

// RequestBody is implemented by every generated Kafka request body struct (for
// example metadata.MetadataRequest). It decodes itself from an already-framed
// Request, re-encodes its body to a writer, and renders a human-readable form.
//
// These interfaces live in the protocol package (rather than alongside the
// generated structs) so that consumers - proxies, dumpers, test harnesses - can
// handle request/response bodies generically without importing every api/*
// package. The concrete constructors that map an API key to the right struct are
// generated into the leaf "messages" package, which is the one that pulls in all
// of the api/* packages.
type RequestBody interface {
	Read(*Request) error
	Write(io.Writer) error
	PrettyPrint() string
}

// ResponseBody is implemented by every generated Kafka response body struct (for
// example metadata.MetadataResponse). See RequestBody for the rationale behind
// keeping these interfaces in the protocol package.
type ResponseBody interface {
	Read(*Response) error
	Write(io.Writer) error
	PrettyPrint() string
}
