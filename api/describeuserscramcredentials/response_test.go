package describeuserscramcredentials

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated DescribeUserScramCredentialsResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestDescribeUserScramCredentialsResponseRoundTrip(t *testing.T) {
	in := &DescribeUserScramCredentialsResponse{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		ErrorMessage:   resPtr("x"),
		Results: &[]DescribeUserScramCredentialsResponseResult{DescribeUserScramCredentialsResponseResult{
			User:         resPtr("x"),
			ErrorCode:    1,
			ErrorMessage: resPtr("x"),
			CredentialInfos: &[]DescribeUserScramCredentialsResponseResultCredentialInfo{DescribeUserScramCredentialsResponseResultCredentialInfo{
				Mechanism:  1,
				Iterations: 1,
			}},
		}},
	}

	// A second instance with every always-nullable field set to nil. The fully-populated
	// instance never encodes a null, so this is what actually exercises the null-marker
	// write/read paths (nullable single structs, nullable arrays, nullable strings/bytes).
	inNulls := &DescribeUserScramCredentialsResponse{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		ErrorMessage:   nil,
		Results: &[]DescribeUserScramCredentialsResponseResult{DescribeUserScramCredentialsResponseResult{
			User:         resPtr("x"),
			ErrorCode:    1,
			ErrorMessage: nil,
			CredentialInfos: &[]DescribeUserScramCredentialsResponseResultCredentialInfo{DescribeUserScramCredentialsResponseResultCredentialInfo{
				Mechanism:  1,
				Iterations: 1,
			}},
		}},
	}

	for v := int16(0); v <= 0; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: populated write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &DescribeUserScramCredentialsResponse{}
			response := &protocol.Response{Body: bytes.NewBuffer(encoded)}
			response.ApiVersion = v
			if err := out.Read(response); err != nil {
				t.Fatalf("v%d: populated read: %v", v, err)
			}

			var reencoded bytes.Buffer
			if err := out.Write(&reencoded); err != nil {
				t.Fatalf("v%d: populated re-write: %v", v, err)
			}
			if !bytes.Equal(encoded, reencoded.Bytes()) {
				t.Errorf("v%d: populated round-trip mismatch:\n  encoded:   %x\n  reencoded: %x", v, encoded, reencoded.Bytes())
			}

			_ = in.PrettyPrint()
		}

		{
			inNulls.ApiVersion = v

			var buf bytes.Buffer
			if err := inNulls.Write(&buf); err != nil {
				t.Fatalf("v%d: nulls write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &DescribeUserScramCredentialsResponse{}
			response := &protocol.Response{Body: bytes.NewBuffer(encoded)}
			response.ApiVersion = v
			if err := out.Read(response); err != nil {
				t.Fatalf("v%d: nulls read: %v", v, err)
			}

			var reencoded bytes.Buffer
			if err := out.Write(&reencoded); err != nil {
				t.Fatalf("v%d: nulls re-write: %v", v, err)
			}
			if !bytes.Equal(encoded, reencoded.Bytes()) {
				t.Errorf("v%d: nulls round-trip mismatch:\n  encoded:   %x\n  reencoded: %x", v, encoded, reencoded.Bytes())
			}

			_ = inNulls.PrettyPrint()
		}

		// PrettyPrint must not panic on the zero value either.
		_ = (&DescribeUserScramCredentialsResponse{}).PrettyPrint()
	}
}
