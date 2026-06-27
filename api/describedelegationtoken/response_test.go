package describedelegationtoken

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated DescribeDelegationTokenResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestDescribeDelegationTokenResponseRoundTrip(t *testing.T) {
	in := &DescribeDelegationTokenResponse{
		ErrorCode: 1,
		Tokens: &[]DescribeDelegationTokenResponseToken{DescribeDelegationTokenResponseToken{
			PrincipalType:               resPtr("x"),
			PrincipalName:               resPtr("x"),
			TokenRequesterPrincipalType: resPtr("x"),
			TokenRequesterPrincipalName: resPtr("x"),
			IssueTimestamp:              1,
			ExpiryTimestamp:             1,
			MaxTimestamp:                1,
			TokenId:                     resPtr("x"),
			Hmac:                        &[]byte{1, 2, 3},
			Renewers: &[]DescribeDelegationTokenResponseTokenRenewer{DescribeDelegationTokenResponseTokenRenewer{
				PrincipalType: resPtr("x"),
				PrincipalName: resPtr("x"),
			}},
		}},
		ThrottleTimeMs: 1,
	}

	for v := int16(1); v <= 3; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &DescribeDelegationTokenResponse{}
			response := &protocol.Response{Body: bytes.NewBuffer(encoded)}
			response.ApiVersion = v
			if err := out.Read(response); err != nil {
				t.Fatalf("v%d: read: %v", v, err)
			}

			var reencoded bytes.Buffer
			if err := out.Write(&reencoded); err != nil {
				t.Fatalf("v%d: re-write: %v", v, err)
			}
			if !bytes.Equal(encoded, reencoded.Bytes()) {
				t.Errorf("v%d: round-trip mismatch:\n  encoded:   %x\n  reencoded: %x", v, encoded, reencoded.Bytes())
			}

			_ = in.PrettyPrint()
		}

		// PrettyPrint must not panic on the zero value either.
		_ = (&DescribeDelegationTokenResponse{}).PrettyPrint()
	}
}
