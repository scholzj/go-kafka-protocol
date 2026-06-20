package describelogdirs

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated DescribeLogDirsResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestDescribeLogDirsResponseRoundTrip(t *testing.T) {
	in := &DescribeLogDirsResponse{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		Results: &[]DescribeLogDirsResponseResult{DescribeLogDirsResponseResult{
			ErrorCode: 1,
			LogDir:    resPtr("x"),
			Topics: &[]DescribeLogDirsResponseResultTopic{DescribeLogDirsResponseResultTopic{
				Name: resPtr("x"),
				Partitions: &[]DescribeLogDirsResponseResultTopicPartition{DescribeLogDirsResponseResultTopicPartition{
					PartitionIndex: 1,
					PartitionSize:  1,
					OffsetLag:      1,
					IsFutureKey:    true,
				}},
			}},
			TotalBytes:  1,
			UsableBytes: 1,
			IsCordoned:  true,
		}},
	}

	for v := int16(1); v <= 5; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &DescribeLogDirsResponse{}
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

		// PrettyPrint must not panic, for the populated and the zero value alike.
		_ = in.PrettyPrint()
		_ = (&DescribeLogDirsResponse{}).PrettyPrint()
	}
}
