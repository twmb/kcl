package metadata

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestSortBrokers(t *testing.T) {
	brokers := make([]kmsg.MetadataResponseBroker, 0, 3)
	for _, id := range []int32{3, 1, 2} {
		b := kmsg.NewMetadataResponseBroker()
		b.NodeID = id
		brokers = append(brokers, b)
	}
	sortBrokers(brokers)
	for i, exp := range []int32{1, 2, 3} {
		if got := brokers[i].NodeID; got != exp {
			t.Errorf("broker %d: got %d != exp %d", i, got, exp)
		}
	}
}
