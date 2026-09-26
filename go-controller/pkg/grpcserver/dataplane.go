package grpcserver

import (
	"github.com/gradientgeeks/aerostream/go-controller/pkg/dataplane"
	pb "github.com/gradientgeeks/aerostream/go-controller/proto/aeromq"
)

// addDataplaneConfig attaches the full quota + topic compression set to a heartbeat response.
func addDataplaneConfig(resp *pb.HeartbeatResponse) {
	store := dataplane.Default()
	resp.DataplaneConfigPresent = true
	resp.TopicCompression = store.TopicCompression()
	for _, q := range store.ListQuotas() {
		cq := &pb.ClientQuota{User: q.User, ClientId: q.ClientID}
		if q.ProducerByteRate != nil {
			cq.HasProducerByteRate, cq.ProducerByteRate = true, *q.ProducerByteRate
		}
		if q.ConsumerByteRate != nil {
			cq.HasConsumerByteRate, cq.ConsumerByteRate = true, *q.ConsumerByteRate
		}
		if q.RequestPercentage != nil {
			cq.HasRequestPercentage, cq.RequestPercentage = true, *q.RequestPercentage
		}
		resp.ClientQuotas = append(resp.ClientQuotas, cq)
	}
}
