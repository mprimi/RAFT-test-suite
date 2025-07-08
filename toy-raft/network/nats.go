package network

import (
	"fmt"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/nats-io/nats.go"
)

const NatsSubjectPrefix = "RAFT"

type NatsNetwork struct {
	conn *nats.Conn

	groupId          string
	broadcastSubject string
	unicastPrefix    string

	networkDevices map[string]NetworkDevice
}

func NewNatsNetwork(groupId, natsUrl string) (Network, error) {

	nc, err := nats.Connect(
		natsUrl,
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	natsNetwork := &NatsNetwork{
		conn:             nc,
		groupId:          groupId,
		broadcastSubject: fmt.Sprintf("%s.%s.broadcast", NatsSubjectPrefix, groupId),
		unicastPrefix:    fmt.Sprintf("%s.%s", NatsSubjectPrefix, groupId),
		networkDevices:   make(map[string]NetworkDevice),
	}

	return natsNetwork, nil
}

func (net *NatsNetwork) RegisterNode(id string, networkDevice NetworkDevice) {
	// subscribe to unicast messages
	{
		recipientSubj := fmt.Sprintf("%s.%s", net.unicastPrefix, id)
		_, err := net.conn.Subscribe(recipientSubj, func(msg *nats.Msg) {
			networkDevice.Receive(msg.Data)
		})
		if err != nil {
			assert.Unreachable(
				"Failed to subscribe",
				map[string]any{
					"subject": recipientSubj,
					"error":   err.Error(),
				},
			)
			panic(fmt.Errorf("failed to subscribe: %w", err))
		}
	}

	// subscribe to broadcast messages
	{
		_, err := net.conn.Subscribe(net.broadcastSubject, func(msg *nats.Msg) {
			networkDevice.Receive(msg.Data)
		})
		if err != nil {
			assert.Unreachable(
				"Failed to subscribe to broadcast",
				map[string]any{
					"subject": net.broadcastSubject,
					"error":   err.Error(),
				},
			)
			panic(fmt.Errorf("failed to subscribe to broadcast: %w", err))
		}
	}
}

// TODO: add retries
func (net *NatsNetwork) Broadcast(msg []byte) {
	if err := net.conn.Publish(net.broadcastSubject, msg); err != nil {
		log.Printf("failed to broadcast message: %s", err)
	}
}

// TODO: add retries
func (net *NatsNetwork) Send(id string, msg []byte) {
	recipientSubj := fmt.Sprintf("%s.%s", net.unicastPrefix, id)
	if err := net.conn.Publish(recipientSubj, msg); err != nil {
		log.Printf("failed to send message: %s", err)
	}
}
