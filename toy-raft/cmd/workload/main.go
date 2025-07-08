package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
	"toy-raft/network"
	"toy-raft/raft"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"github.com/nats-io/nats.go"
)

const ProposalSubjSuffix = "PROPOSALS"

func main() {
	var (
		groupId string
		natsUrl string
	)
	flag.StringVar(&groupId, "group-id", "", "raft group id")
	flag.StringVar(&natsUrl, "nats-url", nats.DefaultURL, "nats url")
	flag.Parse()

	fatalErr := func(err error) {
		fmt.Println(err)
		os.Exit(1)
	}

	if groupId == "" {
		fatalErr(fmt.Errorf("missing required argument: group-id"))
	}

	nc, err := nats.Connect(
		natsUrl,
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
	)
	if err != nil {
		fatalErr(fmt.Errorf("failed to connect: %w", err))
	}
	defer nc.Close()

	// TODO wait for RAFT group to be established -- not sure how.
	// For now, do the dumb thing:
	time.Sleep(3 * time.Second)

	// If running in antithesis, signal setup is complete
	lifecycle.SetupComplete(nil)

	rng := rand.New(rand.NewSource(12345))
	buffer := make([]byte, 10)

	statsTicker := time.NewTicker(10 * time.Second)
	proposalCount := 0

	// Block forever
	for {
		select {
		case <-statsTicker.C:
			fmt.Printf("%d proposals\n", proposalCount)
		case <-time.After(1 * time.Second):
			fmt.Printf("Proposing...\n")
			rng.Read(buffer)
			proposal := &raft.Proposal{
				Data: buffer,
			}
			envelope := raft.Envelope{
				OperationType: raft.ProposalOp,
				Payload:       proposal.Bytes(),
			}
			payload, err := json.Marshal(envelope)
			if err != nil {
				panic(fmt.Errorf("failed to marshal proposal payload %w", err))
			}
			if err := nc.Publish(fmt.Sprintf("%s.%s.%s", network.NatsSubjectPrefix, groupId, ProposalSubjSuffix), payload); err != nil {
				fmt.Println("error while proposing: ", err)
			}
			proposalCount++

		}
	}
}
