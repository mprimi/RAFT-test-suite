package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/nats-io/nats.go"

	"toy-raft/network"
	"toy-raft/raft"
	"toy-raft/server"
	"toy-raft/state"
)

func main() {
	var (
		replicaId  string
		groupId    string
		natsUrl    string
		peerString string
		timeLimit  time.Duration
	)
	flag.StringVar(&replicaId, "replica-id", "", "unique id of replica")
	flag.StringVar(&groupId, "group-id", "", "raft group id")
	flag.StringVar(&natsUrl, "nats-url", nats.DefaultURL, "nats url")
	flag.StringVar(&peerString, "peers", "", "comma separated list of peer ids (including self)")
	flag.DurationVar(&timeLimit, "time-limit", 0, "lifetime duration of replica")
	flag.Parse()

	fatalErr := func(err error) {
		fmt.Println(err)
		os.Exit(1)
	}
	if replicaId == "" {
		fatalErr(fmt.Errorf("missing required argument: replica-id"))
	}

	if groupId == "" {
		fatalErr(fmt.Errorf("missing required argument: group-id"))
	}

	if peerString == "" {
		fatalErr(fmt.Errorf("missing required argument: peers"))
	}
	peers := strings.Split(peerString, ",")

	if !slices.Contains(peers, replicaId) {
		fatalErr(fmt.Errorf("list of peers does not include this replica"))
	}

	natsNetwork, err := network.NewNatsNetwork(groupId, natsUrl)
	if err != nil {
		fatalErr(fmt.Errorf("failed to initialize network: %w", err))
	}

	sm := state.NewKeepLastBlocksStateMachine(replicaId, 10)
	raftNode := raft.NewRaftNodeImpl(replicaId, groupId, sm, raft.NewDiskStorage(replicaId, "raft-store"), natsNetwork, peers)
	natsNetwork.RegisterNode(replicaId, raftNode)
	srv := server.NewServer(replicaId, raftNode, sm)
	srv.Start()

	rng := rand.New(rand.NewSource(12345))
	buffer := make([]byte, 10)

	timer := time.NewTimer(timeLimit)
	if timeLimit == 0 {
		timer.Stop()
	}

	// Block forever
	for {
		select {
		case <-timer.C:
			fmt.Println("Time limit reached, exiting...")
			return
		case <-time.After(1 * time.Second):
			rng.Read(buffer)
			if err := srv.Propose(buffer); err != nil {
				if errors.Is(err, raft.ErrNotLeader) {
					fmt.Printf("Proposal rejected, node is not leader\n")
				} else {
					assert.Unreachable(
						"Propose error",
						map[string]any{
							"error": err.Error(),
						},
					)
					panic(err)
				}
			}
		}
	}
}
