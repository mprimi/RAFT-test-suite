package main

import (
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/nats-io/nats.go"

	"toy-raft/network"
	"toy-raft/raft"
	"toy-raft/server"
	"toy-raft/state"
)

const ProposalSubjectSuffixID = "PROPOSALS"

func main() {
	var (
		replicaId  string
		groupId    string
		natsUrl    string
		peerString string
	)
	flag.StringVar(&replicaId, "replica-id", "", "unique id of replica")
	flag.StringVar(&groupId, "group-id", "", "raft group id")
	flag.StringVar(&natsUrl, "nats-url", nats.DefaultURL, "nats url")
	flag.StringVar(&peerString, "peers", "", "comma separated list of peer ids (including self)")
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
	srv := server.NewServer(replicaId, raftNode, sm, natsNetwork)
	natsNetwork.RegisterNode(ProposalSubjectSuffixID, srv)
	srv.Start()

	select {}
}
