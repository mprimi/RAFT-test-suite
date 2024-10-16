package raft

import (
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"toy-raft/network"
)

const (
	A_LONG_TIME = 100 * time.Hour
)

type MockStateMachine struct {
	blocks [][]byte
}

func (sm *MockStateMachine) Applied() uint64 {
	return uint64(len(sm.blocks))
}

func (sm *MockStateMachine) GetTailBlocks(n int) (blocks [][]byte, offset int) {
	if n > len(sm.blocks) {
		n = len(sm.blocks)
	}
	return sm.blocks[:n], 0
}

func (sm *MockStateMachine) GetId() string {
	return "mock"
}

func (sm *MockStateMachine) Apply(block []byte) {
	sm.blocks = append(sm.blocks, block)
}

func (sm *MockStateMachine) InstallSnapshot(snapshot []byte) error {
	panic("todo")
}

func (sm *MockStateMachine) CreateSnapshot() ([]byte, error) {
	panic("todo")
}

type TestNetwork struct {
	lastMessageSent        []byte
	lastRecipient          string
	lastMessageBroadcasted []byte
}

func (net *TestNetwork) Broadcast(msg []byte) {
	log.Printf("🛜 NETWORK: broadcasting msg with length %d\n", len(msg))
	net.lastMessageBroadcasted = msg
}

func (net *TestNetwork) Send(id string, msg []byte) {
	log.Printf("🛜 NETWORK: sending msg with length %d to %s\n", len(msg), id)
	net.lastMessageSent = msg
	net.lastRecipient = id
}

func (net *TestNetwork) RegisterNode(id string, networkDevice network.NetworkDevice) {
	panic("register node panic")
}

func assertNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func assertEqual[T comparable](t *testing.T, actual T, expected T) {
	t.Helper()
	if actual != expected {
		t.Fatalf("expected: %+v, actual: %+v", expected, actual)
	}
}

func assertNotEqual[T comparable](t *testing.T, actual T, expected T) {
	t.Helper()
	if actual == expected {
		t.Fatalf("expected %+v to not equal", actual)
	}
}

func assertDeepEqual(t *testing.T, actual any, expected any) {
	t.Helper()
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected %+v, actual %+v", expected, actual)
	}
}

func assertNil(t *testing.T, actual any) {
	t.Helper()
	if actual != nil {
		t.Fatalf("expected nil, actual %+v", actual)
	}
}

func assertNotNil(t *testing.T, actual any) {
	t.Helper()
	if actual == nil {
		t.Fatalf("expected not nil")
	}
}

func TestRaftStepDownDueToHigherTerm(t *testing.T) {

	cases := []struct {
		initialTerm  uint64
		voteTerm     uint64
		initialState RaftState
		description  string
	}{
		{
			0,
			1,
			Follower,
			"initial term 0 (startup as follower), vote term is one higher",
		},
		{
			10,
			100,
			Follower,
			"follower with term 10 receives vote request with term 100, should stepdown",
		},
		{
			10,
			100,
			Candidate,
			"candidate with term 10 receives vote request with term 100, should stepdown",
		},
		{
			10,
			100,
			Leader,
			"leader with term 10 receives vote request with term 100, should stepdown",
		},
	}

	for caseNum, c := range cases {
		t.Run(fmt.Sprintf("test-case %d of %d", caseNum+1, len(cases)), func(t *testing.T) {

			dummyNetwork := &TestNetwork{}

			id := "a"
			otherId := "b"
			raftNode := &RaftNodeImpl{
				id:              id,
				inboundMessages: make(chan []byte, 1000),
				state:           c.initialState,
				storage:         NewInMemoryStorage(),
				voteMap:         nil,
				network:         dummyNetwork,
				peers: map[string]bool{
					id:      true,
					otherId: true,
				},
				electionTimeoutTimer:     time.NewTimer(A_LONG_TIME),
				voteResponseTimeoutTimer: time.NewTimer(A_LONG_TIME),
				sendAppendEntriesTicker:  time.NewTicker(A_LONG_TIME),
			}

			switch c.initialState {
			case Candidate:
				raftNode.voteMap = make(map[string]bool)
			case Leader:
				raftNode.followersStateMap = make(map[string]*FollowerState)
				raftNode.acceptingProposals.Store(true)
			}

			if c.initialTerm > 0 {
				raftNode.storage.SetTerm(c.initialTerm)
			}

			voteRequest := VoteRequest{
				Term:         c.voteTerm,
				CandidateId:  otherId,
				LastLogIndex: 1,
				LastLogTerm:  0,
			}
			envelope := Envelope{
				OperationType: VoteRequestOp,
				Payload:       voteRequest.Bytes(),
			}

			raftNode.inboundMessages <- envelope.Bytes()
			raftNode.processOneTransistion()

			if raftNode.state != Follower {
				t.Fatalf("state was %s, but expected Follower", raftNode.state)
			}
			if raftNode.storage.GetCurrentTerm() != c.voteTerm {
				t.Fatalf("current term is %d, but expected %d", raftNode.storage.GetCurrentTerm(), c.voteTerm)
			}

			if dummyNetwork.lastRecipient != otherId {
				t.Fatalf("expected %s, actual %s", otherId, dummyNetwork.lastRecipient)
			}

			opType, message, err := parseMessage(dummyNetwork.lastMessageSent)
			if err != nil {
				t.Fatal(err)
			}
			if opType != VoteResponseOp {
				t.Fatalf("expected last message sent was %d (VoteResponse), was %d instead", VoteRequestOp, opType)
			}

			voteResponse := message.(*VoteResponse)

			if voteResponse.Term != c.voteTerm {
				t.Fatalf("expected %d, actual %d", c.voteTerm, voteResponse.Term)
			}
			if voteResponse.VoterId != raftNode.id {
				t.Fatalf("expected %s, actual %s", raftNode.id, voteResponse.VoterId)
			}
			if voteResponse.VoteGranted != true {
				t.Fatalf("expected %t, actual %t", true, voteResponse.VoteGranted)
			}

		})

	}
}

func TestConvertToCandidate(t *testing.T) {

	// follower who's timer expires
	// test that it converted to candidate, voted for itself and send RequestVoteRPCs
	dummyNetwork := &TestNetwork{}

	// create a node
	// give it a state = leader | candidate
	// send an RPC call (RequestVote(VoteRequest|VoteResponse)) w/ a higher term

	id := "a"
	raftNode := &RaftNodeImpl{
		id:                       id,
		inboundMessages:          make(chan []byte, 1000),
		state:                    Follower, // could be candidate
		storage:                  NewInMemoryStorage(),
		voteMap:                  nil,
		network:                  dummyNetwork,
		electionTimeoutTimer:     time.NewTimer(A_LONG_TIME),
		voteResponseTimeoutTimer: time.NewTimer(A_LONG_TIME),
		sendAppendEntriesTicker:  time.NewTicker(A_LONG_TIME),
	}
	previousTerm := raftNode.storage.GetCurrentTerm()

	raftNode.convertToCandidate()
	currentTerm := raftNode.storage.GetCurrentTerm()

	// TODO: check timer was reset

	// check that we voted for ourselves
	assertEqual(t, len(raftNode.voteMap), 1)
	assertEqual(t, raftNode.voteMap[id], true)

	// check term and votedFor
	assertEqual(t, currentTerm, previousTerm+1)
	assertEqual(t, raftNode.storage.GetVotedFor(), id)

	// check for that it broadcasted RequestVoteRPC to the network
	opType, message, err := parseMessage(dummyNetwork.lastMessageBroadcasted)
	assertNoErr(t, err)
	assertEqual(t, opType, VoteRequestOp)

	voteRequest := message.(*VoteRequest)
	assertEqual(t, voteRequest.Term, currentTerm)
	assertEqual(t, voteRequest.CandidateId, id)

}

func TestAscendToLeadership(t *testing.T) {

	dummyNetwork := &TestNetwork{}

	id := "a"
	raftNode := &RaftNodeImpl{
		id:                id,
		inboundMessages:   make(chan []byte, 1000),
		state:             Follower, // could be candidate
		storage:           NewInMemoryStorage(),
		voteMap:           nil,
		followersStateMap: nil,
		peers: map[string]bool{
			// ourselves
			id:  true,
			"b": true,
			"c": true,
			"d": true,
			"e": true,
		},
		network:                  dummyNetwork,
		electionTimeoutTimer:     time.NewTimer(A_LONG_TIME),
		voteResponseTimeoutTimer: time.NewTimer(A_LONG_TIME),
		sendAppendEntriesTicker:  time.NewTicker(A_LONG_TIME),
	}
	currentTerm := raftNode.storage.GetCurrentTerm()

	// helper func for sending vote reponse
	sendVoteResponse := func(term uint64, voterId string, voteGranted bool) {
		voteResponse := VoteResponse{
			Term:        term,
			VoterId:     voterId,
			VoteGranted: voteGranted,
		}
		envelope := Envelope{
			OperationType: VoteResponseOp,
			Payload:       voteResponse.Bytes(),
		}
		raftNode.inboundMessages <- envelope.Bytes()
	}

	// receive a vote response from a valid peer as a follower
	sendVoteResponse(currentTerm, "b", true)
	raftNode.processOneTransistion()
	assertEqual(t, raftNode.state, Follower)
	assertEqual(t, len(raftNode.voteMap), 0)

	previousTerm := raftNode.storage.GetCurrentTerm()
	// trigger election
	raftNode.convertToCandidate()
	currentTerm = raftNode.storage.GetCurrentTerm()

	// TODO: check timer was reset

	// check that we became a candidate
	assertEqual(t, raftNode.state, Candidate)

	// check that we voted for ourselves
	assertEqual(t, len(raftNode.voteMap), 1)
	assertEqual(t, raftNode.voteMap[id], true)

	// check term and votedFor
	assertEqual(t, currentTerm, previousTerm+1)
	assertEqual(t, raftNode.storage.GetVotedFor(), id)

	// check for that it broadcasted RequestVoteRPC to the network
	opType, message, err := parseMessage(dummyNetwork.lastMessageBroadcasted)
	assertNoErr(t, err)
	assertEqual(t, opType, VoteRequestOp)

	voteRequest := message.(*VoteRequest)
	assertEqual(t, voteRequest.Term, currentTerm)
	assertEqual(t, voteRequest.CandidateId, id)

	// send a valid vote response who is not a peer
	sendVoteResponse(currentTerm, "notAPeer", true)
	raftNode.processOneTransistion()
	// check that it is still a candidate
	assertEqual(t, raftNode.state, Candidate)
	// check that our vote count is still 1
	assertEqual(t, len(raftNode.voteMap), 1)

	// send a voteGranted:false vote response from a peer
	sendVoteResponse(currentTerm, "b", false)
	raftNode.processOneTransistion()
	// check that it is still a candidate
	assertEqual(t, raftNode.state, Candidate)
	// check that our vote count is still 1
	assertEqual(t, len(raftNode.voteMap), 1)

	// send the same valid vote response who is a peer (a different peer than above) x3
	for i := 0; i < 3; i++ {
		sendVoteResponse(currentTerm, "c", true)
		raftNode.processOneTransistion()
	}
	// check that its still a candidate
	assertEqual(t, raftNode.state, Candidate)
	// check that our vote count is 2
	assertEqual(t, len(raftNode.voteMap), 2)

	previousTerm = currentTerm
	// simulate an election timeout -> increase term, stay candidate
	raftNode.convertToCandidate()
	// refresh value
	currentTerm = raftNode.storage.GetCurrentTerm()
	assertEqual(t, currentTerm, previousTerm+1)

	// send a vote response that is out-of-date (lower term)
	sendVoteResponse(previousTerm, "d", true)
	raftNode.processOneTransistion()
	// check that it is still a candidate
	assertEqual(t, raftNode.state, Candidate)
	// check that our vote count is still 1, not 2 since we wipe votemap on new election
	assertEqual(t, len(raftNode.voteMap), 1)

	// send a vote response from someone who has a higher term
	higherTerm := currentTerm + 1
	sendVoteResponse(higherTerm, "e", true)
	raftNode.processOneTransistion()
	currentTerm = raftNode.storage.GetCurrentTerm()
	// check that we stepped down
	assertEqual(t, raftNode.state, Follower)
	// check that our term is now the higher term
	assertEqual(t, currentTerm, higherTerm)

	previousTerm = currentTerm
	// simulate an election timeout -> increase term, stay candidate
	raftNode.convertToCandidate()
	// refresh value
	currentTerm = raftNode.storage.GetCurrentTerm()
	assertEqual(t, currentTerm, previousTerm+1)

	// send two (different) valid votes and check that it becomes leader
	sendVoteResponse(currentTerm, "e", true)
	raftNode.processOneTransistion()
	sendVoteResponse(currentTerm, "b", true)
	raftNode.processOneTransistion()

	// check that it became leader
	assertEqual(t, raftNode.state, Leader)
	// check term
	assertEqual(t, raftNode.storage.GetCurrentTerm(), currentTerm)

	// check follower map contains only peers
	expectedFollowersStateMap := map[string]*FollowerState{
		id: {
			nextIndex:  1,
			matchIndex: 0,
		},
		"b": {
			nextIndex:  1,
			matchIndex: 0,
		},
		"c": {
			nextIndex:  1,
			matchIndex: 0,
		},
		"d": {
			nextIndex:  1,
			matchIndex: 0,
		},
		"e": {
			nextIndex:  1,
			matchIndex: 0,
		},
	}
	// assert equal for only nextIndex and matchIndex
	for k, v := range raftNode.followersStateMap {
		expectedFollowerState := expectedFollowersStateMap[k]
		assertEqual(t, v.nextIndex, expectedFollowerState.nextIndex)
		assertEqual(t, v.matchIndex, expectedFollowerState.matchIndex)
	}

	// check that it broadcasted AppendEntriesRequest to the network
	expectedAppendEntriesRequest := &AppendEntriesRequest{
		Term:            currentTerm,
		LeaderId:        id,
		Entries:         []Entry{},
		PrevLogIdx:      0,
		PrevLogTerm:     NoPreviousTerm,
		LeaderCommitIdx: 0,
	}
	opType, message, err = parseMessage(dummyNetwork.lastMessageBroadcasted)
	assertNoErr(t, err)
	assertEqual(t, opType, AppendEntriesRequestOp)
	actualAppendEntriesRequest := message.(*AppendEntriesRequest)
	// check that we sent a request
	assertNotEqual(t, actualAppendEntriesRequest.RequestId, "")
	// remove request id to validate the rest
	actualAppendEntriesRequest.RequestId = ""
	assertDeepEqual(t, actualAppendEntriesRequest, expectedAppendEntriesRequest)
}

func TestFollowerHandleAppendEntries(t *testing.T) {

	dummyNetwork := &TestNetwork{}

	const (
		id       = "FOLLOWER"
		leaderId = "LEADER"
	)

	var (
		raftNode              *RaftNodeImpl
		initialTerm           uint64
		appendEntriesResponse *AppendEntriesResponse
	)

	createRaftNode := func(id string, term uint64, commitIdx uint64, state RaftState) *RaftNodeImpl {
		rn := &RaftNodeImpl{
			id:                       id,
			stateMachine:             &MockStateMachine{},
			quitCh:                   make(chan bool),
			inboundMessages:          make(chan []byte, 1000),
			network:                  dummyNetwork,
			state:                    state,
			storage:                  NewInMemoryStorage(),
			peers:                    map[string]bool{id: true, leaderId: true, "c": true, "d": true, "e": true},
			voteMap:                  nil,
			commitIndex:              commitIdx,
			lastApplied:              0,
			electionTimeoutTimer:     time.NewTimer(A_LONG_TIME),
			voteResponseTimeoutTimer: time.NewTimer(A_LONG_TIME),
			sendAppendEntriesTicker:  time.NewTicker(A_LONG_TIME),
		}
		if term > 0 {
			rn.storage.SetTerm(term)
		}
		return rn
	}

	sendAndProcessAppendEntriesRequest := func(appendEntriesRequest *AppendEntriesRequest) {
		appendEntriesRequestBytes := appendEntriesRequest.Bytes()
		envelope := Envelope{
			OperationType: AppendEntriesRequestOp,
			Payload:       appendEntriesRequestBytes,
		}
		t.Logf("sending AE request with term %d", appendEntriesRequest.Term)
		raftNode.inboundMessages <- envelope.Bytes()
		raftNode.processOneTransistion()
	}

	getAppendEntriesResponse := func() *AppendEntriesResponse {
		lastMessageSent := dummyNetwork.lastMessageSent
		opType, message, err := parseMessage(lastMessageSent)
		assertNoErr(t, err)
		assertEqual(t, opType, AppendEntriesResponseOp)
		return message.(*AppendEntriesResponse)
	}

	// send AE with lower term, node should ignore request and not change state, response should be false
	t.Run("send AE with lower term, node should ignore request and not change state, response should be false", func(t *testing.T) {
		initialTerm = 10
		raftNode = createRaftNode(id, initialTerm, 0, Follower)

		sendAndProcessAppendEntriesRequest(&AppendEntriesRequest{
			Term:            initialTerm - 1,
			LeaderId:        leaderId,
			Entries:         []Entry{},
			PrevLogIdx:      0,
			PrevLogTerm:     NoPreviousTerm,
			LeaderCommitIdx: 0,
		})
		appendEntriesResponse = getAppendEntriesResponse()
		assertEqual(t, appendEntriesResponse.ResponderId, id)
		assertEqual(t, appendEntriesResponse.Term, initialTerm)
		assertEqual(t, appendEntriesResponse.Success, false)
		assertEqual(t, raftNode.state, Follower)
	})

	/*
		initState = {term: 1, state: Follower}
		log = [{term: 1, cmd: foo, idx: 1}, {term: 1, cmd: bar, idx: 2}]
		AE = {term: 3, prevLogIdx: 3, prevLogTerm: 1} -> false, cuz no prevLogIdx=3, update term to AE.term
	*/
	t.Run("AE with higher term, but no prevLogIdx=3, update term to AE.term", func(t *testing.T) {
		initialTerm = 1
		raftNode = createRaftNode(id, initialTerm, 0, Follower)
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("foo")})
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("bar")})
		var (
			term        uint64 = 3
			prevLogIdx  uint64 = 3
			prevLogTerm uint64 = 1
		)
		sendAndProcessAppendEntriesRequest(&AppendEntriesRequest{
			Term:            term,
			LeaderId:        leaderId,
			Entries:         []Entry{},
			PrevLogIdx:      prevLogIdx,
			PrevLogTerm:     prevLogTerm,
			LeaderCommitIdx: 0,
		})
		assertEqual(t, raftNode.storage.GetCurrentTerm(), term)
		// check response
		appendEntriesResponse = getAppendEntriesResponse()
		assertEqual(t, appendEntriesResponse.ResponderId, id)
		assertEqual(t, appendEntriesResponse.Term, term)
		assertEqual(t, appendEntriesResponse.Success, false)
		assertEqual(t, raftNode.state, Follower)
	})

	/*
		initState = {term: 1, state: Follower}
		log = [{term: 1, cmd: foo, idx: 1}, {term: 1, cmd: bar, idx: 2}]
		AE = {term:3, prevLogIdx: 2, prevLogTerm: 3} -> false, cuz we have prevLogIdx=2 but its term is not 3, update term to AE.term
	*/
	t.Run("AE with higher term, but we have prevLogIdx=2 but its term is not 3, update term to AE.term", func(t *testing.T) {
		initialTerm = 1
		raftNode = createRaftNode(id, initialTerm, 0, Follower)
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("foo")})
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("bar")})
		var (
			term        uint64 = 3
			prevLogIdx  uint64 = 2
			prevLogTerm uint64 = 3
		)
		sendAndProcessAppendEntriesRequest(&AppendEntriesRequest{
			Term:            term,
			LeaderId:        leaderId,
			Entries:         []Entry{},
			PrevLogIdx:      prevLogIdx,
			PrevLogTerm:     prevLogTerm,
			LeaderCommitIdx: 0,
		})
		assertEqual(t, raftNode.storage.GetCurrentTerm(), term)
		// check response
		appendEntriesResponse = getAppendEntriesResponse()
		assertEqual(t, appendEntriesResponse.ResponderId, id)
		assertEqual(t, appendEntriesResponse.Term, term)
		assertEqual(t, appendEntriesResponse.Success, false)
		assertEqual(t, raftNode.state, Follower)
	})

	// this test has a valid append entries request but it should overwrite a portion of the log
	t.Run("valid AE request but it should overwrite a portion of the log", func(t *testing.T) {
		raftNode = createRaftNode(id, 1, 0, Follower)
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("foo")})
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("bar")})
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("faz")})
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("foz")})
		aeReq := &AppendEntriesRequest{
			Term:            2,
			LeaderId:        leaderId,
			Entries:         []Entry{{Term: 2, Cmd: []byte("baz")}},
			PrevLogIdx:      2,
			PrevLogTerm:     1,
			LeaderCommitIdx: 0,
		}
		sendAndProcessAppendEntriesRequest(aeReq)
		assertEqual(t, raftNode.storage.GetCurrentTerm(), aeReq.Term)
		// check response
		appendEntriesResponse = getAppendEntriesResponse()
		assertEqual(t, appendEntriesResponse.ResponderId, id)
		assertEqual(t, appendEntriesResponse.Term, aeReq.Term)
		assertEqual(t, appendEntriesResponse.Success, true)
		assertEqual(t, appendEntriesResponse.MatchIndex, 3)
		assertEqual(t, raftNode.state, Follower)
		// check log
		log := raftNode.storage.TestGetLogEntries()
		for i := 0; i < len(log); i++ {
			entry := log[i]
			t.Logf("entry: %+v", entry)
		}
		expectedLog := []Entry{
			{Term: 1, Cmd: []byte("foo")},
			{Term: 1, Cmd: []byte("bar")},
			{Term: 2, Cmd: []byte("baz")},
		}
		for i := 0; i < len(log); i++ {
			entry := log[i]
			expectedEntry := expectedLog[i]
			assertEqual(t, entry.Term, expectedEntry.Term)
			assertEqual(t, string(entry.Cmd), string(expectedEntry.Cmd))
		}
	})

	/*
		log = [{term: 1, cmd: foo, idx: 1}, {term: 1, cmd: bar, idx: 2}, {term: 1, cmd: faz, idx: 3}, {term: 1, cmd: foz, idx: 4}]
		initState = {term: 1, state: Follower, commitIdx: 0}
		AE = {term: 1, prevLogIdx: 2, prevLogTerm: 1, leaderCommit: 1, entries: [{term: 2, cmd: baz}]} -> accept, but we need to overwrite
		resultant_log = [{term: 1, cmd: foo, idx: 1}, {term: 1, cmd: bar, idx: 2}, {term: 2, cmd: baz, idx: 3}]
		resultant_state = {term: 1, state: Follower, commitIdx: 1} min(lastEntry:3, leaderCommit:1)->1
	*/
	t.Run("accept but we need to overwrite", func(t *testing.T) {
		var (
			initialCommitIdx uint64 = 2
			initialTerm      uint64 = 1
		)
		raftNode = createRaftNode(id, initialTerm, initialCommitIdx, Follower)
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("foo")})
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("bar")})
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("faz")})
		raftNode.storage.AppendEntry(Entry{Term: 1, Cmd: []byte("foz")})
		var (
			prevLogIdx   uint64 = 2
			prevLogTerm  uint64 = 1
			reqTerm      uint64 = 2
			leaderCommit uint64 = 3
		)
		overriteAEReq := &AppendEntriesRequest{
			Term:            reqTerm,
			LeaderId:        leaderId,
			Entries:         []Entry{{Term: 2, Cmd: []byte("baz")}},
			PrevLogIdx:      prevLogIdx,
			PrevLogTerm:     prevLogTerm,
			LeaderCommitIdx: leaderCommit,
		}

		sendAndProcessAppendEntriesRequest(overriteAEReq)
		assertEqual(t, raftNode.state, Follower)
		assertEqual(t, raftNode.storage.GetCurrentTerm(), reqTerm)
		appendEntriesResponse = getAppendEntriesResponse()
		assertEqual(t, appendEntriesResponse.ResponderId, id)
		assertEqual(t, appendEntriesResponse.Success, true)
		assertEqual(t, appendEntriesResponse.MatchIndex, 3)
		// check log
		log := raftNode.storage.TestGetLogEntries()
		expectedLog := []Entry{
			{Term: 1, Cmd: []byte("foo")},
			{Term: 1, Cmd: []byte("bar")},
			{Term: 2, Cmd: []byte("baz")},
		}
		for i := 0; i < len(log); i++ {
			entry := log[i]
			expectedEntry := expectedLog[i]
			t.Logf("entry %d: %+v", i, entry)
			assertEqual(t, entry.Term, expectedEntry.Term)
			assertEqual(t, string(entry.Cmd), string(expectedEntry.Cmd))
		}
		assertEqual(t, raftNode.commitIndex, leaderCommit)

		// TODO: remove these vars and all like it, use req struct instead
		var (
			prevLogEntryIdx  uint64 = 2
			prevLogEntryTerm uint64 = 1
		)
		reqTerm = 2
		duplicateAndNewAEReq := &AppendEntriesRequest{
			Term:            reqTerm,
			LeaderId:        leaderId,
			Entries:         []Entry{{Term: 2, Cmd: []byte("baz")}, {Term: 2, Cmd: []byte("faz")}},
			PrevLogIdx:      prevLogEntryIdx,
			PrevLogTerm:     prevLogEntryTerm,
			LeaderCommitIdx: leaderCommit,
		}
		sendAndProcessAppendEntriesRequest(duplicateAndNewAEReq)
		assertEqual(t, raftNode.state, Follower)
		assertEqual(t, raftNode.storage.GetCurrentTerm(), reqTerm)
		appendEntriesResponse = getAppendEntriesResponse()
		assertEqual(t, appendEntriesResponse.ResponderId, id)
		assertEqual(t, appendEntriesResponse.Success, true)
		assertEqual(t, appendEntriesResponse.MatchIndex, 4)
		// check log
		log = raftNode.storage.TestGetLogEntries()
		expectedLog = []Entry{
			{Term: 1, Cmd: []byte("foo")},
			{Term: 1, Cmd: []byte("bar")},
			{Term: 2, Cmd: []byte("baz")},
			{Term: 2, Cmd: []byte("faz")},
		}
		for i := 0; i < len(log); i++ {
			entry := log[i]
			expectedEntry := expectedLog[i]
			t.Logf("entry %d: %+v", i, entry)
			assertEqual(t, entry.Term, expectedEntry.Term)
			assertEqual(t, string(entry.Cmd), string(expectedEntry.Cmd))
		}
		assertEqual(t, raftNode.commitIndex, leaderCommit)
	})

	t.Run("leader and follower have empty logs", func(t *testing.T) {
		initialCommitIdx := uint64(0)
		raftNode = createRaftNode(id, initialTerm, initialCommitIdx, Follower)

		// send an AE request from a leader w/ an empty log
		sendAndProcessAppendEntriesRequest(&AppendEntriesRequest{
			Term:            1,
			LeaderId:        leaderId,
			Entries:         []Entry{},
			PrevLogIdx:      0,
			PrevLogTerm:     NoPreviousTerm,
			LeaderCommitIdx: 0,
		})
		// check that it sent a successful response
		appendEntriesResponse = getAppendEntriesResponse()
		assertEqual(t, appendEntriesResponse.ResponderId, id)
		assertEqual(t, appendEntriesResponse.Success, true)
		assertEqual(t, appendEntriesResponse.MatchIndex, 0)
		assertEqual(t, raftNode.state, Follower)
	})

	t.Run("unknown peer sends AE request", func(t *testing.T) {
		dummyNetwork.lastMessageSent = nil
		initialCommitIdx := uint64(0)
		raftNode = createRaftNode(id, initialTerm, initialCommitIdx, Follower)

		// send an AE request from an unknown peer
		sendAndProcessAppendEntriesRequest(&AppendEntriesRequest{
			Term:            1,
			LeaderId:        "unknown",
			Entries:         []Entry{},
			PrevLogIdx:      0,
			PrevLogTerm:     NoPreviousTerm,
			LeaderCommitIdx: 0,
		})
		// check that it ignored it by not sending a response
		assertEqual(t, string(dummyNetwork.lastMessageSent), string([]byte{}))
	})
}

func TestHandleVoteRequest(t *testing.T) {
	dummyNetwork := &TestNetwork{}

	const (
		id          = "FOLLOWER"
		candidateId = "CANDIDATE"
	)

	var (
		followerNode *RaftNodeImpl
	)

	createRaftNode := func(id string, term uint64, state RaftState, initLog []*Entry) *RaftNodeImpl {
		rn := &RaftNodeImpl{
			id:                       id,
			stateMachine:             nil,
			quitCh:                   make(chan bool),
			inboundMessages:          make(chan []byte, 1000),
			network:                  dummyNetwork,
			state:                    state,
			storage:                  NewInMemoryStorage(),
			peers:                    map[string]bool{id: true, candidateId: true, "c": true, "d": true, "e": true},
			voteMap:                  nil,
			followersStateMap:        nil,
			commitIndex:              0,
			lastApplied:              0,
			electionTimeoutTimer:     time.NewTimer(A_LONG_TIME),
			voteResponseTimeoutTimer: time.NewTimer(A_LONG_TIME),
			sendAppendEntriesTicker:  time.NewTicker(A_LONG_TIME),
		}
		if term > 0 {
			rn.storage.SetTerm(term)
		}
		for _, entry := range initLog {
			rn.storage.AppendEntry(*entry)
		}
		return rn
	}

	sendAndProcessVoteRequest := func(voteRequest *VoteRequest) {
		envelope := Envelope{
			OperationType: VoteRequestOp,
			Payload:       voteRequest.Bytes(),
		}
		dummyNetwork.lastMessageSent = nil
		followerNode.inboundMessages <- envelope.Bytes()
		followerNode.processOneTransistion()
	}

	// test that a follower shouldn't vote for a valid peer
	t.Run("invalid_peer", func(t *testing.T) {
		initialTerm := uint64(1)
		followerNode = createRaftNode(id, initialTerm, Follower, []*Entry{})

		sendAndProcessVoteRequest(&VoteRequest{
			Term:         initialTerm,
			CandidateId:  "INVALID_PEER",
			LastLogIndex: 1,
			LastLogTerm:  0,
		})

		// check that it ignored the vote request and did not send a response
		assertEqual(t, string(dummyNetwork.lastMessageSent), string([]byte{}))
	})

	t.Run("lower_term", func(t *testing.T) {
		initialTerm := uint64(5)
		followerNode = createRaftNode(id, initialTerm, Follower, []*Entry{})

		sendAndProcessVoteRequest(&VoteRequest{
			Term:         initialTerm - 1,
			CandidateId:  candidateId,
			LastLogIndex: 1,
			LastLogTerm:  0,
		})

		// check that it sent a false vote response
		opType, message, err := parseMessage(dummyNetwork.lastMessageSent)
		assertNoErr(t, err)
		assertEqual(t, opType, VoteResponseOp)
		voteResponse := message.(*VoteResponse)
		assertEqual(t, voteResponse.Term, initialTerm)
		assertEqual(t, voteResponse.VoterId, id)
		assertEqual(t, voteResponse.VoteGranted, false)
	})

	t.Run("higher_term", func(t *testing.T) {
		initialTerm := uint64(5)
		followerNode = createRaftNode(id, initialTerm, Follower, []*Entry{})

		requestTerm := initialTerm + 1
		sendAndProcessVoteRequest(&VoteRequest{
			Term:         requestTerm,
			CandidateId:  candidateId,
			LastLogIndex: 1,
			LastLogTerm:  0,
		})

		// check that our term was updated
		assertEqual(t, followerNode.storage.GetCurrentTerm(), requestTerm)
		// check we voted for the candidate
		assertEqual(t, followerNode.storage.GetVotedFor(), candidateId)

		// check that it sent a true vote response
		opType, message, err := parseMessage(dummyNetwork.lastMessageSent)
		assertNoErr(t, err)
		assertEqual(t, opType, VoteResponseOp)
		voteResponse := message.(*VoteResponse)
		assertEqual(t, voteResponse.Term, initialTerm+1)
		assertEqual(t, voteResponse.VoterId, id)
		assertEqual(t, voteResponse.VoteGranted, true)
	})

	t.Run("same_term_no_vote", func(t *testing.T) {

		initialTerm := uint64(5)
		initialLog := []*Entry{
			{Term: 1, Cmd: []byte("foo")},
			{Term: 1, Cmd: []byte("bar")},
			{Term: 2, Cmd: []byte("baz")},
		}
		expectedLastLogIndex := uint64(3)
		expectedLastLogTerm := uint64(2)

		testCases := []struct {
			lastLogIndex uint64
			lastLogTerm  uint64
			voteGranted  bool
		}{
			{1, 1, false}, // lower term
			{2, 1, false}, // lower term
			{3, 1, false}, // lower term
			{4, 1, false}, // lower term
			{1, 2, false}, // same term, but lower index
			{3, 2, true},  // same term, same index
			{1, 4, true},  // higher term
			{3, 4, true},  // higher term
			{4, 4, true},  // higher term
		}

		for caseNum, c := range testCases {
			t.Run(fmt.Sprintf("test-case_%d_of_%d", caseNum+1, len(testCases)), func(t *testing.T) {

				followerNode = createRaftNode(id, initialTerm, Follower, initialLog)

				actualLastLogIndex, actualLastLogTerm := followerNode.storage.GetLastLogIndexAndTerm()
				assertEqual(t, actualLastLogIndex, expectedLastLogIndex)
				assertEqual(t, actualLastLogTerm, expectedLastLogTerm)

				sendAndProcessVoteRequest(&VoteRequest{
					Term:         initialTerm,
					CandidateId:  candidateId,
					LastLogIndex: c.lastLogIndex,
					LastLogTerm:  c.lastLogTerm,
				})

				// check follower state
				actualLastLogIndex, actualLastLogTerm = followerNode.storage.GetLastLogIndexAndTerm()
				assertEqual(t, actualLastLogIndex, expectedLastLogIndex)
				assertEqual(t, actualLastLogTerm, expectedLastLogTerm)
				if c.voteGranted {
					assertEqual(t, followerNode.storage.GetVotedFor(), candidateId)
				} else {
					assertEqual(t, followerNode.storage.GetVotedFor(), "")
				}
				assertEqual(t, followerNode.storage.GetCurrentTerm(), initialTerm)

				// check that it sent a vote response
				opType, message, err := parseMessage(dummyNetwork.lastMessageSent)
				assertNoErr(t, err)
				assertEqual(t, opType, VoteResponseOp)
				voteResponse := message.(*VoteResponse)
				assertEqual(t, voteResponse.Term, initialTerm)
				assertEqual(t, voteResponse.VoterId, id)
				assertEqual(t, voteResponse.VoteGranted, c.voteGranted)
			})
		}
	})

	t.Run("follower_already_voted", func(t *testing.T) {
		initialTerm := uint64(5)
		followerNode = createRaftNode(id, initialTerm, Follower, []*Entry{})

		// vote for someone
		otherCandidate := "some other candidate"
		followerNode.storage.VoteFor(otherCandidate, initialTerm)

		sendAndProcessVoteRequest(&VoteRequest{
			Term:         initialTerm,
			CandidateId:  candidateId,
			LastLogIndex: 1,
			LastLogTerm:  0,
		})

		currentTerm := followerNode.storage.GetCurrentTerm()

		// follower state
		assertEqual(t, currentTerm, initialTerm)
		assertEqual(t, followerNode.state, Follower)
		assertEqual(t, followerNode.storage.GetVotedFor(), otherCandidate)

		// check that it sent a false vote response
		opType, message, err := parseMessage(dummyNetwork.lastMessageSent)
		assertNoErr(t, err)
		assertEqual(t, opType, VoteResponseOp)
		voteResponse := message.(*VoteResponse)
		assertEqual(t, voteResponse.Term, initialTerm)
		assertEqual(t, voteResponse.VoterId, id)
		assertEqual(t, voteResponse.VoteGranted, false)
	})
}

func TestHandleAppendEntriesResponse(t *testing.T) {

	dummyNetwork := &TestNetwork{}

	const (
		id          = "NODE"
		otherPeerId = "OTHER_PEER"
	)

	var (
		raftNode *RaftNodeImpl
	)

	createRaftNode := func(id string, term uint64, state RaftState, initLog []*Entry) *RaftNodeImpl {
		rn := &RaftNodeImpl{
			id:              id,
			stateMachine:    nil,
			quitCh:          make(chan bool),
			inboundMessages: make(chan []byte, 1000),
			network:         dummyNetwork,
			state:           state,
			storage:         NewInMemoryStorage(),
			peers:           map[string]bool{id: true, otherPeerId: true},
			voteMap:         nil,
			followersStateMap: map[string]*FollowerState{
				id: {
					nextIndex:  1,
					matchIndex: 0,
				},
				otherPeerId: {
					nextIndex:  1,
					matchIndex: 0,
					pendingRequest: &AppendEntriesRequest{
						RequestId: "random-req-id12345",
					},
				},
			},
			commitIndex:              0,
			lastApplied:              0,
			electionTimeoutTimer:     time.NewTimer(A_LONG_TIME),
			voteResponseTimeoutTimer: time.NewTimer(A_LONG_TIME),
			sendAppendEntriesTicker:  time.NewTicker(A_LONG_TIME),
		}
		if term > 0 {
			rn.storage.SetTerm(term)
		}
		for _, entry := range initLog {
			rn.storage.AppendEntry(*entry)
		}
		return rn
	}

	sendAppendEntriesResponse := func(aeReq *AppendEntriesResponse) {
		msg := Envelope{
			OperationType: AppendEntriesResponseOp,
			Payload:       aeReq.Bytes(),
		}
		raftNode.inboundMessages <- msg.Bytes()
		raftNode.processOneTransistion()
	}

	// from unknown peer:
	// should ignore
	t.Run("unknown_peer", func(t *testing.T) {
		initialTerm := uint64(1)
		raftNode = createRaftNode(id, initialTerm, Leader, []*Entry{})

		sendAppendEntriesResponse(&AppendEntriesResponse{
			Term:        initialTerm + 100,
			Success:     true,
			ResponderId: "UNKNOWN_PEER",
			RequestId:   "foo",
			MatchIndex:  0,
		})
		assertEqual(t, string(dummyNetwork.lastMessageSent), "")
		// should not change term for an unknown peer
		assertEqual(t, raftNode.storage.GetCurrentTerm(), initialTerm)
		assertEqual(t, raftNode.state, Leader)
	})

	// response with a higher term:
	// stepdown if leader/candidate
	// ignore since not leader
	// update term to match resp
	t.Run("stepdown", func(t *testing.T) {
		initialTerm := uint64(1)
		raftNode = createRaftNode(id, initialTerm, Leader, []*Entry{})
		raftNode.acceptingProposals.Store(true)
		raftNode.followersStateMap = make(map[string]*FollowerState)

		responseTerm := initialTerm + 100
		sendAppendEntriesResponse(&AppendEntriesResponse{
			Term:        responseTerm,
			Success:     false,
			ResponderId: otherPeerId,
			MatchIndex:  0,
		})

		assertEqual(t, raftNode.state, Follower)
		assertEqual(t, raftNode.storage.GetCurrentTerm(), responseTerm)
		assertEqual(t, string(dummyNetwork.lastMessageSent), "")
	})

	// resp. w/ outdated term: -> ignore
	t.Run("req_lower_term", func(t *testing.T) {
		initialTerm := uint64(5)
		raftNode = createRaftNode(id, initialTerm, Leader, []*Entry{})
		prevFollowerState := raftNode.followersStateMap[otherPeerId]

		sendAppendEntriesResponse(&AppendEntriesResponse{
			Term:        3,
			Success:     true,
			ResponderId: otherPeerId,
			// NOTE: not possible, just for test purposes
			MatchIndex: 3,
		})
		assertEqual(t, string(dummyNetwork.lastMessageSent), "")
		// should not change term for an unknown peer
		assertEqual(t, raftNode.storage.GetCurrentTerm(), initialTerm)
		assertEqual(t, raftNode.state, Leader)
		assertEqual(t, raftNode.followersStateMap[otherPeerId], prevFollowerState)
	})

	// resp. not successful:
	// lower next index
	// fire a new req. w/ lower prevLogIdx/Term
	t.Run("not_successful", func(t *testing.T) {
		testCases := []struct {
			nextIndex               uint64
			matchIndex              uint64
			expectedEntriesToBeSent []Entry
		}{
			{
				4,
				2,
				[]Entry{
					{Term: 2, Cmd: []byte("baz")},
					{Term: 4, Cmd: []byte("faz")},
				},
			},
			{
				3,
				0,
				[]Entry{
					{Term: 1, Cmd: []byte("bar")},
					{Term: 2, Cmd: []byte("baz")},
					{Term: 4, Cmd: []byte("faz")},
				},
			},
		}

		for _, testCase := range testCases {

			dummyNetwork.lastMessageSent = nil
			initialTerm := uint64(5)
			raftNode = createRaftNode(id, initialTerm, Leader, []*Entry{
				{Term: 1, Cmd: []byte("foo")},
				{Term: 1, Cmd: []byte("bar")},
				{Term: 2, Cmd: []byte("baz")},
				{Term: 4, Cmd: []byte("faz")},
			})

			raftNode.followersStateMap[otherPeerId].nextIndex = testCase.nextIndex
			raftNode.followersStateMap[otherPeerId].matchIndex = testCase.matchIndex

			sendAppendEntriesResponse(&AppendEntriesResponse{
				Term:        initialTerm,
				Success:     false,
				ResponderId: otherPeerId,
				RequestId:   "random-req-id12345",
				// NOTE: not set since resp is false anyway
				MatchIndex: 0,
			})

			newNextIndex := testCase.nextIndex - 1

			expectedPrevLogIdx := newNextIndex - 1
			expectedPrevLogEntry, exists := raftNode.storage.GetLogEntry(expectedPrevLogIdx)
			if !exists {
				t.Fatalf("log entry at index %d does not exist", newNextIndex)
			}
			expectedPrevLogTerm := expectedPrevLogEntry.Term

			expectedNewAEReq := &AppendEntriesRequest{
				Term:            initialTerm,
				LeaderId:        id,
				Entries:         testCase.expectedEntriesToBeSent,
				PrevLogIdx:      expectedPrevLogIdx,
				PrevLogTerm:     expectedPrevLogTerm,
				LeaderCommitIdx: raftNode.commitIndex,
				RequestId:       "random-req-id12345",
			}

			assertEqual(t, raftNode.state, Leader)
			assertEqual(t, raftNode.storage.GetCurrentTerm(), initialTerm)

			// updated next index
			assertEqual(t, raftNode.followersStateMap[otherPeerId].nextIndex, newNextIndex)
			assertEqual(t, raftNode.followersStateMap[otherPeerId].matchIndex, testCase.matchIndex)
			// fired a new append entries request with new next index
			opType, msg, err := parseMessage(dummyNetwork.lastMessageSent)
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, opType, AppendEntriesRequestOp)
			aeReq := msg.(*AppendEntriesRequest)
			assertDeepEqual(t, aeReq, expectedNewAEReq)
		}
	})

	// tests a successful AppendEntries response:
	// update match/next index
	t.Run("successful", func(t *testing.T) {
		testCases := []struct {
			description        string
			initialLeaderLog   []*Entry
			initialNextIndex   uint64
			initialMatchIndex  uint64
			expectedNextIndex  uint64
			responseMatchIndex uint64
		}{
			{
				description:        "valid response to heartbeat with empty log",
				initialLeaderLog:   []*Entry{},
				initialNextIndex:   1,
				initialMatchIndex:  0,
				expectedNextIndex:  1,
				responseMatchIndex: 0,
			},
			{
				description: "valid response to request with one entry",
				initialLeaderLog: []*Entry{
					{Term: 1, Cmd: []byte("foo")},
					{Term: 1, Cmd: []byte("bar")},
					{Term: 2, Cmd: []byte("baz")},
					{Term: 4, Cmd: []byte("faz")},
				},
				initialNextIndex:   4,
				initialMatchIndex:  3,
				expectedNextIndex:  5,
				responseMatchIndex: 4,
			},
			{
				description: "valid response to heartbeat with non-empty log",
				initialLeaderLog: []*Entry{
					{Term: 1, Cmd: []byte("foo")},
					{Term: 1, Cmd: []byte("bar")},
					{Term: 2, Cmd: []byte("baz")},
					{Term: 4, Cmd: []byte("faz")},
				},
				initialNextIndex:   5,
				initialMatchIndex:  4,
				expectedNextIndex:  5,
				responseMatchIndex: 4,
			},
		}

		for _, testCase := range testCases {

			t.Run(testCase.description, func(t *testing.T) {

				dummyNetwork.lastMessageSent = nil
				leaderTerm := uint64(5)
				raftNode = createRaftNode(id, leaderTerm, Leader, testCase.initialLeaderLog)
				initialCommitIndex := raftNode.commitIndex

				raftNode.followersStateMap[otherPeerId].nextIndex = testCase.initialNextIndex
				raftNode.followersStateMap[otherPeerId].matchIndex = testCase.initialMatchIndex

				sendAppendEntriesResponse(&AppendEntriesResponse{
					Term:        leaderTerm,
					Success:     true,
					ResponderId: otherPeerId,
					MatchIndex:  testCase.responseMatchIndex,
				})

				// leader state
				assertEqual(t, raftNode.state, Leader)
				assertEqual(t, raftNode.storage.GetCurrentTerm(), leaderTerm)
				// updated next and match index
				assertEqual(t, raftNode.followersStateMap[otherPeerId].nextIndex, testCase.expectedNextIndex)
				assertEqual(t, raftNode.followersStateMap[otherPeerId].matchIndex, testCase.responseMatchIndex)
				assertEqual(t, raftNode.commitIndex, initialCommitIndex)

			})
		}

	})

}

func TestUpdateLeaderCommitIndex(t *testing.T) {

	dummyNetwork := &TestNetwork{}

	const (
		leaderPeerId           = "LEADER"
		responderPeerId        = "RESPONDER"
		otherPeerId            = "OTHER_PEER"
		LEADER_TERM     uint64 = 4
	)

	var (
		leader *RaftNodeImpl
	)

	createLeader := func(id string, term uint64, commitIndex uint64, initLog []*Entry, initFollowerMap map[string]*FollowerState) *RaftNodeImpl {
		rn := &RaftNodeImpl{
			id:                       id,
			stateMachine:             nil,
			quitCh:                   make(chan bool),
			inboundMessages:          make(chan []byte, 1000),
			network:                  dummyNetwork,
			state:                    Leader,
			storage:                  NewInMemoryStorage(),
			peers:                    map[string]bool{id: true, responderPeerId: true, otherPeerId: true},
			voteMap:                  nil,
			followersStateMap:        initFollowerMap,
			commitIndex:              commitIndex,
			lastApplied:              0,
			electionTimeoutTimer:     time.NewTimer(A_LONG_TIME),
			voteResponseTimeoutTimer: time.NewTimer(A_LONG_TIME),
			sendAppendEntriesTicker:  time.NewTicker(A_LONG_TIME),
		}
		if term > 0 {
			rn.storage.SetTerm(term)
		}
		for _, entry := range initLog {
			rn.storage.AppendEntry(*entry)
		}
		return rn
	}

	sendAppendEntriesResponse := func(aeReq *AppendEntriesResponse) {
		msg := Envelope{
			OperationType: AppendEntriesResponseOp,
			Payload:       aeReq.Bytes(),
		}
		leader.inboundMessages <- msg.Bytes()
		leader.processOneTransistion()
	}

	// NOTE: AppendEntriesResponses will always come from Peer1
	testCases := []struct {
		description               string
		initialLeaderLog          []*Entry
		initialFollowerMap        map[string]*FollowerState
		initialLeaderCommitIndex  uint64
		responseMatchIndex        uint64
		expectedLeaderCommitIndex uint64
	}{
		{
			description:              "heartbeat with empty log",
			initialLeaderLog:         []*Entry{},
			initialLeaderCommitIndex: 0,
			initialFollowerMap: map[string]*FollowerState{
				leaderPeerId: {
					nextIndex:  1,
					matchIndex: 0,
				},
				responderPeerId: {
					nextIndex:  1,
					matchIndex: 0,
				},
				otherPeerId: {
					nextIndex:  1,
					matchIndex: 0,
				},
			},
			responseMatchIndex:        0,
			expectedLeaderCommitIndex: 0,
		},
		{
			description: "follower is caught up, commit all entries",
			initialLeaderLog: []*Entry{
				{Term: 1, Cmd: []byte("foo")},
				{Term: 1, Cmd: []byte("bar")},
				{Term: 2, Cmd: []byte("baz")},
				{Term: 4, Cmd: []byte("faz")},
			},
			initialLeaderCommitIndex: 0,
			initialFollowerMap: map[string]*FollowerState{
				leaderPeerId: {
					nextIndex:  5,
					matchIndex: 4,
				},
				responderPeerId: {
					nextIndex:  1,
					matchIndex: 0,
				},
				otherPeerId: {
					nextIndex:  1,
					matchIndex: 0,
				},
			},
			responseMatchIndex:        4,
			expectedLeaderCommitIndex: 4,
		},
		{
			description: "sent entries to responder before we received them ourselves, no quorum",
			initialLeaderLog: []*Entry{
				{Term: 1, Cmd: []byte("foo")},
				{Term: 1, Cmd: []byte("bar")},
				{Term: 2, Cmd: []byte("baz")},
				{Term: 4, Cmd: []byte("faz")},
			},
			initialLeaderCommitIndex: 0,
			initialFollowerMap: map[string]*FollowerState{
				leaderPeerId: {
					nextIndex:  1,
					matchIndex: 0,
				},
				responderPeerId: {
					nextIndex:  1,
					matchIndex: 0,
				},
				otherPeerId: {
					nextIndex:  1,
					matchIndex: 0,
				},
			},
			responseMatchIndex:        4,
			expectedLeaderCommitIndex: 0,
		},
		{
			description: "achieved quorum but cannot commit entry from previous term",
			initialLeaderLog: []*Entry{
				{Term: 1, Cmd: []byte("foo")},
				{Term: 1, Cmd: []byte("bar")},
				{Term: 2, Cmd: []byte("baz")},
				{Term: 3, Cmd: []byte("faz")},
			},
			initialLeaderCommitIndex: 3,
			initialFollowerMap: map[string]*FollowerState{
				leaderPeerId: {
					nextIndex:  5,
					matchIndex: 4,
				},
				responderPeerId: {
					nextIndex:  4,
					matchIndex: 3,
				},
				otherPeerId: {
					nextIndex:  4,
					matchIndex: 3,
				},
			},
			responseMatchIndex:        4,
			expectedLeaderCommitIndex: 3,
		},
	}

	for _, testCase := range testCases {

		t.Run(testCase.description, func(t *testing.T) {

			dummyNetwork.lastMessageSent = nil
			leader = createLeader(leaderPeerId, LEADER_TERM, testCase.initialLeaderCommitIndex, testCase.initialLeaderLog, testCase.initialFollowerMap)

			sendAppendEntriesResponse(&AppendEntriesResponse{
				Term:        LEADER_TERM,
				Success:     true,
				ResponderId: responderPeerId,
				MatchIndex:  testCase.responseMatchIndex,
			})

			// leader state
			assertEqual(t, leader.state, Leader)
			assertEqual(t, leader.storage.GetCurrentTerm(), LEADER_TERM)
			// updated match index and commit index
			assertEqual(t, leader.followersStateMap[responderPeerId].matchIndex, testCase.responseMatchIndex)
			assertEqual(t, leader.commitIndex, testCase.expectedLeaderCommitIndex)

		})
	}
}

func TestFollowerElectionTimer(t *testing.T) {

	dummyNetwork := &TestNetwork{}

	id := "FOLLOWER_NODE"
	peer := "PEER_NODE"
	electionTimerDuration := 100 * time.Millisecond
	node := &RaftNodeImpl{
		id:                       id,
		stateMachine:             nil,
		quitCh:                   make(chan bool),
		inboundMessages:          make(chan []byte, 1000),
		network:                  dummyNetwork,
		state:                    Follower,
		storage:                  NewInMemoryStorage(),
		peers:                    map[string]bool{id: true, peer: true},
		voteMap:                  nil,
		commitIndex:              0,
		lastApplied:              0,
		electionTimeoutTimer:     time.NewTimer(electionTimerDuration),
		voteResponseTimeoutTimer: time.NewTimer(A_LONG_TIME),
		sendAppendEntriesTicker:  time.NewTicker(A_LONG_TIME),
	}
	node.processOneTransistionInternal(electionTimerDuration / 2)
	// verify it didn't do anything
	assertEqual(t, node.state, Follower)
	assertEqual(t, string(dummyNetwork.lastMessageBroadcasted), "")
	time.Sleep(electionTimerDuration)
	node.processOneTransistion()
	// verify that it started candidacy
	assertEqual(t, node.state, Candidate)
	opType, _, err := parseMessage(dummyNetwork.lastMessageBroadcasted)
	assertNil(t, err)
	assertEqual(t, opType, VoteRequestOp)

	// send a message from a peer w/ a higher term so we can stepdown
	peerTerm := node.storage.GetCurrentTerm() + 1
	stepDownVoteRequest := &VoteRequest{
		Term:         peerTerm,
		CandidateId:  peer,
		LastLogIndex: 1,
		LastLogTerm:  0,
	}
	msg := Envelope{
		OperationType: VoteRequestOp,
		Payload:       stepDownVoteRequest.Bytes(),
	}
	node.inboundMessages <- msg.Bytes()

	node.processOneTransistion()
	// stepped down
	assertEqual(t, node.state, Follower)
	// term is updated
	assertEqual(t, node.storage.GetCurrentTerm(), peerTerm)

	// election timeout, back to candidate
	time.Sleep(maxElectionTimeout)
	node.processOneTransistion()
	// verify that it started candidacy
	assertEqual(t, node.state, Candidate)
	opType, _, err = parseMessage(dummyNetwork.lastMessageBroadcasted)
	assertNil(t, err)
	assertEqual(t, opType, VoteRequestOp)

}

func TestSendAppendEntriesTicker(t *testing.T) {

	dummyNetwork := &TestNetwork{}

	term := uint64(1)
	id := "NODE"
	peer1 := "PEER_1"
	peer2 := "PEER_2"
	electionTimerDuration := 100 * time.Millisecond
	node := &RaftNodeImpl{
		id:                       id,
		stateMachine:             nil,
		quitCh:                   make(chan bool),
		inboundMessages:          make(chan []byte, 1000),
		network:                  dummyNetwork,
		state:                    Candidate,
		storage:                  NewInMemoryStorage(),
		peers:                    map[string]bool{id: true, peer1: true, peer2: true},
		voteMap:                  make(map[string]bool, 3),
		commitIndex:              0,
		lastApplied:              0,
		electionTimeoutTimer:     time.NewTimer(electionTimerDuration),
		voteResponseTimeoutTimer: time.NewTimer(A_LONG_TIME),
		sendAppendEntriesTicker:  time.NewTicker(100 * time.Millisecond),
	}
	node.storage.SetTerm(term)

	//candidate -> leader
	//broadcast ae
	node.ascendToLeader()
	followerToAeTimestamp := make(map[string]time.Time)
	for follower, followerState := range node.followersStateMap {
		followerToAeTimestamp[follower] = followerState.aeTimestamp
		//..check request has been sent and is waiting for response
		assertNotNil(t, followerState.pendingRequest)
	}

	//call processOneTransition#nonBlocking
	node.processOneTransistionInternal(1 * time.Nanosecond)
	for follower, followerState := range node.followersStateMap {
		//..check that we are still waiting for a response
		assertNotNil(t, followerState.pendingRequest)
		//..check aeTimestamp for all followers didn't change
		assertEqual(t, followerState.aeTimestamp, followerToAeTimestamp[follower])
	}

	// leader responds to itself
	{
		aeResponse := &AppendEntriesResponse{
			Term:        term,
			Success:     true,
			ResponderId: id,
			MatchIndex:  0,
		}
		msg := Envelope{
			OperationType: AppendEntriesResponseOp,
			Payload:       aeResponse.Bytes(),
		}
		node.inboundMessages <- msg.Bytes()
		node.processOneTransistion()
	}

	// one of the peers responds
	{
		aeResponse := &AppendEntriesResponse{
			Term:        term,
			Success:     true,
			ResponderId: peer1,
			MatchIndex:  0,
		}
		msg := Envelope{
			OperationType: AppendEntriesResponseOp,
			Payload:       aeResponse.Bytes(),
		}
		node.inboundMessages <- msg.Bytes()
		node.processOneTransistion()
	}

	//..check 2/3 waitingForAEResponse is false, other node is true
	assertNil(t, node.followersStateMap[id].pendingRequest)
	assertNil(t, node.followersStateMap[peer1].pendingRequest)
	// peer has not responded yet
	assertNotNil(t, node.followersStateMap[peer2].pendingRequest)
	peer2Request := node.followersStateMap[peer2].pendingRequest

	//wait for AE timeout
	time.Sleep(aeResponseTimeoutDuration)
	node.processOneTransistion()

	//.. check that leader resends message to 1/3
	assertEqual(t, dummyNetwork.lastRecipient, peer2)
	if dummyNetwork.lastMessageSent == nil {
		t.Fatalf("expected message to be resent to %s", peer2)
	}
	opType, _, err := parseMessage(dummyNetwork.lastMessageSent)
	assertNoErr(t, err)
	assertEqual(t, opType, AppendEntriesRequestOp)
	assertNotNil(t, node.followersStateMap[peer2].pendingRequest)
	assertNotEqual(t, peer2Request.RequestId, node.followersStateMap[peer2].pendingRequest.RequestId)

	//wait -> send heartbeat
	time.Sleep(heartbeatInterval)
	node.processOneTransistion()

	//..check heartbeat sent to 2/3
	opType, _, err = parseMessage(dummyNetwork.lastMessageSent)
	assertNil(t, err)
	assertEqual(t, opType, AppendEntriesRequestOp)
	for _, peer := range []string{id, peer1} {
		//..check 2/3 waitingForAEResponse is true
		assertNotNil(t, node.followersStateMap[peer].pendingRequest)
	}

}

func TestCandidateReceiveVoteResponseTimeoutTimer(t *testing.T) {
	dummyNetwork := &TestNetwork{}

	initTerm := uint64(1)
	id := "NODE"
	peer1 := "PEER_1"
	peer2 := "PEER_2"
	electionTimerDuration := 100 * time.Millisecond
	node := &RaftNodeImpl{
		id:                       id,
		stateMachine:             nil,
		quitCh:                   make(chan bool),
		inboundMessages:          make(chan []byte, 1000),
		network:                  dummyNetwork,
		state:                    Follower,
		storage:                  NewInMemoryStorage(),
		peers:                    map[string]bool{id: true, peer1: true, peer2: true},
		commitIndex:              0,
		lastApplied:              0,
		electionTimeoutTimer:     time.NewTimer(electionTimerDuration),
		voteResponseTimeoutTimer: time.NewTimer(A_LONG_TIME),
		sendAppendEntriesTicker:  time.NewTicker(A_LONG_TIME),
	}
	node.storage.SetTerm(initTerm)

	//OG_follower -> candidate
	time.Sleep(electionTimerDuration)
	node.processOneTransistion()
	//..check state is Candidate
	assertEqual(t, node.state, Candidate)
	//..check term is increased by 1
	assertEqual(t, node.storage.GetCurrentTerm(), initTerm+1)

	//candidate broadcasts vote requests
	//..check lastMessageBroadcasted is VoteRequest
	opType, _, err := parseMessage(dummyNetwork.lastMessageBroadcasted)
	assertNoErr(t, err)
	assertEqual(t, opType, VoteRequestOp)
	//receives votes: yes, no, no_answer
	{
		voteResponse := &VoteResponse{
			Term:        initTerm + 1,
			VoterId:     id,
			VoteGranted: true,
		}
		msg := Envelope{
			OperationType: VoteResponseOp,
			Payload:       voteResponse.Bytes(),
		}
		node.inboundMessages <- msg.Bytes()
		node.processOneTransistion()
	}
	{
		voteResponse := &VoteResponse{
			Term:        initTerm + 1,
			VoterId:     peer1,
			VoteGranted: false,
		}
		msg := Envelope{
			OperationType: VoteResponseOp,
			Payload:       voteResponse.Bytes(),
		}
		node.inboundMessages <- msg.Bytes()
		node.processOneTransistion()
	}
	//..check state is still Candidate
	assertEqual(t, node.state, Candidate)

	//wait for timer to expire
	time.Sleep(voteResponseTimeoutDuration)
	node.processOneTransistion()
	//candidate does relection
	//..check state is Candidate
	assertEqual(t, node.state, Candidate)
	//..check term is OG_follower.Term + 2
	assertEqual(t, node.storage.GetCurrentTerm(), initTerm+2)
	//receives votes: yes, yes, whatever
	{
		voteResponse := &VoteResponse{
			Term:        initTerm + 2,
			VoterId:     id,
			VoteGranted: true,
		}
		msg := Envelope{
			OperationType: VoteResponseOp,
			Payload:       voteResponse.Bytes(),
		}
		node.inboundMessages <- msg.Bytes()
		node.processOneTransistion()
	}
	{
		voteResponse := &VoteResponse{
			Term:        initTerm + 2,
			VoterId:     peer1,
			VoteGranted: true,
		}
		msg := Envelope{
			OperationType: VoteResponseOp,
			Payload:       voteResponse.Bytes(),
		}
		node.inboundMessages <- msg.Bytes()
		node.processOneTransistion()
	}
	//..check state leader
	assertEqual(t, node.state, Leader)

}
