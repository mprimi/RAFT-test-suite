package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/google/uuid"
	"golang.org/x/exp/maps"

	"toy-raft/network"
	"toy-raft/state"
)

const (
	updateChannelBufferSz          int           = 10000
	proposalQueueBufferSz          int           = 100
	heartbeatInterval              time.Duration = 1 * time.Second
	aeResponseTimeoutDuration      time.Duration = 200 * time.Millisecond
	voteResponseTimeoutDuration    time.Duration = 3 * time.Second
	maxElectionTimeout             time.Duration = 6 * time.Second
	minElectionTimeout             time.Duration = 5 * time.Second
	initialElectionTimeoutDuration time.Duration = 1 * time.Second

	// NoPreviousTerm Used in AppendEntriesRequest to denote a "null" consistency check
	NoPreviousTerm = uint64(0)
)

type RaftOperation interface {
	Bytes() []byte
	OpType() OperationType
}

type RaftNodeImpl struct {
	// Replica ID
	id string
	// RAFT Group ID
	groupId string

	stateMachine state.StateMachine

	quitCh chan bool

	inboundMessages chan []byte

	network network.Network

	// -- RAFT -- //
	state RaftState
	// the log
	storage Storage
	// set of peers including self
	peers map[string]bool

	acceptingProposals atomic.Bool

	// Follower
	electionTimeoutTimer *time.Timer

	// Candidate
	voteMap                  map[string]bool
	voteResponseTimeoutTimer *time.Timer

	// Leader only
	followersStateMap       map[string]*FollowerState
	sendAppendEntriesTicker *time.Ticker
	inboundProposals        chan []byte

	// All servers
	// index of highest log entry known to be committed
	commitIndex uint64
	// index of highest log entry applied to state machine
	lastApplied uint64
}

func NewRaftNodeImpl(id string, groupId string, sm state.StateMachine, storage Storage, network network.Network, peers []string) *RaftNodeImpl {
	peersMap := make(map[string]bool, len(peers))
	for _, peer := range peers {
		peersMap[peer] = true
	}
	return &RaftNodeImpl{
		id:               id,
		groupId:          groupId,
		storage:          storage,
		stateMachine:     sm,
		network:          network,
		quitCh:           make(chan bool),
		inboundMessages:  make(chan []byte, updateChannelBufferSz),
		inboundProposals: make(chan []byte, proposalQueueBufferSz),
		peers:            peersMap,
		state:            Follower,
		commitIndex:      0,
		lastApplied:      0,
	}
}

func parseMessage(messageBytes []byte) (OperationType, any, error) {
	var envelope Envelope
	if err := json.Unmarshal(messageBytes, &envelope); err != nil {
		return 0, nil, err
	}

	var message any
	switch envelope.OperationType {
	case VoteRequestOp:
		message = &VoteRequest{}
	case VoteResponseOp:
		message = &VoteResponse{}
	case AppendEntriesRequestOp:
		message = &AppendEntriesRequest{}
	case AppendEntriesResponseOp:
		message = &AppendEntriesResponse{}
	default:
		return 0, nil, fmt.Errorf("unknown operation type %d", envelope.OperationType)
	}
	if err := json.Unmarshal(envelope.Payload, message); err != nil {
		assert.Unreachable(
			"Failed to decode protocol message",
			map[string]any{
				"op":  envelope.OperationType,
				"err": err.Error(),
			},
		)
		return 0, nil, fmt.Errorf("failed to parse message: %w", err)
	}

	return envelope.OperationType, message, nil
}

func (rn *RaftNodeImpl) Start() {
	randomElectionTimeoutDuration := randomTimerDuration(minElectionTimeout, maxElectionTimeout)
	rn.electionTimeoutTimer = time.NewTimer(randomElectionTimeoutDuration)

	// init and stop
	rn.voteResponseTimeoutTimer = time.NewTimer(voteResponseTimeoutDuration)
	stopAndDrainTimer(rn.voteResponseTimeoutTimer)
	rn.sendAppendEntriesTicker = time.NewTicker(heartbeatInterval)
	stopAndDrainTicker(rn.sendAppendEntriesTicker)

	snapshotTicker := time.NewTicker(30 * time.Second)

	go func() {
		for {
			select {
			case <-rn.quitCh:
				return
			case <-snapshotTicker.C:
				if err := rn.maybeSnapshot(); err != nil {
					panic(fmt.Errorf("failed to snapshot: %w", err))
				}
			default:
				rn.processOneTransistion()
			}
		}
	}()
}

func (rn *RaftNodeImpl) processOneTransistion() {
	rn.processOneTransistionInternal(100 * time.Second)
}

func (rn *RaftNodeImpl) processOneTransistionInternal(inactivityTimeout time.Duration) {

	select {
	case proposal := <-rn.inboundProposals:
		// consume proposal
		if rn.state == Leader {
			msgCopy := make([]byte, len(proposal))
			// encode
			entry := Entry{
				Term: rn.storage.GetCurrentTerm(),
				Cmd:  msgCopy,
			}
			if err := rn.storage.AppendEntry(entry); err != nil {
				panic(fmt.Errorf("failed to append entry: %w", err))
			}
		} else {
			rn.Log("ignoring proposal since we are not leader")
		}
	case inboundMessage := <-rn.inboundMessages:
		// handle the new message from network
		opType, message, err := parseMessage(inboundMessage)
		if err != nil {
			rn.Log("bad message: %s", err)
			return
		}

		// Print internal node state and details of inbound request
		rn.LogState("Processing %s", messageToString(opType, message))

		switch opType {
		case AppendEntriesRequestOp:
			rn.handleAppendEntriesRequest(message.(*AppendEntriesRequest))

		case AppendEntriesResponseOp:
			rn.handleAppendEntriesResponse(message.(*AppendEntriesResponse))

		case VoteRequestOp:
			rn.handleVoteRequest(message.(*VoteRequest))

		case VoteResponseOp:
			rn.handleVoteResponse(message.(*VoteResponse))

		default:
			rn.Log("unknown operation type %d", opType)

		}

	case <-rn.electionTimeoutTimer.C:
		// guard:
		if rn.state != Follower {
			panic(fmt.Errorf("election timeout while in state %s", rn.state))
		}
		rn.Log("election timeout, converting to candidate")
		rn.convertToCandidate()

	case <-rn.voteResponseTimeoutTimer.C:
		// guard:
		if rn.state != Candidate {
			panic(fmt.Errorf("vote response timeout while in state %s", rn.state))
		}
		rn.Log("election timeout, restarting campaign")
		rn.convertToCandidate()

	case <-rn.sendAppendEntriesTicker.C:
		// Only ticks if this node is leader
		// May send/resend append entries request and/or heartbeats
		rn.maybeSendAppendEntriesToFollowers()

	case <-time.After(inactivityTimeout):
		// inactivity
	}
}

func (rn *RaftNodeImpl) isKnownPeer(peerId string) bool {
	_, peerExists := rn.peers[peerId]
	return peerExists
}

func (rn *RaftNodeImpl) ascendToLeader() {
	// guard: node should transition to leader iff it was a candidate before
	if rn.state != Candidate {
		assert.Unreachable(
			"Transitioning to leader when not candidate",
			map[string]any{
				"state": rn.state,
			},
		)
		panic(fmt.Errorf("transition to leader from state %s", rn.state))
	}

	// guard: followers map should not exist before node becomes leader
	if rn.followersStateMap != nil {
		panic(fmt.Errorf("followersStateMap is not nil during leader transition"))
	}

	rn.Log("ascending to leader")

	// Set state
	rn.state = Leader
	// Clear vote map, a 'candidate' state construct
	rn.voteMap = nil
	// Stop voteResponseTimeoutTimer, only relevant when 'candidate'
	stopAndDrainTimer(rn.voteResponseTimeoutTimer)
	// Stop electionTimeoutTimer, only relevant if 'candidate' or 'follower'
	stopAndDrainTimer(rn.electionTimeoutTimer)

	// Open the gate to accept proposals (via proposal channel, this is not a lock)
	wasAcceptingProposals := rn.acceptingProposals.CompareAndSwap(false, true)
	// guard: check that node was not accepting proposals before becoming leader
	if !wasAcceptingProposals {
		panic(fmt.Errorf("accepting proposals before being leader"))
	}

	// Prepare to send a first empty AppendEntry request to all followers.
	// This step is necessary to properly set followers match & next indices, which now are just an optimistic guess.

	var prevLogTerm = NoPreviousTerm
	var prevLogIdx = rn.storage.GetLastLogIndex()

	// If log is not empty, find the term for the last entry (at prevLogIdx)
	if prevLogIdx > 0 {
		lastLogEntry, exists := rn.storage.GetLogEntry(prevLogIdx)
		// guard: entry at last log index should exist
		if !exists {
			assert.Unreachable(
				"Failed to look up last log entry",
				map[string]any{
					"lastLogIndex": prevLogIdx,
				},
			)
			panic(fmt.Errorf("last log entry does not exist"))
		}
		// This is the term to include in the next AE Request
		prevLogTerm = lastLogEntry.Term
	}

	// Compose and broadcast the empty AE to all followers
	initialAppendEntryRequest := &AppendEntriesRequest{
		Term:            rn.storage.GetCurrentTerm(),
		LeaderId:        rn.id,
		Entries:         []Entry{},
		PrevLogIdx:      prevLogIdx,
		PrevLogTerm:     prevLogTerm,
		LeaderCommitIdx: rn.commitIndex,
		RequestId:       uuid.NewString(),
	}
	rn.BroadcastMessage(initialAppendEntryRequest)
	rn.Log("broadcast first (empty) AE requests to followers")

	// After sending it, mark down the time it was sent
	now := time.Now()

	// And initialize the followers state
	rn.followersStateMap = make(map[string]*FollowerState, len(rn.peers))
	for peerId := range rn.peers {
		rn.followersStateMap[peerId] = &FollowerState{
			nextIndex:      rn.storage.GetLastLogIndex() + 1,
			matchIndex:     0,
			aeTimestamp:    now,
			pendingRequest: initialAppendEntryRequest,
		}
	}

	// Start the ticker that periodically checks for AE responses timeout and re-sends them as necessary
	resetAndDrainTicker(rn.sendAppendEntriesTicker, aeResponseTimeoutDuration)
}

func (rn *RaftNodeImpl) convertToCandidate() {
	rn.state = Candidate

	// candidate election timer
	resetAndDrainTimer(rn.voteResponseTimeoutTimer, voteResponseTimeoutDuration)
	// stop election timer
	stopAndDrainTimer(rn.electionTimeoutTimer)
	// stop followers append entries ticker
	stopAndDrainTicker(rn.sendAppendEntriesTicker)

	// reset vote map
	rn.voteMap = make(map[string]bool)
	// increment term
	currentTerm := rn.storage.IncrementTerm()
	// vote for self
	rn.storage.VoteFor(rn.id, currentTerm)
	rn.voteMap[rn.id] = true
	// request votes from other nodes
	rn.requestVotes(currentTerm, rn.id)
	rn.Log("converted to candidate, requested votes from other nodes")
}

// this method is triggered by receiving an RPC with a higher term, regardless of state
// TODO: turn the following logic into a switch-case,
// remove unconditional reset of electionTimer
// restart electionTimer for valid previousStates (Candidate|Leader)
func (rn *RaftNodeImpl) stepdown() {

	currentState := rn.state
	{
		// has followers state iff leader
		if currentState == Leader && rn.followersStateMap == nil {
			panic(fmt.Errorf("leader followersStateMap is nil when stepping down"))
		} else if currentState != Leader && rn.followersStateMap != nil {
			panic(fmt.Errorf("followersStateMap present when stepping down from state: %s", currentState))
		}

		// has vote map iff candidate
		if currentState == Candidate && rn.voteMap == nil {
			panic(fmt.Errorf("candidate voteMap is nil when stepping down"))
		} else if currentState != Candidate && rn.voteMap != nil {
			panic(fmt.Errorf("voteMap present when stepping down from state: %s", currentState))
		}
	}

	switch currentState {
	case Leader:
		rn.followersStateMap = nil
		stopAndDrainTicker(rn.sendAppendEntriesTicker)
		rn.Log("leader stepped down, cleared followersStateMap, and stopped sendAppendEntriesTicker")
		resetAndRestartTimer(rn.electionTimeoutTimer, randomTimerDuration(minElectionTimeout, maxElectionTimeout))
		swapped := rn.acceptingProposals.CompareAndSwap(true, false)
		// guard:
		if !swapped {
			panic(fmt.Errorf("was not accepting proposals as leader"))
		}
	case Candidate:
		rn.voteMap = nil
		stopAndDrainTimer(rn.voteResponseTimeoutTimer)
		rn.Log("candidate stepped down, cleared voteMap, and stopped voteResponseTimeoutTimer")
		resetAndRestartTimer(rn.electionTimeoutTimer, randomTimerDuration(minElectionTimeout, maxElectionTimeout))
	case Follower:
		// already follower
	default:
		panic(fmt.Errorf("unknown raft state %s", currentState))
	}

	rn.state = Follower
}

// this method is triggered by receiving an RPC with a higher term, regardless of state
func (rn *RaftNodeImpl) stepdownDueToHigherTerm(term uint64) {
	rn.stepdown()
	rn.storage.SetTerm(term)
}

func (rn *RaftNodeImpl) requestVotes(term uint64, candidateId string) {

	lastLogIndex, lastLogEntryTerm := rn.storage.GetLastLogIndexAndTerm()
	voteRequest := &VoteRequest{
		Term:         term,
		CandidateId:  candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogEntryTerm,
	}
	rn.BroadcastMessage(voteRequest)
}

func (rn *RaftNodeImpl) applyUpdate(update *Entry) {
	rn.stateMachine.Apply(update.Cmd)
}

func (rn *RaftNodeImpl) Stop() {
	rn.quitCh <- true
}

func (rn *RaftNodeImpl) Log(format string, args ...any) {

	stateIcon := func() string {
		switch rn.state {
		case Leader:
			return "👑 LEAD"
		case Candidate:
			return "🗳️ CAND"
		case Follower:
			return "🪵 FOLL"
		default:
			panic(fmt.Errorf("unknown raft state %s", rn.state))
		}
	}

	header := fmt.Sprintf(
		"[RAFT %s] [%s - %s - T:%d LLI:%d C:%d A:%d] ",
		rn.groupId,
		rn.id,
		stateIcon(),
		rn.storage.GetCurrentTerm(),
		rn.storage.GetLastLogIndex(),
		rn.commitIndex,
		rn.lastApplied,
	)
	log.Printf(header+format+"\n", args...)
}

func (rn *RaftNodeImpl) LogState(format string, args ...any) {

	b := strings.Builder{}
	b.WriteString("{")
	b.WriteString(fmt.Sprintf(" peers: %v", maps.Keys(rn.peers)))
	switch rn.state {
	case Follower:
	case Candidate:
		b.WriteString(fmt.Sprintf(" votes: %v", maps.Keys(rn.voteMap)))
	case Leader:
		b.WriteString(" followers: [")
		for followerName, followerState := range rn.followersStateMap {
			b.WriteString(" " + followerName + ":{")
			b.WriteString(fmt.Sprintf(" NI:%d", followerState.nextIndex))
			b.WriteString(fmt.Sprintf(" MI:%d", followerState.matchIndex))
			if followerState.pendingRequest == nil {
				b.WriteString(fmt.Sprintf(" Next AE:%s", (heartbeatInterval - time.Since(followerState.aeTimestamp)).Round(time.Millisecond)))
			} else {
				b.WriteString(fmt.Sprintf(
					" AE-TO:%s (RID:%s)",
					(aeResponseTimeoutDuration - time.Since(followerState.aeTimestamp)).Round(time.Millisecond),
					followerState.pendingRequest.RequestId,
				))
			}
			b.WriteString("}")
		}
		b.WriteString(" ]")
	default:
		panic(fmt.Errorf("unknown raft state %s", rn.state))
	}
	b.WriteString(" } ")

	rn.Log(b.String()+format, args...)
}

var ErrNotLeader = fmt.Errorf("not leader")

func (rn *RaftNodeImpl) Propose(msg []byte) error {
	// HACK:this way of accepting proposals might have false positives/negatives
	if rn.acceptingProposals.Load() {
		proposal := make([]byte, len(msg))
		bytesCopied := copy(proposal, msg)
		if len(msg) != bytesCopied {
			panic("failed to copy buffer")
		}
		//TODO: inbound proposals channel should be bound, this call should fail if it the channel is full
		rn.inboundProposals <- proposal
	} else {
		return ErrNotLeader
	}
	return nil
}

func (rn *RaftNodeImpl) Receive(msg []byte) {
	rn.inboundMessages <- msg
}

func (rn *RaftNodeImpl) BroadcastMessage(msg RaftOperation) {
	opType := msg.OpType()
	msgEnvelope := Envelope{
		OperationType: opType,
		Payload:       msg.Bytes(),
	}
	rn.Log("Broadcast: %s", messageToString(opType, msg))
	rn.network.Broadcast(msgEnvelope.Bytes())
}

func (rn *RaftNodeImpl) SendMessage(peerId string, msg RaftOperation) {
	//TODO add some guards to validate outbound messages:
	// e.g.: AppendEntryRequest
	//         - IFF previousLogIndex > 0 then PreviousLogTerm != 0
	//         - RequestID not empty
	opType := msg.OpType()
	msgEnvelope := Envelope{
		OperationType: opType,
		Payload:       msg.Bytes(),
	}
	rn.Log("Send to %s: %s", peerId, messageToString(opType, msg))
	rn.network.Send(peerId, msgEnvelope.Bytes())
}

// If last log index ≥ nextIndex for a follower: send
// AppendEntries RPC with log entries starting at nextIndex
func (rn *RaftNodeImpl) entriesToSendToFollower(followerId string) []Entry {
	if rn.storage.GetLastLogIndex() >= rn.followersStateMap[followerId].nextIndex {
		return rn.storage.GetLogEntriesFrom(rn.followersStateMap[followerId].nextIndex)
	}
	return []Entry{}
}

func (rn *RaftNodeImpl) handleAppendEntriesRequest(appendEntriesRequest *AppendEntriesRequest) {
	// get current term before we process a message
	currentTerm := rn.storage.GetCurrentTerm()

	// peer is unknown, ignore request
	if !rn.isKnownPeer(appendEntriesRequest.LeaderId) {
		rn.Log("ignoring AppendEntries request from unknown peer: %s", appendEntriesRequest.LeaderId)
		return
	}

	// request has higher term, stepdown and update term
	if appendEntriesRequest.Term > currentTerm {
		rn.Log("stepping down and updating term (currentTerm: %d -> requestTerm: %d) due to AppendEntries request having a higher term", currentTerm, appendEntriesRequest.Term)
		// set new term to append entries request term
		rn.stepdownDueToHigherTerm(appendEntriesRequest.Term)
		// refresh value
		currentTerm = rn.storage.GetCurrentTerm()
	}

	resp := &AppendEntriesResponse{
		Term:        currentTerm,
		Success:     false,
		ResponderId: rn.id,
		MatchIndex:  0,
		RequestId:   appendEntriesRequest.RequestId,
	}

	// request's term is lower than current term, deny request
	if appendEntriesRequest.Term < currentTerm {
		rn.SendMessage(appendEntriesRequest.LeaderId, resp)
		return
	} else if rn.state == Follower {
		// refresh election timer
		// NOTE: any state can receive an AE req, but the timer should only be reset for a follower
		resetAndDrainTimer(rn.electionTimeoutTimer, randomTimerDuration(minElectionTimeout, maxElectionTimeout))
	} else if rn.state == Candidate {
		// stepdown if a leader with the same term as us is found
		rn.stepdown()
	}

	// guard:
	if appendEntriesRequest.PrevLogIdx > 0 && appendEntriesRequest.PrevLogTerm == NoPreviousTerm {
		// Request is invalid, leader should never send such a thing
		assert.Unreachable(
			"Invalid request consistency check values",
			map[string]any{
				"leader":      appendEntriesRequest.LeaderId,
				"term":        appendEntriesRequest.Term,
				"prevLogIdx":  appendEntriesRequest.PrevLogIdx,
				"prevLogTerm": appendEntriesRequest.PrevLogTerm,
			},
		)
		rn.Log(
			"Leader sent invalid AERequest with PrevLogIdx: %d and PrevLogTerm: %d, ignoring",
			appendEntriesRequest.PrevLogIdx,
			appendEntriesRequest.PrevLogTerm,
		)
		return
	}

	// check if log state is consistent with leader
	if appendEntriesRequest.PrevLogTerm != NoPreviousTerm {
		// no entry exists
		entry, exists := rn.storage.GetLogEntry(appendEntriesRequest.PrevLogIdx)
		if !exists {
			rn.Log("found non-existent log entry at index %d when comparing with leader", appendEntriesRequest.PrevLogIdx)
			resp.Success = false
			rn.SendMessage(appendEntriesRequest.LeaderId, resp)
			return
		} else if entry.Term != appendEntriesRequest.PrevLogTerm {
			rn.Log("discovered log inconsistency with leader at index %d, expected term %d, got term %d", appendEntriesRequest.PrevLogIdx, appendEntriesRequest.PrevLogTerm, entry.Term)
			resp.Success = false
			rn.SendMessage(appendEntriesRequest.LeaderId, resp)
			return
		}
	}

	// append entries from request
	logEntryToBeAddedIdx := appendEntriesRequest.PrevLogIdx + 1
	rn.Log("attempting to add %d entries to log starting at index %d", len(appendEntriesRequest.Entries), logEntryToBeAddedIdx)
	for i, entry := range appendEntriesRequest.Entries {
		logEntry, exists := rn.storage.GetLogEntry(logEntryToBeAddedIdx)
		if !exists {
			rn.Log("appending entry %d/%d (%+v) at index %d", i+1, len(appendEntriesRequest.Entries), entry, logEntryToBeAddedIdx)
			err := rn.storage.AppendEntry(entry)
			if err != nil {
				panic(fmt.Errorf("failed to append entry: %w", err))
			}
		} else if entry.Term != logEntry.Term {
			rn.Log("deleting entries from index %d", logEntryToBeAddedIdx)
			rn.storage.DeleteEntriesFrom(logEntryToBeAddedIdx)
			rn.Log("appending entry %d/%d (%+v) at index %d", i+1, len(appendEntriesRequest.Entries), entry, logEntryToBeAddedIdx)
			err := rn.storage.AppendEntry(entry)
			if err != nil {
				panic(fmt.Errorf("failed to append entry: %w", err))
			}
		} else {
			rn.Log("entry %d/%d, already exists at index %d", i+1, len(appendEntriesRequest.Entries), logEntryToBeAddedIdx)
		}
		logEntryToBeAddedIdx++
	}

	// update commit index
	indexOfLastNewEntry := appendEntriesRequest.PrevLogIdx + uint64(len(appendEntriesRequest.Entries))
	if appendEntriesRequest.LeaderCommitIdx > rn.commitIndex {
		prevCommitIndex := rn.commitIndex
		rn.commitIndex = min(appendEntriesRequest.LeaderCommitIdx, indexOfLastNewEntry)
		// guard: commit index should only increase monotonically
		if rn.commitIndex < prevCommitIndex {
			assert.Unreachable(
				"Non monotonic commit index",
				map[string]any{
					"commitIndex":          rn.commitIndex,
					"prevCommitIndex":      prevCommitIndex,
					"aeRequestCommitIndex": appendEntriesRequest.LeaderCommitIdx,
					"indexOfLastNewEntry":  indexOfLastNewEntry,
					"aeRequestSender":      appendEntriesRequest.LeaderId,
					"aeRequestTerm":        appendEntriesRequest.Term,
				},
			)
			panic(fmt.Errorf("attempting to decrease commit index"))
		}
	}

	resp.Success = true
	resp.MatchIndex = appendEntriesRequest.PrevLogIdx + uint64(len(appendEntriesRequest.Entries))

	rn.SendMessage(appendEntriesRequest.LeaderId, resp)

	// guard:
	if rn.commitIndex > rn.storage.GetLastLogIndex() {
		assert.Unreachable(
			"Commit index points to entries not in log",
			map[string]any{
				"commitIndex":          rn.commitIndex,
				"lastLogIndex":         rn.storage.GetLastLogIndex(),
				"aeRequestCommitIndex": appendEntriesRequest.LeaderCommitIdx,
				"indexOfLastNewEntry":  indexOfLastNewEntry,
				"aeRequestSender":      appendEntriesRequest.LeaderId,
				"aeRequestTerm":        appendEntriesRequest.Term,
			},
		)
		panic(fmt.Errorf("attempting to set commit index beyond last log index"))
	}

	// apply newly committed entries
	for i := rn.lastApplied + 1; i <= rn.commitIndex; i++ {
		entry, exists := rn.storage.GetLogEntry(i)
		// guard:
		if !exists || entry == nil {
			assert.Unreachable(
				"Commit index points to entries not in log",
				map[string]any{
					"lastApplied":          rn.lastApplied,
					"i":                    i,
					"entryExists":          exists,
					"entryIsNil":           entry == nil,
					"lastLogIndex":         rn.storage.GetLastLogIndex(),
					"aeRequestCommitIndex": appendEntriesRequest.LeaderCommitIdx,
					"indexOfLastNewEntry":  indexOfLastNewEntry,
					"aeRequestSender":      appendEntriesRequest.LeaderId,
					"aeRequestTerm":        appendEntriesRequest.Term,
				},
			)
			panic(fmt.Errorf("attempting to apply entry that is not in log"))
		}
		rn.Log("applying entry %d to state machine", i)
		rn.applyUpdate(entry)
	}
	rn.lastApplied = rn.commitIndex

}

func (rn *RaftNodeImpl) handleAppendEntriesResponse(appendEntriesResponse *AppendEntriesResponse) {
	currentTerm := rn.storage.GetCurrentTerm()

	if !rn.isKnownPeer(appendEntriesResponse.ResponderId) {
		rn.Log("ignoring append entries response from unknown peer: %s", appendEntriesResponse.ResponderId)
		return
	}

	if appendEntriesResponse.Term > currentTerm {
		// set new term to vote request term
		rn.Log("append entries response with a higher term: %d", appendEntriesResponse.Term)
		rn.stepdownDueToHigherTerm(appendEntriesResponse.Term)
		return
	}

	if rn.state != Leader {
		rn.Log("ignoring append entries response as not leader")
		return
	}

	if appendEntriesResponse.Term < currentTerm {
		rn.Log("ignoring append entries response with a lower term: %d", appendEntriesResponse.Term)
		return
	}

	followerState, exists := rn.followersStateMap[appendEntriesResponse.ResponderId]
	// guard:
	if !exists {
		assert.Unreachable(
			"Peer not present in followers map",
			map[string]any{
				"peers":             maps.Keys(rn.peers),
				"followers":         maps.Keys(rn.followersStateMap),
				"followerResponder": appendEntriesResponse.ResponderId,
			},
		)
		panic(fmt.Errorf("peer is not in followers map"))
	}

	if followerState.pendingRequest == nil {
		rn.Log("ignoring append entries response, not waiting for a response from this follower")
		return
	}

	if followerState.pendingRequest.RequestId != appendEntriesResponse.RequestId {
		rn.Log("ignoring append entries response for old request")
		return
	}

	// successfully received a response from this follower
	followerState.pendingRequest = nil

	matchIndexUpdated := false
	if appendEntriesResponse.Success {
		// guard:
		if appendEntriesResponse.MatchIndex < followerState.matchIndex {
			assert.Unreachable(
				"Match index in response is lower than expected",
				map[string]any{
					"aeResponseMatchIndex": appendEntriesResponse.MatchIndex,
					"followerMatchIndex":   followerState.matchIndex,
					"followerResponder":    appendEntriesResponse.ResponderId,
					"followerState":        followerState,
				},
			)
			panic(fmt.Errorf("match index lower than follower match index"))
		} else if followerState.matchIndex == appendEntriesResponse.MatchIndex {
			// match index didn't change
		} else {
			matchIndexUpdated = true
			followerState.matchIndex = appendEntriesResponse.MatchIndex
			followerState.nextIndex = followerState.matchIndex + 1
		}
	} else {
		// NOTE: this only executes if log doesn't match

		// minimum next index is 1
		if followerState.nextIndex > 1 {
			followerState.nextIndex -= 1
		}

		// guard:
		if followerState.nextIndex <= followerState.matchIndex {
			// TODO maybe should consider this request invalid and ignore it?
			// TODO maybe should add this guard before sending the response
			assert.Unreachable(
				"Invalid follower state, nextIndex is not greater than matchIndex",
				map[string]any{
					"followerResponder":    appendEntriesResponse.ResponderId,
					"followerState":        followerState,
					"aeResponseMatchIndex": appendEntriesResponse.MatchIndex,
					"aeResponseTerm":       appendEntriesResponse.Term,
				},
			)
			panic(fmt.Errorf("follower nextIndex <= matchIndex "))
		}

		prevLogIndex := followerState.nextIndex - 1
		prevLogTerm := NoPreviousTerm
		if prevLogIndex > 0 {
			prevLogEntry, exists := rn.storage.GetLogEntry(prevLogIndex)
			// guard:
			if !exists {
				assert.Unreachable(
					"Entry for follower does not exist",
					map[string]any{
						"followerResponder":    appendEntriesResponse.ResponderId,
						"followerState":        followerState,
						"aeResponseMatchIndex": appendEntriesResponse.MatchIndex,
						"aeResponseTerm":       appendEntriesResponse.Term,
						"prevLogIndex":         prevLogIndex,
					},
				)
				panic(fmt.Errorf("log entry does not exist"))
			}
			prevLogTerm = prevLogEntry.Term
		}

		entries := rn.entriesToSendToFollower(appendEntriesResponse.ResponderId)

		aeRequest := &AppendEntriesRequest{
			Term:            currentTerm,
			LeaderId:        rn.id,
			Entries:         entries,
			PrevLogIdx:      prevLogIndex,
			PrevLogTerm:     prevLogTerm,
			LeaderCommitIdx: rn.commitIndex,
			RequestId:       uuid.NewString(),
		}
		rn.SendMessage(appendEntriesResponse.ResponderId, aeRequest)
		followerState.aeTimestamp = time.Now()
		followerState.pendingRequest = aeRequest
	}

	// commit index only is incremented if matchIndex has been changed
	if matchIndexUpdated {
		// guard:
		if currentTerm != rn.storage.GetCurrentTerm() {
			assert.Unreachable(
				"Unexpected term",
				map[string]any{
					"currentTerm": currentTerm,
					"storedTerm":  rn.storage.GetCurrentTerm(),
				},
			)
			panic(fmt.Errorf("unexpected term change while processing AppendEntriesResponse"))
		}

		quorum := len(rn.peers)/2 + 1
		lastLogIndex := rn.storage.GetLastLogIndex()

		upperBound := min(appendEntriesResponse.MatchIndex, lastLogIndex)
		lowerBound := rn.commitIndex + 1

		for n := upperBound; n >= lowerBound; n-- {
			logEntry, exists := rn.storage.GetLogEntry(n)
			// guard:
			if !exists {
				assert.Unreachable(
					"Attempting to load entry that does not exist",
					map[string]any{
						"index":        n,
						"upperBound":   upperBound,
						"lowerBound":   lowerBound,
						"lastLogIndex": lastLogIndex,
					},
				)
				panic(fmt.Errorf("attempt to load entry that does not exist"))
			}

			// NOTE: as an optimization we could just break here since it is guaranteed that all
			// entries previous to this will have lower terms than us
			if currentTerm != logEntry.Term {
				rn.Log("cannot set commitIndex to %d, term mismatch", n)
				continue
			}
			// count how many peer's log matches leader's upto N
			count := 0
			for _, followerState := range rn.followersStateMap {
				if followerState.matchIndex >= n {
					count++
				}
			}
			// majority of peers has entry[n], commit entries up to N
			if count >= quorum {
				rn.commitIndex = n
				rn.Log("commit index updated to %d", n)
				break
			}
		}
	}
}

func (rn *RaftNodeImpl) handleVoteRequest(voteRequest *VoteRequest) {
	currentTerm := rn.storage.GetCurrentTerm()

	lastLogIndex, lastLogEntryTerm := rn.storage.GetLastLogIndexAndTerm()

	if !rn.isKnownPeer(voteRequest.CandidateId) {
		rn.Log("ignoring vote request from unknown peer: %s", voteRequest.CandidateId)
		return
	}

	if voteRequest.Term > currentTerm {
		// set new term to vote request term
		rn.Log("vote request with a higher term, currentTerm: %d, voteRequestTerm: %d", currentTerm, voteRequest.Term)
		rn.stepdownDueToHigherTerm(voteRequest.Term)
		// refresh value
		currentTerm = rn.storage.GetCurrentTerm()
	}

	var voteGranted bool
	if voteRequest.Term < currentTerm {
		rn.Log("vote not granted to %s, voteRequestTerm %d < currentTerm %d", voteRequest.CandidateId, voteRequest.Term, currentTerm)
		voteGranted = false
	} else if rn.storage.Voted() && rn.storage.GetVotedFor() != voteRequest.CandidateId {
		rn.Log("vote not granted to %s, already voted for %s in term %d", voteRequest.CandidateId, rn.storage.GetVotedFor(), rn.storage.GetCurrentTerm())
		voteGranted = false
	} else if lastLogEntryTerm > voteRequest.LastLogTerm {
		rn.Log("vote not granted to %s, lastLogTerm %d > voteRequestLastLogTerm %d", voteRequest.CandidateId, lastLogEntryTerm, voteRequest.LastLogTerm)
		voteGranted = false
	} else if lastLogEntryTerm == voteRequest.LastLogTerm && lastLogIndex > voteRequest.LastLogIndex {
		rn.Log("vote not granted to %s, lastLogIndex %d > voteRequestLastLogIndex %d with same term %d", voteRequest.CandidateId, lastLogIndex, voteRequest.LastLogIndex, lastLogEntryTerm)
		voteGranted = false
	} else if rn.storage.Voted() && rn.storage.GetVotedFor() == voteRequest.CandidateId {
		rn.Log("already voted %s for them in term: %d, granted vote anyway", voteRequest.CandidateId, currentTerm)
		voteGranted = true
	} else {
		rn.Log("granted vote to %s with term %d", voteRequest.CandidateId, voteRequest.Term)
		voteGranted = true
		rn.storage.VoteFor(voteRequest.CandidateId, voteRequest.Term)
	}

	// do not reset timer for useless vote requests
	// timer will only be running if we are a follower
	if voteGranted && rn.state == Follower {
		resetAndDrainTimer(rn.electionTimeoutTimer, randomTimerDuration(minElectionTimeout, maxElectionTimeout))
	}

	// send vote response to candidate
	resp := &VoteResponse{
		Term:        currentTerm,
		VoteGranted: voteGranted,
		VoterId:     rn.id,
	}
	rn.SendMessage(voteRequest.CandidateId, resp)
}

func (rn *RaftNodeImpl) handleVoteResponse(voteResponse *VoteResponse) {
	currentTerm := rn.storage.GetCurrentTerm()

	if !rn.isKnownPeer(voteResponse.VoterId) {
		rn.Log("ignoring vote response from unknown peer: %s", voteResponse.VoterId)
		return
	}

	currentTerm = rn.storage.GetCurrentTerm()
	if voteResponse.Term > currentTerm {
		rn.Log("received vote response with a higher term, voteResponseTerm: %d", voteResponse.Term)
		rn.stepdownDueToHigherTerm(voteResponse.Term)
		return
	} else if voteResponse.Term < currentTerm {
		rn.Log("ignoring vote response from previous term %d", voteResponse.Term)
		return
	} else {
		rn.Log("received vote response from %s", voteResponse.VoterId)
	}

	// if we are not candidate, ignore
	if rn.state != Candidate {
		rn.Log("ignoring vote response, not a candidate")
		return
	}

	if !voteResponse.VoteGranted {
		rn.Log("voter %s voted no", voteResponse.VoterId)
		return
	}

	_, exists := rn.voteMap[voteResponse.VoterId]
	if exists {
		rn.Log("received duplicate vote from %s", voteResponse.VoterId)
		return
	}

	// add vote to map
	rn.Log("recording vote from %s", voteResponse.VoterId)
	rn.voteMap[voteResponse.VoterId] = true

	voteCount := len(rn.voteMap)
	numPeers := len(rn.peers)

	// majority
	if voteCount >= (numPeers/2)+1 {
		rn.ascendToLeader()
	}
}

func (rn *RaftNodeImpl) maybeSendAppendEntriesToFollowers() {
	// guard: this is expected to be invoked only by a node in leader state
	if rn.state != Leader {
		panic(fmt.Errorf("send append entries tick in state %s", rn.state))
	}

	currentTerm := rn.storage.GetCurrentTerm()

	for followerId, followerState := range rn.followersStateMap {
		if followerState.pendingRequest != nil && time.Since(followerState.aeTimestamp) > aeResponseTimeoutDuration {
			// Previous request timed out, send it again
			rn.Log("AE response timeout, re-sending last AE request to %s", followerId)
			rn.SendMessage(followerId, followerState.pendingRequest)
			followerState.aeTimestamp = time.Now()
		} else if followerState.pendingRequest == nil && time.Since(followerState.aeTimestamp) > heartbeatInterval {
			// It's time to send a new heartbeat or next AE request to this follower
			rn.Log("Composing next AERequest for %s", followerId)

			// Choose previous log index and corresponding term for this follower consistency check portion of the
			// AE Request
			var prevLogTerm = NoPreviousTerm
			prevLogIndex := followerState.nextIndex - 1

			if prevLogIndex > 0 {
				// Unless sending from the start of the log, look up the term for the last matching index
				prevLogEntry, exists := rn.storage.GetLogEntry(prevLogIndex)
				// guard: if looking up an entry, make sure it exists
				if !exists {
					assert.Unreachable(
						"Load entry that does not exist",
						map[string]any{
							"followerId":    followerId,
							"followerState": followerState,
							"index":         prevLogIndex,
							"lastLogIndex":  rn.storage.GetLastLogIndex(),
						},
					)
					panic(fmt.Errorf("attempting to load non-existent entry"))
				}
				// Save the term for this entry
				prevLogTerm = prevLogEntry.Term
			}

			// Load entries (if any) for this follower starting at match index + 1
			entriesToSend := rn.entriesToSendToFollower(followerId)
			aeRequest := &AppendEntriesRequest{
				Term:            currentTerm,
				LeaderId:        rn.id,
				Entries:         entriesToSend,
				PrevLogIdx:      prevLogIndex,
				PrevLogTerm:     prevLogTerm,
				LeaderCommitIdx: rn.commitIndex,
				RequestId:       uuid.NewString(),
			}
			rn.SendMessage(followerId, aeRequest)
			followerState.aeTimestamp = time.Now()
			followerState.pendingRequest = aeRequest
		} else {
			// Don't send a request to this follower
		}
	}
}

func resetAndRestartTimer(t *time.Timer, resetDuration time.Duration) {
	resetAndDrainTimer(t, resetDuration)
}

func resetAndDrainTimer(t *time.Timer, resetDuration time.Duration) {
	stopAndDrainTimer(t)
	t.Reset(resetDuration)
}

func resetAndDrainTicker(t *time.Ticker, resetDuration time.Duration) {
	stopAndDrainTicker(t)
	t.Reset(resetDuration)
}

func stopAndDrainTimer(t *time.Timer) {
	t.Stop()
	for {
		select {
		case <-t.C:
		default:
			return
		}
	}
}

func stopAndDrainTicker(t *time.Ticker) {
	t.Stop()
	for {
		select {
		case <-t.C:
		default:
			return
		}
	}
}

func randomTimerDuration(min time.Duration, max time.Duration) time.Duration {
	return min + time.Duration(rand.Float64()*(float64(max-min)))
}

func messageToString(opType OperationType, message any) string {

	var msgString string

	switch opType {
	case VoteRequestOp:
		voteRequest := message.(*VoteRequest)
		msgString = fmt.Sprintf(
			"FROM:%s T:%d LLI:%d LLT:%d",
			voteRequest.CandidateId,
			voteRequest.Term,
			voteRequest.LastLogIndex,
			voteRequest.LastLogTerm,
		)
	case VoteResponseOp:
		voteResponse := message.(*VoteResponse)
		msgString = fmt.Sprintf(
			"FROM:%s T:%d G?:%v",
			voteResponse.VoterId,
			voteResponse.Term,
			voteResponse.VoteGranted,
		)
	case AppendEntriesRequestOp:
		appendEntriesRequest := message.(*AppendEntriesRequest)

		totalEntriesSize := uint64(0)
		entriesSummary := strings.Builder{}
		entriesSummary.WriteString("[")
		for i, entry := range appendEntriesRequest.Entries {
			entrySize := uint64(len(entry.Cmd))
			totalEntriesSize += entrySize
			entriesSummary.WriteString(
				fmt.Sprintf(
					"{(I:%d) T:%d [...] %dB}",
					appendEntriesRequest.PrevLogIdx+uint64(i+1),
					entry.Term,
					entrySize,
				),
			)
		}
		entriesSummary.WriteString(
			fmt.Sprintf("](%d - %dB)", len(appendEntriesRequest.Entries), totalEntriesSize),
		)
		msgString = fmt.Sprintf(
			"FROM:%s T:%d PLI:%d PLT:%d LCI:%d ID:%s E:%s",
			appendEntriesRequest.LeaderId,
			appendEntriesRequest.Term,
			appendEntriesRequest.PrevLogIdx,
			appendEntriesRequest.PrevLogTerm,
			appendEntriesRequest.LeaderCommitIdx,
			appendEntriesRequest.RequestId,
			entriesSummary.String(),
		)
	case AppendEntriesResponseOp:
		appendEntriesResponse := message.(*AppendEntriesResponse)
		msgString = fmt.Sprintf(
			"FROM:%s T:%d MI:%d S?:%v RID:%s",
			appendEntriesResponse.ResponderId,
			appendEntriesResponse.Term,
			appendEntriesResponse.MatchIndex,
			appendEntriesResponse.Success,
			appendEntriesResponse.RequestId,
		)

	default:
		panic(fmt.Sprintf("unknown message type %s", opType))
	}

	return fmt.Sprintf("%s:{%s}", opType, msgString)
}

// maybeSnapshot will create snapshot
func (rn *RaftNodeImpl) maybeSnapshot() error {

	/*
		if stateMachine.Applied - lastSnapshot.Applied < snapshotThreshold -> return

		create a filename
		open file and create a writer
		pass writer to statemachine#Snapshot
		close the file

		create a snapshot_metadata filename
		open snapshot_metadata file
		write checksum and stateMachine.Applied of snapshot to metadatafile
		close metadata file

		trim the log upto stateMachine.Applied

		in AeReqHandling (follower):
			[if the prevLogIndex is lt node.Committed -> ignore req]
			will save you from doing the consistency check on entries that might have been trimmed away

		in AeRespHandling (leader):
			if resp.success is false and follower.nextIndex is lt trimThreshold -> InstallSnapshot() on follower
			how do we handle in flight snapshots and heartbeats??
			how do we handle timeout of InstallSnapshot()?

	*/

	return nil
}
