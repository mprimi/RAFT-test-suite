package raft

import (
	"encoding/json"
	"time"
)

type RaftState uint64

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (rs RaftState) String() string {
	switch rs {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "unknown state"
	}
}

type Entry struct {
	Term uint64 `json:"term"`
	Cmd  []byte `json:"cmd"`
}

func (e *Entry) Bytes() []byte {
	bytes, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return bytes
}

func LoadEntry(data []byte) Entry {
	var entry Entry
	err := json.Unmarshal(data, &entry)
	if err != nil {
		panic(err)
	}
	return entry
}

// Equal checks if two Entry structs are equal
func (e Entry) Equal(other Entry) bool {
	if e.Term != other.Term {
		return false
	}
	if len(e.Cmd) != len(other.Cmd) {
		return false
	}
	for i, v := range e.Cmd {
		if v != other.Cmd[i] {
			return false
		}
	}
	return true
}

type Envelope struct {
	OperationType OperationType `json:"operationType"`
	Payload       []byte        `json:"payload"`
}

func (e *Envelope) Bytes() []byte {
	bytes, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return bytes
}

type OperationType int

const (
	VoteRequestOp OperationType = iota
	VoteResponseOp
	AppendEntriesRequestOp
	AppendEntriesResponseOp
	InstallSnapshotRequestOp
	InstallSnapshotResponseOp
	ProposalOp
)

func (ot OperationType) String() string {
	switch ot {
	case VoteRequestOp:
		return "VoteRequest"
	case VoteResponseOp:
		return "VoteResponse"
	case AppendEntriesRequestOp:
		return "AppendEntriesRequest"
	case AppendEntriesResponseOp:
		return "AppendEntriesResponse"
	case InstallSnapshotRequestOp:
		return "InstallSnapshotRequest"
	case InstallSnapshotResponseOp:
		return "InstallSnapshotResponse"
	case ProposalOp:
		return "Proposal"
	default:
		return "unknown operation type"
	}
}

type Proposal struct {
	Data []byte
}

func (p *Proposal) Bytes() []byte {
	bytes, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (p *Proposal) OpType() OperationType {
	return ProposalOp
}

type InstallSnapshotRequest struct {
	LeaderId string `json:"leader_id,omitempty"`
	Term     uint64 `json:"term,omitempty"`
	// snapshotCommitIdx + len(commitEntries) = leaderCommitIdx
	SnapshotCommitIdx         uint64 `json:"snapshot_commit_idx,omitempty"`
	LastIncludedSnapshotEntry Entry  `json:"last_included_snapshot_entry,omitempty"`

	// snapshot content
	Data []byte `json:"data,omitempty"`
	// extra entries that aren't included in the snapshot (snapshotIdx+1 -> commitIdx)
	CommittedEntries []Entry `json:"committed_entries,omitempty"`
}

func (isr *InstallSnapshotRequest) Bytes() []byte {
	bytes, err := json.Marshal(isr)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (isr *InstallSnapshotRequest) OpType() OperationType {
	return InstallSnapshotRequestOp
}

func LoadInstallSnapshotRequest(bytes []byte) InstallSnapshotRequest {
	var req InstallSnapshotRequest
	err := json.Unmarshal(bytes, &req)
	if err != nil {
		panic(err)
	}
	return req
}

type InstallSnapshotResponse struct {
	ResponderId string `json:"responder_id,omitempty"`

	CommitIdx uint64 `json:"commit_idx,omitempty"`
}

func (isr *InstallSnapshotResponse) Bytes() []byte {
	bytes, err := json.Marshal(isr)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (isr *InstallSnapshotResponse) OpType() OperationType {
	return InstallSnapshotResponseOp
}

func LoadInstallSnapshotResponse(bytes []byte) InstallSnapshotResponse {
	var resp InstallSnapshotResponse
	err := json.Unmarshal(bytes, &resp)
	if err != nil {
		panic(err)
	}
	return resp
}

type AppendEntriesRequest struct {
	Term            uint64  `json:"term"`
	LeaderId        string  `json:"leader_id"`
	Entries         []Entry `json:"entries"`
	PrevLogIdx      uint64  `json:"prev_log_idx"`
	PrevLogTerm     uint64  `json:"prev_log_term"`
	LeaderCommitIdx uint64  `json:"leader_commit_idx"`
	RequestId       string  `json:"request_id"`
}

func (aer *AppendEntriesRequest) Bytes() []byte {
	bytes, err := json.Marshal(aer)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (aer *AppendEntriesRequest) OpType() OperationType {
	return AppendEntriesRequestOp
}

func LoadAppendEntriesRequest(bytes []byte) AppendEntriesRequest {
	var req AppendEntriesRequest
	err := json.Unmarshal(bytes, &req)
	if err != nil {
		panic(err)
	}
	return req
}

type AppendEntriesResponse struct {
	Term        uint64 `json:"term"`
	Success     bool   `json:"success"`
	ResponderId string `json:"responder_id"`
	MatchIndex  uint64 `json:"match_index"`  // entries replicated on responder
	CommitIndex uint64 `json:"commit_index"` // set only for unsuccessful ae reqs because the follower is more ahead than the leader thinks
	RequestId   string `json:"request_id"`   // ID of the request this is a response to
}

func (aer *AppendEntriesResponse) Bytes() []byte {
	bytes, err := json.Marshal(aer)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (aer *AppendEntriesResponse) OpType() OperationType {
	return AppendEntriesResponseOp
}

func LoadAppendEntriesResponse(bytes []byte) AppendEntriesResponse {
	var resp AppendEntriesResponse
	err := json.Unmarshal(bytes, &resp)
	if err != nil {
		panic(err)
	}
	return resp
}

type VoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateId  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

func (req *VoteRequest) Bytes() []byte {
	bytes, err := json.Marshal(req)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (req *VoteRequest) OpType() OperationType {
	return VoteRequestOp
}

func LoadVoteRequest(bytes []byte) VoteRequest {
	var req VoteRequest
	err := json.Unmarshal(bytes, &req)
	if err != nil {
		panic(err)
	}
	return req
}

type VoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	VoterId     string `json:"voter_id"`
}

func (resp *VoteResponse) Bytes() []byte {
	bytes, err := json.Marshal(resp)
	if err != nil {
		panic(err)
	}
	return bytes
}

func (resp *VoteResponse) OpType() OperationType {
	return VoteResponseOp
}

func LoadVoteResponse(bytes []byte) VoteResponse {
	var resp VoteResponse
	err := json.Unmarshal(bytes, &resp)
	if err != nil {
		panic(err)
	}
	return resp
}

type FollowerState struct {
	// index of next log entry to send to each peer
	nextIndex uint64
	// index of highest log entry known to be replicated on server
	matchIndex uint64
	// timestamp of AppendEntries request that was last sent to the follower
	aeTimestamp time.Time
	// Last request sent, if still unanswered
	pendingRequest *AppendEntriesRequest
}
