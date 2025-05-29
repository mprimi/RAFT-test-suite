# Snapshots

leader sends follower AE req
follower responds `success:false`
leader gets AE resp, finds that `nextIndex` is below trim threshold (meaning, follower needs a snapshot)
leader sends follower a `NeedsSnapshotReq{leaderCommit}`
follower checks to see if leaderCommit is greater than its own, enters *recovery* mode if behind
follower broadcasts SnapshotRequestRPC
leader streams snapshot + committed entries to follower 
follower applies snapshots and adds entries, exits recovery mode (if timeout, retry `NeedsSnapshotReq...`)

recovery mode:
reset election timer (will be refreshed as leader sends chunks)
should still listen and respond to vote requests
start ignoring AE reqs

SnapshotRequestRPC:
open a private inbox
`SnapshotReq{minCommit=leaderCommit}`

Open Questions
---
How does the leader know what the `matchIdx` is, for a node that was recently caught up

Hacky POC
---
assumptions:
- all nodes are running on the same disk
    - this won't work on AT, but it'll stop the leader from panicking
or
- snapshot fits in a single NATS message
    - works on AT, our workloads don't propose large msgs

when leader detects node is behind, send `InstallSnapshotRequest`

```go
type InstallSnapshotRequest struct {
    Term uint64 
    // snapshotCommitIdx + len(commitEntries) = leaderCommitIdx
    SnapshotCommitIdx uint64

    // snapshot content
    Data []byte
    // extra entries that aren't included in the snapshot (snapshotIdx+1 -> commitIdx)
    CommittedEntries [][]byte
}
```

receiver:
look at term
if snapshotCommitIdx > commitIdx:
    install the snapshot (after `StateMachine.wipe()`)

for i := commitIdx; i =< leaderCommitIdx; i++:
    commit_and_apply(entry[i])

```go
type InstallSnapshotResponse struct {
    ResponderId string

    CommitIdx uint64
}
```

