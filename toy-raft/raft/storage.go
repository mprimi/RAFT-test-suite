package raft

type Storage interface {
	GetCurrentTerm() uint64
	// zeroes votedFor as a side effect
	IncrementTerm() uint64
	// zeroes votedFor as a side effect
	SetTerm(uint64)
	GetVotedFor() string
	Voted() bool
	VoteFor(id string, currentTerm uint64)

	// Writes
	AppendEntry(entry Entry) error
	PrependEntry(entry Entry) error
	DeleteEntriesFrom(index uint64)
	DeleteEntriesUpTo(index uint64)

	// Mutations
	AdjustOffset(offsetIndex uint64) // sets first log index

	// Reads
	GetFirstLogIndex() uint64
	GetLastLogIndex() uint64
	GetLastLogIndexAndTerm() (index uint64, term uint64)
	GetLogEntriesFrom(index uint64) []Entry
	TestGetLogEntries() ([]*Entry, uint64)
	GetLogEntry(index uint64) (Entry, bool)
	Close() error
}
