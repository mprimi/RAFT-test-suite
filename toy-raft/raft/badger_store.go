package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"path/filepath"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/dgraph-io/badger/v4"
)

var (
	TermKey = []byte("term")
	VoteKey = []byte("vote")
	// TODO: load these once on startup, keep flushing values to disk on log writes
	LastLogIdxKey  = []byte("lastLogIdx")
	FirstLogIdxKey = []byte("firstLogIdx")
)

type BadgerStorage struct {
	db *badger.DB
}

type logEntry struct {
	Data []byte
	Term uint64
}

func NewDiskStorage(replicaId string, baseDir string) *BadgerStorage {
	dbPath := filepath.Join(baseDir, replicaId)
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		panic(fmt.Errorf("failed to initialize database at %s: %w", dbPath, err))
	}

	store := &BadgerStorage{
		db: db,
	}

	// if term is found, this is an already existing db
	var keyExists bool
	err = db.View(func(txn *badger.Txn) error {
		_, err = txn.Get(TermKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// key doesn't exist
		} else if err != nil {
			return err
		} else {
			keyExists = true
		}
		return nil
	})
	if err != nil {
		panic(fmt.Errorf("failed to check term during init: %w", err))
	}
	if !keyExists {
		store.storageInit()
	}

	return store
}

func NewInMemoryStorage() *BadgerStorage {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		panic(fmt.Errorf("failed to init memory store: %w", err))
	}

	store := &BadgerStorage{
		db: db,
	}
	store.storageInit()

	return store
}

func (store *BadgerStorage) Close() error {
	return store.db.Close()
}

func (store *BadgerStorage) storageInit() {
	// initialize term
	store.SetTerm(0)

	// initialize log
	store.setLastLogIdx(0)
	store.setFirstLogIdx(1)
}

func (store *BadgerStorage) setLastLogIdx(newLastLogIdx uint64) {

	currentLastLogIdx := store.GetLastLogIndex()
	if currentLastLogIdx == 0 && newLastLogIdx == 0 {
		// initial case, don't panic
	} else if newLastLogIdx == currentLastLogIdx {
		assert.Unreachable(
			"Setting invalid lastLogIndex",
			map[string]any{
				"newLastLogIdx":     newLastLogIdx,
				"currentLastLogIdx": currentLastLogIdx,
			},
		)
		panic(fmt.Errorf("setting invalid last log index"))
	}

	if firstLogIdx := store.GetFirstLogIndex(); firstLogIdx > newLastLogIdx+1 {
		assert.Unreachable("Setting lastLogIndex below trim threshold (firstLogIndex)", map[string]any{
			"newLastLogIdx":     newLastLogIdx,
			"currentLastLogIdx": currentLastLogIdx,
			"firstLogIndex":     firstLogIdx,
		})
		panic(fmt.Errorf("setting invalid last log index"))
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, newLastLogIdx)

	if err := store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(LastLogIdxKey, buf); err != nil {
			return fmt.Errorf("failed to set lastLogIdx: %s", err)
		}
		return nil
	}); err != nil {
		panic(fmt.Errorf("failed to commit last log index: %w", err))
	}
}

func (store *BadgerStorage) setFirstLogIdx(newFirstLogIdx uint64) {

	currentFirstLogIdx := store.GetFirstLogIndex()
	if currentFirstLogIdx == 0 && newFirstLogIdx == 0 {
		// initial case, don't panic
	} else if newFirstLogIdx == currentFirstLogIdx {
		assert.Unreachable(
			"Setting invalid firstLogIndex",
			map[string]any{
				"newFirstLogIdx":     newFirstLogIdx,
				"currentFirstLogIdx": currentFirstLogIdx,
			},
		)
		panic(fmt.Errorf("setting invalid first log index"))
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, newFirstLogIdx)

	if err := store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(FirstLogIdxKey, buf); err != nil {
			return fmt.Errorf("failed to set firstLogIdx: %s", err)
		}
		return nil
	}); err != nil {
		panic(fmt.Errorf("failed to commit first log index: %w", err))
	}
}

func (store *BadgerStorage) GetLogEntry(idx uint64) (Entry, bool) {
	lastLogIdx := store.GetLastLogIndex()
	firstLogIdx := store.GetFirstLogIndex()
	if idx == 0 {
		assert.Unreachable(
			"Invalid entry lookup index",
			map[string]any{
				"index":      idx,
				"lastLogIdx": lastLogIdx,
			},
		)
		panic(fmt.Errorf("invalid entry lookup index"))
	}

	if idx > lastLogIdx {
		return Entry{}, false
	} else if idx < firstLogIdx {
		assert.Unreachable(
			"Entry below trim threshold",
			map[string]any{
				"index":       idx,
				"lastLogIdx":  lastLogIdx,
				"firstLogIdx": firstLogIdx,
			},
		)
		panic("attempted to look up entry below trim threshold")
	}

	var entry Entry
	err := store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(store.idxToKey(idx))
		if err != nil {
			return err
		}

		if err := item.Value(func(val []byte) error {
			entry = LoadEntry(val)
			return nil
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		assert.Unreachable(
			"Failed to load entry",
			map[string]any{
				"index":       idx,
				"lastLogIdx":  lastLogIdx,
				"firstLogIdx": firstLogIdx,
			},
		)
		panic(fmt.Errorf("failed to load entry: %w", err))
	}

	return entry, true
}

func (store *BadgerStorage) TestGetLogEntries() ([]*Entry, uint64) {
	lastLogIdx := store.GetLastLogIndex()
	firstLogIdx := store.GetFirstLogIndex()
	entries := make([]*Entry, 0, lastLogIdx)
	err := store.db.View(func(txn *badger.Txn) error {
		for idx := firstLogIdx; idx <= lastLogIdx; idx++ {

			item, err := txn.Get(store.idxToKey(idx))
			if err != nil {
				return fmt.Errorf("failed to get store entry at index %d: %w", idx, err)
			}

			if err := item.Value(func(val []byte) error {
				x := LoadEntry(val)
				entries = append(entries, &x)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return entries, firstLogIdx - 1
}

// DeleteEntriesUpTo and including endlingLogIndex
func (store *BadgerStorage) DeleteEntriesUpTo(endingLogIndex uint64) {
	firstLogIndex := store.GetFirstLogIndex()
	lastLogIndex := store.GetLastLogIndex()

	if endingLogIndex == 0 {
		assert.Unreachable("Trimming up to 0", map[string]any{})
		panic(fmt.Errorf("trimming log up to index 0"))
	}

	if endingLogIndex < firstLogIndex || endingLogIndex > lastLogIndex {
		assert.Unreachable("attempted to trim outside log range", map[string]any{
			"firstLogIndex":  firstLogIndex,
			"lastLogIndex":   lastLogIndex,
			"endingLogIndex": endingLogIndex,
		})
		log.Printf("attempted to trim outside log range, firstLogIndex: %d, lastLogIndex: %d, endingLogIndex: %d", firstLogIndex, lastLogIndex, endingLogIndex)
		panic(fmt.Errorf("attempted to trim outside log range"))
	}

	err := store.db.Update(func(txn *badger.Txn) error {
		for idx := firstLogIndex; idx <= endingLogIndex; idx++ {
			err := txn.Delete(store.idxToKey(idx))
			if err != nil {
				return fmt.Errorf("failed to delete key at index %d: %w", idx, err)
			}
		}
		return nil
	})
	if err != nil {
		assert.Unreachable("failed to trim log", map[string]any{
			"firstLogIndex":  firstLogIndex,
			"lastLogIndex":   lastLogIndex,
			"endingLogIndex": endingLogIndex,
			"error":          err,
		})
		panic(fmt.Errorf("failed to trim log"))
	}

	store.setFirstLogIdx(endingLogIndex + 1)
}

func (store *BadgerStorage) DeleteEntriesFrom(startingLogIdx uint64) {
	lastLogIdx := store.GetLastLogIndex()
	firstLogIdx := store.GetFirstLogIndex()
	if startingLogIdx == 0 {
		assert.Unreachable(
			"Invalid delete start index",
			map[string]any{
				"startingLogIdx": startingLogIdx,
				"lastLogIdx":     lastLogIdx,
			},
		)
		panic(fmt.Errorf("invalid delete start index"))
	} else if startingLogIdx < firstLogIdx {
		assert.Unreachable(
			"Delete below trim threshold",
			map[string]any{
				"startingLogIdx": startingLogIdx,
				"lastLogIdx":     lastLogIdx,
				"firstLogIdx":    firstLogIdx,
			},
		)
		panic(fmt.Errorf("attempted to delete below trim threshold"))
	}

	err := store.db.Update(func(txn *badger.Txn) error {
		for idx := startingLogIdx; idx <= lastLogIdx; idx++ {
			err := txn.Delete(store.idxToKey(idx))
			if err != nil {
				return fmt.Errorf("failed to delete key at index %d: %w", idx, err)
			}
		}
		return nil
	})
	if err != nil {
		assert.Unreachable(
			"Invalid delete entries",
			map[string]any{
				"startingLogIdx": startingLogIdx,
				"lastLogIdx":     lastLogIdx,
				"firstLogIdx":    firstLogIdx,
			},
		)
		panic(fmt.Errorf("failed to delete: %w", err))
	}
	store.setLastLogIdx(startingLogIdx - 1)
}

func (store *BadgerStorage) GetLastLogIndexAndTerm() (lastLogIndex uint64, term uint64) {
	lastLogIndex = store.GetLastLogIndex()
	if lastLogIndex == 0 {
		term = 0
		return
	}
	entry, exists := store.GetLogEntry(lastLogIndex)
	if !exists {
		assert.Unreachable(
			"Non-existent entry term lookup",
			map[string]any{
				"lastLogIndex": lastLogIndex,
			},
		)
		panic(fmt.Errorf("expected existing entry not found"))
	}
	term = entry.Term
	return
}

func (store *BadgerStorage) GetFirstLogIndex() uint64 {
	var firstLogIdx uint64
	store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(FirstLogIdxKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// key doesn't exist yet
			return nil
		}

		if err != nil {
			return fmt.Errorf("failed to get firstLogIdx: %w", err)
		}

		err = item.Value(func(val []byte) error {
			firstLogIdx = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read value of firstLogIdx item: %w", err)
		}
		return nil
	})
	return firstLogIdx
}

func (store *BadgerStorage) GetLastLogIndex() uint64 {
	var lastLogIdx uint64
	store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(LastLogIdxKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// key doesn't exist yet
			return nil
		}

		if err != nil {
			return fmt.Errorf("failed to get lastLogIdx: %w", err)
		}

		err = item.Value(func(val []byte) error {
			lastLogIdx = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read value of lastLogIdx item: %w", err)
		}
		return nil
	})
	return lastLogIdx
}

func (store *BadgerStorage) idxToKey(idx uint64) []byte {
	return []byte(fmt.Sprintf("%d", idx))
}

// TODO should return index so caller can check it matches expected (alternatively, pass in the expected index)
func (store *BadgerStorage) AppendEntry(entry Entry) error {
	// guard: check for valid entry
	if entry.Term == 0 {
		assert.Unreachable("entry has term 0", nil)
		panic("entry has term 0")
	}

	if store.GetCurrentTerm() == 0 {
		assert.Unreachable(
			"Append entry with zero term",
			map[string]any{
				"entry":       entry.Term,
				"currentTerm": 0,
			},
		)
		panic("append entry with zero term")
	}

	lastLogIdx := store.GetLastLogIndex()
	entryIdx := lastLogIdx + 1

	key := store.idxToKey(entryIdx)
	value := entry.Bytes()

	// put in log
	if err := store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(key, value); err != nil {
			return fmt.Errorf("failed to append entry at index %d: %w", entryIdx, err)
		}
		return nil
	}); err != nil {
		panic(fmt.Errorf("failed to append entry: %w", err))
	}

	// update lastLogIdx
	store.setLastLogIdx(entryIdx)

	return nil
}

func (store *BadgerStorage) PrependEntry(entry Entry) error {
	// guard: check for valid entry
	if entry.Term == 0 {
		assert.Unreachable("entry has term 0", nil)
		panic("entry has term 0")
	}

	// guard: firstLogIdx > 1
	firstLogIndex := store.GetFirstLogIndex()
	if firstLogIndex <= 1 {
		assert.Unreachable("no room to prepend", map[string]any{
			"firstLogIdx": firstLogIndex,
			"lastLogIdx":  store.GetLastLogIndex(),
		})
		panic("no room to prepend")
	}

	entryIdx := firstLogIndex - 1

	key := store.idxToKey(entryIdx)
	value := entry.Bytes()

	// put in log
	if err := store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(key, value); err != nil {
			return fmt.Errorf("failed to prepend entry at index %d: %w", entryIdx, err)
		}
		return nil
	}); err != nil {
		panic(fmt.Errorf("failed to prepend entry: %w", err))
	}

	// update trim threshold
	store.setFirstLogIdx(entryIdx)

	return nil
}

func (store *BadgerStorage) VoteFor(id string, currentTerm uint64) {
	storedCurrentTerm := store.GetCurrentTerm()
	if storedCurrentTerm != currentTerm {
		assert.Unreachable(
			"Vote commit term mismatch",
			map[string]any{
				"currentTerm": currentTerm,
				"storedTerm":  storedCurrentTerm,
				"id":          id,
			},
		)
		panic(fmt.Errorf("unexpected term during vote commit"))
	}

	storedVote := store.GetVotedFor()
	if storedVote != "" {
		assert.Unreachable(
			"Already voted for different node in this term",
			map[string]any{
				"currentTerm": currentTerm,
				"storedVote":  storedVote,
				"id":          id,
			},
		)
		panic(fmt.Errorf("already voted in this term"))
	}

	if err := store.db.Update(func(txn *badger.Txn) error {
		return txn.Set(VoteKey, []byte(id))
	}); err != nil {
		panic(fmt.Errorf("failed to update vote: %w", err))
	}
}

func (store *BadgerStorage) GetLogEntriesFrom(startingLogIdx uint64) []Entry {
	lastLogIdx := store.GetLastLogIndex()
	firstLogIdx := store.GetFirstLogIndex()

	if startingLogIdx < firstLogIdx {
		assert.Unreachable("entries below trim threshold", map[string]any{
			"startingLogIdx": startingLogIdx,
			"firstLogIdx":    firstLogIdx,
			"lastLogIdx":     lastLogIdx,
		})
		panic(fmt.Errorf("attempted to load entries below trim threshold"))
	}

	entries := make([]Entry, 0, lastLogIdx-startingLogIdx+1)
	err := store.db.View(func(txn *badger.Txn) error {
		for idx := startingLogIdx; idx <= lastLogIdx; idx++ {

			item, err := txn.Get(store.idxToKey(idx))
			if err != nil {
				return fmt.Errorf("failed to get store entry at index %d: %w", idx, err)
			}

			if err := item.Value(func(val []byte) error {
				entries = append(entries, LoadEntry(val))
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(fmt.Errorf("failed to get entries: %w", err))
	}
	return entries
}

func (store *BadgerStorage) GetVotedFor() string {
	votedFor := ""
	if err := store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(VoteKey)
		if err != nil {
			return err
		}
		item.Value(func(val []byte) error {
			votedFor = string(val)
			return nil
		})
		return nil
	}); err != nil {
		panic(fmt.Errorf("failed to load vote: %w", err))
	}

	return votedFor
}

func (store *BadgerStorage) Voted() bool {
	return store.GetVotedFor() != ""
}

// clears vote
func (store *BadgerStorage) SetTerm(term uint64) {
	currentTerm := store.GetCurrentTerm()
	if currentTerm == 0 && term == 0 {
		// initial case, don't panic
	} else if term <= currentTerm {
		assert.Unreachable(
			"Attempting to decrease term",
			map[string]any{
				"currentTerm": currentTerm,
				"newTerm":     term,
			},
		)
		panic(fmt.Errorf("attempting to decrease term"))
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, term)

	if err := store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(TermKey, buf); err != nil {
			return fmt.Errorf("failed to set term: %w", err)
		}
		if err := txn.Set(VoteKey, []byte("")); err != nil {
			return fmt.Errorf("failed to clear vote: %w", err)
		}
		return nil
	}); err != nil {
		panic(fmt.Errorf("failed to commit new term and clear vote: %w", err))
	}
}

func (store *BadgerStorage) GetCurrentTerm() uint64 {
	var term uint64
	store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(TermKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			// key doesn't exist yet
			return nil
		}

		if err != nil {
			return fmt.Errorf("failed to get term: %w", err)
		}

		err = item.Value(func(val []byte) error {
			term = binary.BigEndian.Uint64(val)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to read value of term item: %w", err)
		}
		return nil
	})
	return term
}

func (store *BadgerStorage) IncrementTerm() uint64 {
	newTerm := store.GetCurrentTerm() + 1
	store.SetTerm(newTerm)
	return newTerm
}

func (store *BadgerStorage) AdjustOffset(offsetIndex uint64) {
	lastLogIdx := store.GetLastLogIndex()
	firstLogIdx := store.GetFirstLogIndex()

	// guard: only should be invoked if log is empty
	if firstLogIdx == 1 && lastLogIdx == 0 {
		// log is empty
	} else if lastLogIdx-firstLogIdx > 0 {
		// log has no entries
	} else {
		assert.Unreachable(
			"Attempting to adjust offset on non-empty log",
			map[string]any{
				"firstLogIdx":        firstLogIdx,
				"lastLogIdx":         lastLogIdx,
				"attemptedOffsetIdx": offsetIndex,
			},
		)
		panic(fmt.Errorf("attempting to adjust offset on non-empty log"))
	}

	store.setFirstLogIdx(offsetIndex)
	store.setLastLogIdx(offsetIndex - 1)
}
