package raft

import (
	"bytes"
	"log"
)
import "labgob"

// PersistentState
//
// struct for persistent states of a raft instance
// when saving raft's state, encode these attributes via gob
// when reading raft's state, decode these attributes via gob
//
type PersistentState struct {
	CurrentTerm int
	VoteFor     int
	Logs        []Log

	LastIncludeTerm   int
	LastIncludedIndex int

	Snapshot []byte
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(snapshot []byte) {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	logs := make([]Log, len(rf.logs))
	copy(logs, rf.logs)
	ps := PersistentState{
		rf.currentTerm,
		rf.voteFor,
		logs,
		rf.lastIncludeTerm,
		rf.lastIncludedIndex,
		snapshot,
	}
	err := encoder.Encode(ps)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	data := buffer.Bytes()
	rf.LogPersist(ps)
	// Never use an empty snapshot to overwrite a valid snapshot
	if snapshot == nil {
		rf.persister.SaveRaftState(data)
	} else {
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var ps PersistentState
	err := decoder.Decode(&ps)
	if err != nil {
		log.Fatal("decode error:", err)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.LogReadPersist(ps)

	// persistent state on all servers
	rf.currentTerm = ps.CurrentTerm
	rf.voteFor = ps.VoteFor
	rf.logs = ps.Logs

	// persistent state for snapshot
	rf.lastIncludeTerm = ps.LastIncludeTerm
	rf.lastIncludedIndex = ps.LastIncludedIndex

	rf.snapshot = ps.Snapshot

	// volatile state on all servers
	if rf.lastIncludedIndex == -1 {
		rf.commitIndex = 0 // initialized to 0, increases monotonically
		rf.lastApplied = 0 // initialized to 0, increases monotonically
	} else {
		rf.commitIndex = rf.lastIncludedIndex // any log in snapshot should be committed and applied
		rf.lastApplied = rf.lastIncludedIndex // any log in snapshot should be committed and applied
	}

	// volatile state on leaders (must be reinitialized after election)
	rf.nextIndex = make([]int, len(rf.peers)) // don't need to initialize now since I'm not a leader on first boot
	for idx, _ := range rf.nextIndex {
		rf.nextIndex[idx] = 1 // log starts with 1, so the nextIndex should be 1 (every raft peer has the same log[0]!)
	}
	rf.matchIndex = make([]int, len(rf.peers)) // don't need to initialize now since I'm not a leader on first boot
}
