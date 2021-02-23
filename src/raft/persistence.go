package raft

import (
	"bytes"
	"log"
)
import "labgob"

//
// struct for persistent states of a raft instance
// when saving raft's state, encode these attributes via gob
// when reading raft's state, decode these attributes via gob
//
type PersistentState struct {
	CurrentTerm int
	VoteFor     int
	Logs        []Log
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var ps PersistentState
	err := decoder.Decode(&ps)
	rf.LogReadPersistState(&ps)
	if err != nil {
		log.Fatal("decode error:", err)
	}

	PrintLock("=================================[Server%d] readPersist Lock=================================\n", rf.me)
	rf.mu.Lock()

	// persistent state on all servers
	rf.currentTerm = ps.CurrentTerm
	rf.voteFor = ps.VoteFor
	rf.logs = ps.Logs

	// volatile state on all servers
	rf.commitIndex = 0 // initialized to 0, increases monotonically
	rf.lastApplied = 0 // initialized to 0, increases monotonically

	// volatile state on leaders (must be reinitialized after election)
	rf.nextIndex = make([]int, len(rf.peers)) // don't need to initialize now since I'm not a leader on first boot
	for idx, _ := range rf.nextIndex {
		rf.nextIndex[idx] = 1 // log starts with 1, so the nextIndex should be 1 (every raft peer has the same log[0]!)
	}
	rf.matchIndex = make([]int, len(rf.peers)) // don't need to initialize now since I'm not a leader on first boot
	PrintLock("=================================[Server%d] readPersist Unlock=================================\n", rf.me)
	rf.mu.Unlock()
}
