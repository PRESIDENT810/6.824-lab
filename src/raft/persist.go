package raft

import (
	"bytes"
	"labgob"
	"log"
)

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
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//f
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	PrintLock("=================================[Server%d] Persist Lock=================================\n", rf.me)
	rf.mu.Lock()
	ps := PersistentState{rf.currentTerm, rf.voteFor, rf.logs}
	PrintLock("=================================[Server%d] Persist Unlock=================================\n", rf.me)
	rf.mu.Unlock()
	rf.LogPersistState(&ps)
	err := encoder.Encode(ps)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
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

//
// find the last index of the given term in a raft instance's log
// don't use lock here or everything will be fucked up
//
func (rf *Raft) findLastIndex(term int) int {
	for i := len(rf.logs) - 1; i >= 0; i-- {
		// sometimes I don't have log in this term, for example:
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:   1 1 1 2 2 4 4 4
		// FOLLOWER: 1 1 1 3 3 3
		// with prevLogIndex=5, so he don't have log entry with term 4, and he replies me with term 3,
		// which means our log can only match with term at most 3
		// but I don't have log entries with term 3, so I have to find a entry with term smaller than or equal to 3
		if rf.logs[i].Term <= term {
			return i
		}
	}
	//panic("findLastIndex fucked up!!!")
	return -1 // if we return -1, then this is really fucked up
}

//
// find the first index of the given term in a raft instance's log
// don't use lock here or everything will be fucked up
//
func (rf *Raft) findFirstIndex(term int) int {
	for i := 0; i < len(rf.logs); i++ {
		if rf.logs[i].Term == term {
			return i
		}
	}
	//panic("findFirstIndex fucked up!!!")
	return -1 // if we return -1, then this is really fucked up
}
