package raft

import (
	"fmt"
	"sync"
)

var logMutex sync.Mutex

// whether show corresponding log
const (
	debug       = false
	showLog     = false
	showLock    = false
	showPersist = false
)

// wrapper of fmt.Printf, if showLog is false, then don't print log about states
func Printf(format string, a ...interface{}) {
	if showLog {
		fmt.Printf(format, a...)
	}
}

// wrapper of fmt.Printf, if showLock is false, then don't print log about locks
func PrintLock(format string, a ...interface{}) {
	if showLock {
		fmt.Printf(format, a...)
	}
}

// wrapper of fmt.Printf, if showPersist is false, then don't print log about persister
func PrintPersist(format string, a ...interface{}) {
	if showPersist {
		fmt.Printf(format, a...)
	}
}

//
// log server info
//
func (rf *Raft) LogServerStates() {
	Printf("=======================================================================================================\n")
	Printf("[server%d] reports\nCurrentTerm: %d, voteFor: %d, commitIndex: %d, lastApplied: %d, role: %d, electionExpired: %t\n",
		rf.me, rf.currentTerm, rf.voteFor, rf.commitIndex, rf.lastApplied, rf.role, rf.electionExpired)
	Printf("My log entries are: %v\n", rf.logs)
	Printf("Volatile state on leaders:\n")
	Printf("nextIndex: ")
	for _, i := range rf.nextIndex {
		Printf("%d ", i)
	}
	Printf("\nmatchIndex: ")
	for _, i := range rf.matchIndex {
		Printf("%d ", i)
	}
	Printf("\n")
	Printf("=======================================================================================================\n\n")
}

//
// log AppendEntries RPC info on receiver side
//
func (rf *Raft) LogAppendEntriesReceive(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	Printf("[server%d] receives [AppendEntries RPC %d]\nArguments:\nterm: %d, leaderId: %d, prevLogIndex: %d, prevLogTerm: %d, entry: %v, leaderCommit: %d\n\n\n",
		rf.me, args.RPCID, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	rf.LogServerStates()
}

//
// log RequestVote RPC info on sender side
//
func (rf *Raft) LogRequestVoteReceive(args *RequestVoteArgs, reply *RequestVoteReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	Printf("[server%d] receives [RequestVote RPC %d]\nArguments:\nterm: %d, candidateId: %d, lastLogIndex: %d, lastLogTerm: %d\n\n\n",
		rf.me, args.RPCID, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	rf.LogServerStates()
}

//
// log AppendEntries RPC info on sender side
//
func (rf *Raft) LogAppendEntriesSend(sender, receiver int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	Printf("[server%d] sends [AppendEntries RPC %d] to [server%d]\nArguments:\nterm: %d, leaderId: %d, prevLogIndex: %d, prevLogTerm: %d, entry: %v, leaderCommit: %d\nReply:\nterm: %d, success: %t, conflictTerm: %d, TryNextIndex: %d\n\n\n",
		sender, args.RPCID, receiver, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, reply.Term, reply.Success, reply.ConflictTerm, reply.TryNextIndex)
	rf.LogServerStates()
}

//
// log RequestVote RPC info on sender side
//
func (rf *Raft) LogRequestVoteSend(sender, receiver int, args *RequestVoteArgs, reply *RequestVoteReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	Printf("[server%d] sends [RequestVote RPC %d] to [server%d]\nArguments:\nterm: %d, candidateId: %d, lastLogIndex: %d, lastLogTerm: %d\nReply:\nterm: %d, voteGranted: %t\n\n\n",
		sender, args.RPCID, receiver, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, reply.Term, reply.VoteGranted)
	rf.LogServerStates()
}

//
// log ReadPersist
//
func (rf *Raft) LogReadPersistState(ps *PersistentState) {
	logMutex.Lock()
	defer logMutex.Unlock()
	PrintPersist("[server%d] calls ReadPersist from persister\ncurrentTerm: %d, voteFor: %d, logs: %v\n\n\n", rf.me, ps.CurrentTerm, ps.VoteFor, ps.Logs)
}

//
// log Persist
//
func (rf *Raft) LogPersistState(ps *PersistentState) {
	logMutex.Lock()
	defer logMutex.Unlock()
	PrintPersist("[server%d] calls Persist to persister\ncurrentTerm: %d, voteFor: %d, logs: %v\n\n\n", rf.me, ps.CurrentTerm, ps.VoteFor, ps.Logs)
}
