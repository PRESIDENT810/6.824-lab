package raft

import (
	"fmt"
	"sync"
)

var logMutex sync.Mutex

//
// log server info
//
func (rf *Raft) LogServerStates() {
	fmt.Printf("=======================================================================================================\n")
	fmt.Printf("[server%d] reports\nCurrentTerm: %d, voteFor: %d, commitIndex: %d, lastApplied: %d, role: %d, electionExpired: %t\n",
		rf.me, rf.currentTerm, rf.voteFor, rf.commitIndex, rf.lastApplied, rf.role, rf.electionExpired)
	fmt.Printf("My log entries are: %v\n", rf.logs)
	fmt.Printf("Volatile state on leaders:\n")
	fmt.Printf("nextIndex: ")
	for _, i := range rf.nextIndex {
		fmt.Printf("%d ", i)
	}
	fmt.Printf("\nmatchIndex: ")
	for _, i := range rf.matchIndex {
		fmt.Printf("%d ", i)
	}
	fmt.Printf("\n")
	fmt.Printf("=======================================================================================================\n\n")
}

//
// log AppendEntries RPC info on receiver side
//
func (rf *Raft) LogAppendEntriesReceive(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	fmt.Printf("[server%d] receives AppendEntries RPC\nArguments:\nterm: %d, leaderId: %d, prevLogIndex: %d, prevLogTerm: %d, entry: %v, leaderCommit: %d\n\n\n",
		rf.me, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	rf.LogServerStates()
}

//
// log RequestVote RPC info on sender side
//
func (rf *Raft) LogRequestVoteReceive(args *RequestVoteArgs, reply *RequestVoteReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	fmt.Printf("[server%d] receives RequestVote RPC\nArguments:\nterm: %d, candidateId: %d, lastLogIndex: %d, lastLogTerm: %d\n\n\n",
		rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	rf.LogServerStates()
}

//
// log AppendEntries RPC info on sender side
//
func (rf *Raft) LogAppendEntriesSend(sender, receiver int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	fmt.Printf("[server%d] sends AppendEntries RPC to %d\nArguments:\nterm: %d, leaderId: %d, prevLogIndex: %d, prevLogTerm: %d, entry: %v, leaderCommit: %d\nReply:\nterm: %d, success: %t\n\n\n",
		sender, receiver, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, reply.Term, reply.Success)
	rf.LogServerStates()
}

//
// log RequestVote RPC info on sender side
//
func (rf *Raft) LogRequestVoteSend(sender, receiver int, args *RequestVoteArgs, reply *RequestVoteReply) {
	logMutex.Lock()
	defer logMutex.Unlock()
	fmt.Printf("[server%d] sends RequestVote RPC to %d\nArguments:\nterm: %d, candidateId: %d, lastLogIndex: %d, lastLogTerm: %d\nReply:\nterm: %d, voteGranted: %t\n\n\n",
		sender, receiver, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, reply.Term, reply.VoteGranted)
	rf.LogServerStates()
}
