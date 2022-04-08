package raft

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
)

var LogMutex sync.Mutex

type LogConfig struct {
	EnablePrintf bool
	// Log config for callers
	EnableLogSendAppendEntries   bool
	EnableLogSendRequestVote     bool
	EnableLogSendInstallSnapshot bool
	// Log config for handlers
	EnableLogAppendEntries   bool
	EnableLogRequestVote     bool
	EnableLogInstallSnapshot bool
	// Log config for persistence
	EnableLogPersist bool
	// Log config for snapshot
	EnableLogSnapshot bool
	// Log config for periodic
	EnableLogPeriodic bool
}

var logConfig = LogConfig{
	true,
	false,
	false,
	false,
	false,
	false,
	false,
	false,
	false,
	false,
}

func init() {
	DebugRaft := os.Getenv("DEBUG_RAFT")
	if DebugRaft != "ON" {
		logConfig = LogConfig{
			false,
			false,
			false,
			false,
			false,
			false,
			false,
			false,
			false,
			false,
		}
	}
}

func Printf(format string, a ...interface{}) {
	if logConfig.EnablePrintf {
		fmt.Printf(format, a...)
	}
}

func LogStruct(args interface{}) {
	t := reflect.TypeOf(args)
	name := t.Name()
	n := t.NumField()
	v := reflect.ValueOf(args)
	fmt.Printf("------------------------%v------------------------\n", name)
	for i := 0; i < n; i++ {
		fieldName := t.Field(i).Name
		fieldValue := v.FieldByName(fieldName)
		fmt.Printf("%v=%v\n", fieldName, fieldValue)
	}
	fmt.Println("")
}

func LogRaft(rf *Raft) {
	fmt.Printf("------------------------[Raft %d] status------------------------\n", rf.me)
	fmt.Printf("currentTerm=%d\n", rf.currentTerm)
	fmt.Printf("voteFor=%d\n", rf.voteFor)
	fmt.Printf("logs=%v\n", rf.logs)
	fmt.Printf("commitIndex=%d\n", rf.commitIndex)
	fmt.Printf("lastApplied=%d\n", rf.lastApplied)
	if rf.role == LEADER {
		fmt.Printf("nextIndex=%d\n", rf.nextIndex)
		fmt.Printf("matchIndex=%d\n", rf.matchIndex)
	}
	fmt.Printf("lastIncludedTerm=%d\n", rf.lastIncludeTerm)
	fmt.Printf("lastIncludedIndex=%d\n", rf.lastIncludedIndex)
	var role string
	if rf.role == LEADER {
		role = "LEADER"
	} else if rf.role == CANDIDATE {
		role = "CANDIDATE"
	} else {
		role = "FOLLOWER"
	}
	fmt.Printf("role=%v\n", role)
}

// Log utility functions for callers

func (rf *Raft) LogSendAppendEntriesPerpare(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogSendAppendEntries {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] prepares SendAppendEntries with [RPCID %d] -> {Server %d}\n", rf.me, args.RPCID, server)
		LogStruct(args)
		LogRaft(rf)
	}
}

func (rf *Raft) LogSendAppendEntriesIn(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogSendAppendEntries {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] enters SendAppendEntries with [RPCID %d] -> {Server %d}\n", rf.me, args.RPCID, server)
		LogStruct(args)
		LogStruct(reply)
		LogRaft(rf)
	}
}

func (rf *Raft) LogSendAppendEntriesOut(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogSendAppendEntries {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] exits SendAppendEntries with [RPCID %d] -> {Server %d}\n", rf.me, args.RPCID, server)
		LogRaft(rf)
	}
}

func (rf *Raft) LogSendRequestVoteIn(server int, args RequestVoteArgs, reply RequestVoteReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogSendRequestVote {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] enters SendRequestVote with [RPCID %d] -> {Server %d}\n", rf.me, args.RPCID, server)
		LogStruct(args)
		LogStruct(reply)
		LogRaft(rf)
	}
}

func (rf *Raft) LogSendRequestVoteOut(server int, args RequestVoteArgs, reply RequestVoteReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogSendRequestVote {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] exits SendRequestVote with [RPCID %d] -> {Server %d}\n", rf.me, args.RPCID, server)
		LogRaft(rf)
	}
}

func (rf *Raft) LogSendInstallSnapshotPrepare(server int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogSendInstallSnapshot {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] prepares SendInstallSnapshot with [RPCID %d] -> {Server %d}\n", rf.me, args.RPCID, server)
		LogStruct(args)
		LogRaft(rf)
	}
}

func (rf *Raft) LogSendInstallSnapshotIn(server int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogSendInstallSnapshot {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] enters SendInstallSnapshot with [RPCID %d] -> {Server %d}\n", rf.me, args.RPCID, server)
		LogStruct(args)
		LogStruct(reply)
		LogRaft(rf)
	}
}

func (rf *Raft) LogSendInstallSnapshotOut(server int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogSendInstallSnapshot {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] exits SendInstallSnapshot with [RPCID %d] -> {Server %d}\n", rf.me, args.RPCID, server)
		LogRaft(rf)
	}
}

// Log utility functions for handlers

func (rf *Raft) LogAppendEntriesIn(args AppendEntriesArgs, reply AppendEntriesReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogAppendEntries {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] -> {Server %d} enters AppendEntries with [RPCID %d]\n", args.LeaderId, rf.me, args.RPCID)
		LogStruct(args)
		LogRaft(rf)
	}
}

func (rf *Raft) LogAppendEntriesOut(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogAppendEntries {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] -> {Server %d} exits AppendEntries with [RPCID %d]\n", args.LeaderId, rf.me, args.RPCID)
		//LogStruct(reply)
		LogRaft(rf)
	}
}

func (rf *Raft) LogRequestVoteIn(server int, args RequestVoteArgs, reply RequestVoteReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogRequestVote {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] -> {Server %d} enters RequestVote with [RPCID %d]\n", args.CandidateId, rf.me, args.RPCID)
		LogStruct(args)
		LogRaft(rf)
	}
}

func (rf *Raft) LogRequestVoteOut(server int, args RequestVoteArgs, reply RequestVoteReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogRequestVote {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] -> {Server %d} exits RequestVote with [RPCID %d]\n", args.CandidateId, rf.me, args.RPCID)
		//LogStruct(reply)
		LogRaft(rf)
	}
}

func (rf *Raft) LogInstallSnapshotIn(server int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogInstallSnapshot {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] enters InstallSnapshot with [RPCID %d]\n", server, args.RPCID)
		LogStruct(args)
		LogRaft(rf)
	}
}

func (rf *Raft) LogInstallSnapshotOut(server int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogInstallSnapshot {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] exits InstallSnapshot with [RPCID %d]\n", server, args.RPCID)
		//LogStruct(reply)
		LogRaft(rf)
	}
}

// Log utility functions for persistence

func (rf *Raft) LogPersist(ps PersistentState) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogPersist {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] persists state\n", rf.me)
		LogStruct(ps)
		LogRaft(rf)
	}
}

func (rf *Raft) LogReadPersist(ps PersistentState) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogPersist {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] reads persisted state\n", rf.me)
		LogStruct(ps)
		LogRaft(rf)
	}
}

// Log utility functions for snapshot

func (rf *Raft) LogSnapshotIn(index int) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogSnapshot {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] enters Snapshot with index=%d\n", rf.me, index)
		LogRaft(rf)
	}
}

func (rf *Raft) LogSnapshotOut(index int) {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogSnapshot {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		log.Printf("[Server %d] exits Snapshot with index=%d\n", rf.me, index)
		LogRaft(rf)
	}
}

// Log utlity functions for periodic

func (rf *Raft) LogSetCommitterIn() {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogPeriodic {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		LogRaft(rf)
	}
}

func (rf *Raft) LogSetCommitterOut() {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogPeriodic {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		LogRaft(rf)
	}
}

func (rf *Raft) LogSetApplierIn() {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogPeriodic {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		LogRaft(rf)
	}
}

func (rf *Raft) LogSetApplierOut() {
	if !rf.verbose {
		return
	}
	if logConfig.EnableLogPeriodic {
		LogMutex.Lock()
		defer LogMutex.Unlock()
		fmt.Println("")
		log.Println("====================================================================")
		defer log.Println("====================================================================")
		LogRaft(rf)
	}
}
