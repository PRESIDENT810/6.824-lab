package raft

import (
	"math/rand"
	"time"
)

//
// the applier can be awaken by a cond var; every time there are logs are committed,
// you should awake the cond var and tell this function to apply logs
// which is to send log through applyCh, and also increase raft instance's lastApplied value
//
func (rf *Raft) SetApplier(applyCh chan ApplyMsg) {
	for {
		time.Sleep(100 * time.Millisecond)
		for {
			PrintLock("=================================[Server%d] Applier Lock=================================\n", rf.me)
			rf.mu.Lock()
			if rf.lastApplied < rf.commitIndex { // there is log committed but not applied
				rf.lastApplied++ // now apply the next command
				PrintLock("=================================[Server%d] Applier Unlock=================================\n", rf.me)
				command := rf.logs[rf.lastApplied].Command
				rf.mu.Unlock()
				applyMsg := ApplyMsg{true, command, rf.lastApplied}
				applyCh <- applyMsg
			} else {
				PrintLock("=================================[Server%d] Applier Unlock=================================\n", rf.me)
				rf.mu.Unlock()
				break
			}
		}
		if rf.killed() { // if the raft instance is killed, it means this test is finished and we should quit
			return
		}
	}
}

//
// only called when someone becomes leader
// the committer periodically check the following conditions to determine whether the leader's commitIndex should be incremented
// it first check whether you are the leader, because followers can only set commitIndex when receiving AppendEntries RPC
// then it check the following three conditions
// (1) there exists an N such that N > commitIndex; (2) a majority of matchIndex[i] >= N; (3) rf.logs[N].Term == rf.currentTerm
// if these conditions are satisfied, set commitIndex = N
// return when it's no longer leader
//
func (rf *Raft) SetCommitter() {
	for {
		PrintLock("=================================[Server%d] Committer Lock=================================\n", rf.me)
		rf.mu.Lock()
		if rf.role != LEADER { // if I'm not leader, I have no right to increment commitIndex here
			PrintLock("=================================[Server%d] Committer Unlock=================================\n", rf.me)
			rf.mu.Unlock()
			return
		}
		// N must not exceed log's bound
		for N := len(rf.logs) - 1; N > rf.commitIndex; N-- { // check if such N exists
			replicatedCnt := 1                            // how many servers have replicated this log, initialized to 1 since I already have this log
			for server, matchIdx := range rf.matchIndex { // count how many server replicated my log at N
				if server == rf.me { // no need to count me since I already replicated my log
					continue
				}
				if matchIdx >= N {
					replicatedCnt++ // one more server replicates log at N
				}
			}
			if replicatedCnt > len(rf.peers)/2 && rf.logs[N].Term == rf.currentTerm {
				rf.commitIndex = N
				break
			}
			N--
		}
		PrintLock("=================================[Server%d] Committer Unlock=================================\n", rf.me)
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		if rf.killed() { // if the raft instance is killed, it means this test is finished and we should quit
			return
		}
	}
}

//
// the timer to calculate time passed since rf.electionLastTime
// if the time is more than a random threshold between 150ms to 300ms
// then there is a timeout, and the raft instance should set electionExpired to true
// to notify itself it should run a new election
//
func (rf *Raft) SetTimer() {
	rand.Seed(int64(rf.me))         // set a random number seed to ensure it generates different random number
	timeout := rand.Int()%500 + 250 // generate a random timeout threshold between 150 to 300ms
	for {
		if rf.killed() { // if the raft instance is killed, it means this test is finished and we should quit
			return
		}
		time.Sleep(10 * time.Millisecond) // sleep a while to save some CPU time
		electionCurrentTime := time.Now()
		rf.mu.Lock()
		if electionCurrentTime.Sub(rf.electionLastTime).Milliseconds() > int64(timeout) {
			rf.electionExpired = true // election time expired! you should run a new election now
			if rf.role != LEADER {
				Printf("[Server%d]'s election time expired\n\n", rf.me)
			}
			rf.electionLastTime = electionCurrentTime // reset the timer
			timeout = rand.Int()%150 + 150            // reset the random timeout threshold
		}
		rf.mu.Unlock()
	}
}

//
// reset timer
//
func (rf *Raft) ResetTimer() {
	rf.electionExpired = false
	rf.electionLastTime = time.Now()
}

//
// why the fuck go don't have a integer min function???
//
func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}
