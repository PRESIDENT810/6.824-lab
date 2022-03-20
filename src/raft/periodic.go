package raft

import (
	"math/rand"
	"time"
)

// SetApplier
//
// the applier can be awaken by a cond var; every time there are logs are committed,
// you should awake the cond var and tell this function to apply logs
// which is to send log through applyCh, and also increase raft instance's lastApplied value
//
func (rf *Raft) SetApplier(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		rf.LogSetApplierIn()
		rf.mu.Unlock()
		for {
			rf.mu.Lock()
			if rf.lastApplied < rf.commitIndex { // there is log committed but not applied
				rf.lastApplied++ // now apply the next command
				// lastApplied is not the actual index, we need to consider snapshot and find the actual index
				// INDEX:    0 1 2 3 4 5 6 7
				// =========================
				// Actual:         0 1 2 3 4
				// FOLLOWER:       2 2 2 3 3
				// Snapshot: 1 1 1
				// lastApplied:        |
				// lastActualApplied = lastApplied (5) - lastIncludedIndex (2) - 1 = 2
				lastActualApplied := rf.lastApplied - rf.lastIncludedIndex - 1
				// There must be that lastActualApplied < len(rf.logs), because that implies
				// rf.lastApplied - rf.lastIncludedIndex (always >= -1) - 1 <= rf.lastApplied < len(rf.logs)

				var applyMsg ApplyMsg
				// if logs should be applied are in snapshot, we should apply our snapshot instead
				if lastActualApplied < 0 {
					applyMsg = ApplyMsg{false, nil, 0, true, rf.snapshot, rf.lastIncludeTerm, rf.lastIncludedIndex}
					// update lastApplied to last included log entry in snapshot
					rf.lastApplied = rf.lastIncludedIndex
				} else {
					command := rf.logs[lastActualApplied].Command
					applyMsg = ApplyMsg{true, command, rf.lastApplied, false, nil, 0, 0}
				}
				rf.mu.Unlock()
				applyCh <- applyMsg
			} else {
				rf.mu.Unlock()
				break
			}
		}
		rf.mu.Lock()
		rf.LogSetApplierIn()
		rf.mu.Unlock()
	}
}

// SetCommitter
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
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role != LEADER { // if I'm not leader, I have no right to increment commitIndex here
			rf.mu.Unlock()
			return
		}
		rf.LogSetCommitterIn()
		// N must not exceed log's bound
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// Actual:         0 1 2 3 4
		// FOLLOWER:       2 2 2 3 3
		// Snapshot: 1 1 1
		// commitIndex:        |
		// len(rf.logs) = 5, lastIncludedIndex = 2
		// commitIndex should be always larger than lastIncludedIndex, since uncommitted log entries shouldn't be persisted
		for N := len(rf.logs) + rf.lastIncludedIndex; N > (rf.commitIndex); N-- { // check if such N exists
			replicatedCnt := 1                            // how many servers have replicated this log, initialized to 1 since I already have this log
			for server, matchIdx := range rf.matchIndex { // count how many server replicated my log at N
				if server == rf.me { // no need to count me since I already replicated my log
					continue
				}
				if matchIdx >= N {
					replicatedCnt++ // one more server replicates log at N
				}
			}
			actualIndex := N - rf.lastIncludedIndex - 1
			if replicatedCnt > len(rf.peers)/2 && rf.logs[actualIndex].Term == rf.currentTerm {
				rf.commitIndex = N
				break
			}
			N--
		}
		rf.LogSetCommitterOut()
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

//
// the timer to calculate time passed since rf.electionLastTime
// if the time is more than a random threshold between 150ms to 300ms
// then there is a timeout, and the raft instance should set electionExpired to true
// to notify itself it should run a new election

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

//
func (rf *Raft) ticker() {
	rand.Seed(int64(rf.me) * time.Now().Unix()) // set a random number seed to ensure it generates different random number
	timeout := rand.Intn(300) + 300             // generate a random timeout threshold between 150 to 300ms
	for rf.killed() == false {                  // if the raft instance is killed, it means this test is finished and we should quit

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(5 * time.Millisecond) // sleep a while to save some CPU time
		electionCurrentTime := time.Now()
		rf.mu.Lock()
		if electionCurrentTime.Sub(rf.electionLastTime).Milliseconds() > int64(timeout) {
			rf.electionExpired = true // election time expired! you should run a new election now
			if rf.role != LEADER {
				Printf("[Server%d]'s election time expired\n\n", rf.me)
			}
			rf.electionLastTime = time.Now() // reset the timer
			timeout = rand.Intn(300) + 300   // generate a random timeout threshold between 150 to 300ms
		}
		rf.mu.Unlock()
	}
}

// ResetTimer
//
// reset timer
//
func (rf *Raft) ResetTimer() {
	rf.electionExpired = false
	rf.electionLastTime = time.Now()
}

//
// why the fuck go don't have a min function???
//
func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}
