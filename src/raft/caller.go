package raft

import (
	"sync/atomic"
)

var serialNumber int64 = 0

// SendHeartbeats
//
// this function send AppendEntries RPCs to all its peers with no log
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// I don't think we need to wait for AppendEntries RPC to return, since we will handle the reply in SendAppendEntries
// and if the role changes, MainRoutine will know in the switch statement in the next iteration
//
func (rf *Raft) SendHeartbeats(term int) {

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	if term != rf.currentTerm { // in case currentTerm is changed by other goroutines before sending RPC
		return
	}

	leaderId := rf.me              // current leader, which is me
	leaderCommit := rf.commitIndex // last log I committed
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		prevLogIndex := rf.nextIndex[server] - 1 // index of log entry immediately preceding new ones (nextIndex-1)
		var prevLogTerm int

		// we need to know the prevLogTerm, but the log entry of prefLogIndex may be already compacted in snapshot
		if prevLogIndex < rf.lastIncludedIndex {
			// actual index:            0 1 2
			// log:         0 1 2 3 4 5 6 7 8
			// snapshot:    |---------|
			// prevLogIndex:      |
			// nextIndex:           |
			// entries:             |-------|
			// lastIncludedIndex = 5, prevLogTerm = 3, leader has already discarded the next log entry that it needs to send
			// we don't know what's the term of prevLogIndex since it is in the snapshot, so we just install snapshot and return

			// Note: if prevLogIndex == lastIncludedIndex, we still need to install snapshot
			// this is because rf.logs might be empty, and the consequent handling will be troublesome
			// thus we just install snapshot to make it easier

			snapshot := make([]byte, len(rf.snapshot))
			copy(snapshot, rf.snapshot)

			id := atomic.AddInt64(&serialNumber, 1) // get the RPC's serial number
			rf.newestInstallSnapshotRPCID[server] = id
			args := InstallSnapshotArgs{
				term,
				leaderId,
				rf.lastIncludedIndex,
				rf.lastIncludeTerm,
				snapshot,
				id,
			}
			reply := InstallSnapshotReply{}
			go rf.SendInstallSnapshot(server, args, reply)
			return
		} else if prevLogIndex == rf.lastIncludedIndex {
			prevLogTerm = rf.lastIncludeTerm
		} else { // prevLogIndex > rf.lastIncludedIndex
			// actual index:            0 1 2
			// log:         0 1 2 3 4 5 6 7 8
			// snapshot:    |---------|
			// prevLogIndex:            |
			// nextIndex:                 |
			// entries:                   |-|
			// lastIncludedIndex = 5, prevLogTerm = 6, leader still has the next log entry that it needs to send
			// the actual index of logs to send is 6-5 = 1

			// Node: if prevLogIndex > rf.lastIncludedIndex, rf.logs will never be empty
			// because prefLogIndex < len(rf.logs) (before compaction) = lastIncludedIndex+len(rf.logs)+1 (after compaction)
			// if rf.logs is empty, len(rf.logs) == 0, prefLogIndex < lastIncludedIndex+1, prefLogIndex <= lastIncludedIndex
			// and this case is already handled

			actualIndex := prevLogIndex - rf.lastIncludedIndex - 1
			prevLogTerm = rf.logs[actualIndex].Term // term of prevLogIndex entry
		}

		entries := make([]Log, 0)               // heartbeat should carry no log, if not match, resending will carry logs
		id := atomic.AddInt64(&serialNumber, 1) // get the RPC's serial number
		rf.newestAppendEntriesRPCID[server] = id
		args := AppendEntriesArgs{
			term,
			leaderId,
			prevLogIndex,
			prevLogTerm,
			entries,
			leaderCommit,
			id,
		}
		reply := AppendEntriesReply{}
		go rf.SendAppendEntries(server, args, reply) // pass a copy instead of reference (I think args and reply may lost after it returns)
	}
}

// RequestReplication
//
// this function send AppendEntries RPCs to all its peers with logs that need to be replicated
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// if majority of the cluster replied success, then the logs are regard as replicated
// then we commit these logs on the leader side, and followers will commit these in consequent AppendEntries RPCs
//
func (rf *Raft) RequestReplication(term int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term != rf.currentTerm { // in case currentTerm is changed by other goroutines before sending RPC
		return
	}

	leaderId := rf.me              // current leader, which is me
	leaderCommit := rf.commitIndex // last log I committed
	for server, _ := range rf.peers {
		if server == leaderId {
			continue // I already appended this shit
		}
		nextIndex := rf.nextIndex[server]
		actualNextIndex := nextIndex - rf.lastIncludedIndex - 1
		prevLogIndex := rf.nextIndex[server] - 1 // index of log entry immediately preceding new ones (nextIndex-1)
		prevLogTerm := -1
		var entries []Log

		// we need to know the prevLogTerm, but the log entry of prefLogIndex may be already compacted in snapshot
		if prevLogIndex < rf.lastIncludedIndex {
			// actual index:            0 1 2
			// log:         0 1 2 3 4 5 6 7 8
			// snapshot:    |---------|
			// prevLogIndex:      |
			// nextIndex:           |
			// entries:             |-------|
			// lastIncludedIndex = 5, prevLogTerm = 3, leader has already discarded the next log entry that it needs to send
			// we don't know what's the term of prevLogIndex since it is in the snapshot, and we cannot send log of index 4, 5
			// so we just install snapshot and return

			// Note: if prevLogIndex == lastIncludedIndex, we still need to install snapshot
			// this is because rf.logs might be empty, and the consequent handling will be troublesome
			// thus we just install snapshot to make it easier

			id := atomic.AddInt64(&serialNumber, 1) // get the RPC's serial number
			rf.newestInstallSnapshotRPCID[server] = id
			args := InstallSnapshotArgs{
				term,
				leaderId,
				rf.lastIncludedIndex,
				rf.lastIncludeTerm,
				rf.snapshot,
				id,
			}
			reply := InstallSnapshotReply{}
			go rf.SendInstallSnapshot(server, args, reply)
			return
		} else if prevLogIndex == rf.lastIncludedIndex {
			prevLogTerm = rf.lastIncludeTerm
		} else { // prevLogIndex > rf.lastIncludedIndex
			// actual index:            0 1 2
			// log:         0 1 2 3 4 5 6 7 8
			// snapshot:    |---------|
			// prevLogIndex:            |
			// nextIndex:                 |
			// entries:                   |-|
			// lastIncludedIndex = 5, prevLogTerm = 6, leader still has the next log entry that it needs to send
			// the actual index of logs to send is 6-5 = 1

			// Node: if prevLogIndex > rf.lastIncludedIndex, rf.logs will never be empty
			// because prefLogIndex < len(rf.logs) (before compaction) = lastIncludedIndex+len(rf.logs)+1 (after compaction)
			// if rf.logs is empty, len(rf.logs) == 0, prefLogIndex < lastIncludedIndex+1, prefLogIndex <= lastIncludedIndex
			// and this case is already handled

			actualIndex := prevLogIndex - rf.lastIncludedIndex - 1
			prevLogTerm = rf.logs[actualIndex].Term // term of prevLogIndex entry
			actualNextIndex = actualIndex + 1
			nextIndex = actualIndex + rf.lastIncludedIndex + 1
		}

		entries = rf.logs[actualNextIndex:] // nextIndex's possible maximal value is len(rf.logs), since we just append a new log, it won't exceed bound
		entriesCopy := make([]Log, len(entries))
		copy(entriesCopy, entries)

		id := atomic.AddInt64(&serialNumber, 1) // get the RPC's serial number
		rf.newestAppendEntriesRPCID[server] = id
		args := AppendEntriesArgs{
			term,
			leaderId,
			prevLogIndex,
			prevLogTerm,
			entriesCopy,
			leaderCommit,
			id,
		}
		reply := AppendEntriesReply{}
		go rf.SendAppendEntries(server, args, reply) // pass a copy instead of reference (I think args and reply may lose after it returns)
	}
}

// SendAppendEntries
//
// send AppendEntries RPC to a single server and handle the reply
//
func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if rf.killed() { // if raft instance is dead, then no need to sending RPCs
		return
	}

	success := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	if !success { // RPC failed
		Printf("AppendEntries from LEADER %d to FOLLOWER %d [RPC %d] failed\n", rf.me, server, args.RPCID)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.LogSendAppendEntriesIn(server, args, reply)
	defer rf.LogSendAppendEntriesOut(server, args, reply)

	if args.Term != rf.currentTerm || rf.role != LEADER { // a long winding path of blood, sweat, tears and despair
		return
	}

	if reply.Term > rf.currentTerm { // I should no longer be the leader since my term is too old
		rf.currentTerm = reply.Term // set currentTerm = T
		rf.voteFor = -1             // reset voteFor
		rf.role = FOLLOWER          // convert to follower
		rf.persist(nil)             // currentTerm and voteFor are changed, so I need to save my states
		rf.ResetTimer()
		return
	}

	if reply.Success { // this follower's log now is matched to mine, so update the nextIndex and matchIndex
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries) // prevLog matches, and since success, entries just append also matches
		rf.nextIndex[server] = rf.matchIndex[server] + 1              // increment nextIndex by the number of entries just append
	} else { // this follower didn't catch up with my logs
		if rf.nextIndex[server]-1 == args.PrevLogIndex { // no one else has decremented nextIndex
			rf.nextIndex[server] = rf.findNextIndex(&args, &reply, server) // findNextIndex returns the entry where our logs might match, and nextIndex should be its next entry
		}
	}

	// actual index:            0 1 2
	// log:         0 1 2 3 4 5 6 7 8
	// matchIndex:                  |
	// len(rf.logs) = 3, matchIndex = 8, lastIncludedIndex = 5
	// if matchIndex == len(rf.logs)+lastIncludedIndex, it means that all logs are update to date
	if rf.matchIndex[server] != len(rf.logs)+rf.lastIncludedIndex { // if not matched, keep sending AppendEntries to force it to match
		// since this is re-sending, you need to reset you arguments since they might change
		term := rf.currentTerm // my term
		leaderId := rf.me      // current leader, which is me
		nextIndex := rf.nextIndex[server]
		actualNextIndex := nextIndex - rf.lastIncludedIndex - 1
		prevLogIndex := nextIndex - 1
		var prevLogTerm int
		var entries []Log

		// we need to know the prevLogTerm, but the log entry of prefLogIndex may be already compacted in snapshot
		if prevLogIndex < rf.lastIncludedIndex {
			// actual index:            0 1 2
			// log:         0 1 2 3 4 5 6 7 8
			// snapshot:    |---------|
			// prevLogIndex:      |
			// nextIndex:           |
			// entries:             |-------|
			// lastIncludedIndex = 5, prevLogTerm = 3, leader has already discarded the next log entry that it needs to send
			// we don't know what's the term of prevLogIndex since it is in the snapshot, and we cannot send log of index 4, 5
			// so we just install snapshot and return

			// Note: if prevLogIndex == lastIncludedIndex, we still need to install snapshot
			// this is because rf.logs might be empty, and the consequent handling will be troublesome
			// thus we just install snapshot to make it easier

			id := atomic.AddInt64(&serialNumber, 1) // get the RPC's serial number
			rf.newestInstallSnapshotRPCID[server] = id
			args := InstallSnapshotArgs{
				term,
				leaderId,
				rf.lastIncludedIndex,
				rf.lastIncludeTerm,
				rf.snapshot,
				id,
			}
			reply := InstallSnapshotReply{}
			go rf.SendInstallSnapshot(server, args, reply)
			return
		} else if prevLogIndex == rf.lastIncludedIndex {
			prevLogTerm = rf.lastIncludeTerm
		} else { // prevLogIndex > rf.lastIncludedIndex
			// actual index:            0 1 2
			// log:         0 1 2 3 4 5 6 7 8
			// snapshot:    |---------|
			// prevLogIndex:            |
			// nextIndex:                 |
			// entries:                   |-|
			// lastIncludedIndex = 5, prevLogTerm = 6, leader still has the next log entry that it needs to send
			// the actual index of logs to send is 6-5 = 1

			// Node: if prevLogIndex > rf.lastIncludedIndex, rf.logs will never be empty
			// because prefLogIndex < len(rf.logs) (before compaction) = lastIncludedIndex+len(rf.logs)+1 (after compaction)
			// if rf.logs is empty, len(rf.logs) == 0, prefLogIndex < lastIncludedIndex+1, prefLogIndex <= lastIncludedIndex
			// and this case is already handled

			actualIndex := prevLogIndex - rf.lastIncludedIndex - 1
			if actualIndex == -1 {
				prevLogTerm = rf.lastIncludeTerm
			} else {
				prevLogTerm = rf.logs[actualIndex].Term
			}
			prevLogTerm = rf.logs[actualIndex].Term // term of prevLogIndex entry
			actualNextIndex = actualIndex + 1
			nextIndex = actualIndex + rf.lastIncludedIndex + 1
		}

		entries = rf.logs[actualNextIndex:] // send the log entry with nextIndex, since follower doesn't match, nextIndex won't exceed len(rf.log)
		entriesCopy := make([]Log, len(entries))
		copy(entriesCopy, entries)
		leaderCommit := rf.commitIndex
		id := atomic.AddInt64(&serialNumber, 1) // get the RPC's serial number
		rf.newestRequestVoteRPCID[server] = id
		args = AppendEntriesArgs{
			term,
			leaderId,
			prevLogIndex,
			prevLogTerm,
			entriesCopy,
			leaderCommit,
			id,
		}
		reply = AppendEntriesReply{}
		go rf.SendAppendEntries(server, args, reply) // resend AppendEntries RPC
	}
}

// RunElection
//
// as a raft instance becomes a candidate, it asks all its peers to vote after it votes for itself
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// according to the result of the election, it decides whether it becomes the leader or convert to follower;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// This function quits only when I converts to follower or leader, or my election time expires so I should re-elect
//
func (rf *Raft) RunElection(term int) {

	//rf.mu.Lock()                              // lock raft instance to prepare the RPC arguments
	rf.ResetTimer()      // reset the election timer because when starting an election
	candidateId := rf.me // candidate id, which is me!

	var lastLogIndex, lastLogTerm int
	if len(rf.logs) == 0 {
		// actual index:              |
		// log:         0 1 2 3 4 5 6
		lastLogIndex = rf.lastIncludedIndex
		lastLogTerm = rf.lastIncludeTerm
	} else {
		// actual index:              0 1
		// log:         0 1 2 3 4 5 6 7 8
		lastLogActualIndex := len(rf.logs) - 1 // index of my last log entry
		lastLogIndex = lastLogActualIndex + rf.lastIncludedIndex + 1
		lastLogTerm = rf.logs[lastLogActualIndex].Term // term of my last log entry
	}

	//rf.mu.Unlock()                            // unlock raft when RPC arguments are prepared

	//electionDone := make(chan int, 1)
	rf.upVote = 1   // how many servers agree to vote for me (initialized to 1 since I vote for myself!)
	rf.downVote = 0 // how many servers disagree to vote for me

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	if term != rf.currentTerm { // in case currentTerm is changed by other goroutines before sending RPC
		return
	}

	for idx, _ := range rf.peers {
		if idx == candidateId {
			continue // I don't have to vote for myself, I did this in my MainRoutine function
		}
		id := atomic.AddInt64(&serialNumber, 1) // get the RPC's serial number
		rf.newestRequestVoteRPCID[idx] = id
		args := RequestVoteArgs{term,
			candidateId,
			lastLogIndex,
			lastLogTerm,
			id,
		}
		reply := RequestVoteReply{}
		go rf.SendRequestVote(idx, args, reply) // send RPC to each server
	}
}

// SendRequestVote
//
// send RequestVote RPC to a single server and handle the reply
// also signal the voteDone cond var
//
func (rf *Raft) SendRequestVote(server int, args RequestVoteArgs, reply RequestVoteReply) {
	if rf.killed() { // if raft instance is dead, then no need to sending RPCs
		return
	}

	success := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !success { // RPC failed
		Printf("RequestVote from LEADER %d to FOLLOWER %d [RPC %d] failed\n", rf.me, server, args.RPCID)
		return
	}

	rf.mu.Lock()         // add mutex lock before you access attributes of raft instance
	defer rf.mu.Unlock() // release mutex lock when the function quits

	rf.LogSendRequestVoteIn(server, args, reply)
	defer rf.LogSendRequestVoteOut(server, args, reply)

	if args.Term != rf.currentTerm { // a long winding path of blood, sweat, tears and despair
		return
	}

	if rf.role != CANDIDATE { // no need to count the vote since I'm no longer a candidate
		return
	}

	if reply.Term < rf.currentTerm { // this RPC is too old (a newer term now), just discard it
		return
	}

	if reply.Term > rf.currentTerm { // someone's term is bigger than me, so I'm not the newest one
		rf.currentTerm = reply.Term // update my current term
		rf.voteFor = -1             // reset my voteFor
		rf.role = FOLLOWER          // convert to follower
		rf.persist(nil)             // currentTerm and voteFor are changed, so I need to save my states
		rf.ResetTimer()
		return
	}

	if rf.role == CANDIDATE { // if there is already a result, then I don't need to check this shit
		if reply.VoteGranted { // this server agree to vote for me
			rf.upVote++
			if rf.upVote > len(rf.peers)/2 && reply.Term == rf.currentTerm { // I won majority in this term
				rf.role = LEADER // so I'm a leader now
				// initialized the leader
				for idx, _ := range rf.nextIndex {
					rf.nextIndex[idx] = args.LastLogIndex + 1 // initialize nextIndex to leader last log index+1
					rf.matchIndex[idx] = 0                    // initialize matchIndex to 0
				}
				go rf.SetCommitter()              // set a committer to periodically check if the commitIndex can be incremented
				rf.SendHeartbeats(rf.currentTerm) // upon election, send initial heartbeat to each server
			}
		} else { // this server agree to vote for me
			rf.downVote++
			if rf.downVote > len(rf.peers)/2 && reply.Term == rf.currentTerm { // I lost majority in this term
				rf.role = FOLLOWER // so I'm a follower now
			}
		}
	}
}

const (
	NoEntryWithGivenTerm = -3
	ConflictTermIsNone   = -2
)

//
// this function find the appropriate nextIndex a raft instance should set according to AppendEntries RPC's reply
// Note: if rf.nextIndex[server]-1 == args.PrevLogIndex is not satisfied, which means in other RPCs nextIndex is already reset
// then this function should not be called because someone else already adjusted the value of nextIndex
//
func (rf *Raft) findNextIndex(args *AppendEntriesArgs, reply *AppendEntriesReply, server int) int {

	// lastMatchedIdx could be one of the followings:
	// ConflictTermIsNone (-2)
	// NoEntryWithGivenTerm (-3)
	// a valid index (between 0 and len(rf.logs)-1)
	lastMatchedIdx := rf.findLastIndex(reply.ConflictTerm)

	if lastMatchedIdx != ConflictTermIsNone && lastMatchedIdx != NoEntryWithGivenTerm {
		// case 4: I have this term, and it's not in snapshot
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:   1 1 1 2 2 3 3 3
		// FOLLOWER: 1 1 1 2 2 2
		// prevLogIndex = 5
		// ConflictTerm = 2, ConflictIndex = 2
		// Leader has logs with conflict term 2, so hopefully followers has all correct logs up to term 2 (index = 3, 4),
		// but for some reason follower receives more logs with term 2 (index = 5)
		// We need to start with the last correct log with term 2, and next index is 5,
		// then follower will replicate log with index = 5, 6, 7
		return lastMatchedIdx + 1
	} else if lastMatchedIdx == ConflictTermIsNone {
		// case 1: follower doesn't have this term
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:       1 2 2 4 4 4
		// Snapshot: 1 1
		// FOLLOWER: 1 1 1 2
		// prevLogIndex = 5
		// ConflictTerm = 3, ConflictIndex = 3
		// Leader doesn't have a log with conflict term 3, so we need to start with conflict index, which will be follower's
		// log length (4). In this case, nextIndex = 4, prevLogIndex = 3
		// If prevLogIndex & prevLogTerm matches follower's, follower will hopefully replicate log with index = 4, 5, 6, 7
		return reply.ConflictIndex
	} else { // lastMatchedIdx == NoEntryWithGivenTerm
		// case 2: I really don't have this term
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:       1 2 2 4 4 4
		// Snapshot: 1 1
		// FOLLOWER: 1 1 1 3 3 3
		// prevLogIndex = 5, ConflictTerm = 3, ConflictIndex = 3
		// Leader doesn't have a log with conflict term 3, so we need to start with ConflictIndex, which will be follower's
		// first log index of conflict term 3 (3). If before this point follower got the correct log (index = 0, 1, 2), with
		// our next index = conflict index (3), follower will replicate log with index = 3, 4, 5, 6, 7

		// Another possibility
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:               4 4
		// Snapshot: 1 1 1 2 2 4
		// FOLLOWER: 1 1 1 3 3 3
		// same as case 3, use ConflictIndex as nextIndex, eventually AppendEntries RPC will install snapshot

		// case 3: I have this term, but it's in my snapshot
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:             4 4 4
		// Snapshot: 1 1 1 2 3
		// FOLLOWER: 1 1 1 3 3 3
		// prevLogIndex = 5, ConflictTerm = 3, ConflictIndex = 3
		// Leader could have this log entry if it's not in snapshot. In this case we still set nextIndex to ConflictIndex,
		// because when doing so, AppendEntries RPC will notice that I don't have log entry at ConflictIndex (3),
		// so I will just install snapshot
		return reply.ConflictIndex
	}
}

//
// find the last index of the given term in a raft instance's log
// don't use lock here or everything will be fucked up
//
func (rf *Raft) findLastIndex(term int) int {
	if term == ConflictTermIsNone { // follower doesn't have this term
		// case 1: follower doesn't have this term
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:       1 2 2 4 4 4
		// Snapshot: 1 1
		// FOLLOWER: 1 1 1 2
		// prevLogIndex:       |
		// with prevLogIndex=5, follower doesn't have log entry here, so we should return ConflictTermIsNone
		// Following code should set nextIndex to ConflictIndex, which is the length of follower's logs (4)
		// AppendEntries RPC will handle when log entry at nextIndex is already compacted, so we don't need to worry about it here
		// Return value is ConflictTermIsNone
		return ConflictTermIsNone
	}

	for i := len(rf.logs) - 1; i >= 0; i-- {
		// case 2: I really don't have this term
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:       1 2 2 4 4 4
		// Snapshot: 1 1
		// FOLLOWER: 1 1 1 3 3 3
		// prevLogIndex:       |
		// with prevLogIndex=5, so follower doesn't have log entry with term 4, and he replies me with ConflictTerm 3 and ConflictIndex 3
		// I cannot find any log entry with term=3, so I have to start from ConflictIndex, hopefully the handler got correct logs before ConflictIndex
		// For handler, log entries before ConflictIndex might be correct (because log entries with index >= ConflictIndex must be wrong)
		// So we start trying from ConflictIndex, if that does not match, I will receive false in consequent AppendEntries RPC
		// and I will try again from older log entries
		// In this case we should return NoEntryWithGivenTerm, and the following code should set nextIndex to ConflictIndex (3)

		// Another possibility
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:               4 4
		// Snapshot: 1 1 1 2 2 4
		// FOLLOWER: 1 1 1 3 3 3
		// prevLogIndex:       |
		// with prevLogIndex=5, so follower doesn't have log entry with term 4, and he replies me with ConflictTerm 3 and ConflictIndex 3
		// Same as case 3, we return NoEntryWithGivenTerm, and later we will use nextIndex = ConflictIndex, which is 3
		// and eventually AppendEntries RPC will notice we don't have log entry with index 3, then it will install snapshot

		// Return value is NoEntryWithGivenTerm

		// case 3: I have this term, but it's in my snapshot
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:             4 4 4
		// Snapshot: 1 1 1 2 3
		// FOLLOWER: 1 1 1 3 3 3
		// prevLogIndex:       |
		// with prevLogIndex=5, so he doesn't have log entry with term 4, and he replies me with ConflictTerm 3 and ConflictIndex 3
		// I had a log entry with term 3, but now it's in snapshot
		// In this case, ConflictTerm = 3, ConflictIndex=3 (first log entry with term=3)
		// We just return NoEntryWithGivenTerm, let the following code to determine should we install snapshot or not
		// In this case we cannot find log entry with ConflictTerm, so we just return NoEntryWithGivenTerm
		// Return value is NoEntryWithGivenTerm

		// case 4: I have this term, and it's not in snapshot
		// INDEX:    0 1 2 3 4 5 6 7
		// Actual:           0 1 2 3
		// =========================
		// LEADER:           3 4 4 4
		// Snapshot: 1 1 1 2
		// FOLLOWER: 1 1 1 3 3 3
		// prevLogIndex:       |
		// Then I should return index = 4 (actual index = 0, lastIncludedIndex = 3)
		// Return value is a valid index
		if rf.logs[i].Term == term {
			return i + rf.lastIncludedIndex + 1
		}
	}

	return NoEntryWithGivenTerm // if we return NoEntryWithGivenTerm, we don't have this term
}

// SendInstallSnapshot
//
// send InstallSnapshot RPC command a follower to replicate leader's snapshot
//
func (rf *Raft) SendInstallSnapshot(server int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	if rf.killed() { // if raft instance is dead, then no need to sending RPCs
		return
	}

	success := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	if !success { // RPC failed
		Printf("InstallSnapshot from LEADER %d to FOLLOWER %d [RPC %d] failed\n", rf.me, server, args.RPCID)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.LogSendInstallSnapshotIn(server, args, reply)
	defer rf.LogSendInstallSnapshotOut(server, args, reply)

	if args.Term != rf.currentTerm || rf.role != LEADER { // a long winding path of blood, sweat, tears and despair
		return
	}

	if reply.Term > rf.currentTerm { // I should no longer be the leader since my term is too old
		rf.currentTerm = reply.Term // set currentTerm = T
		rf.voteFor = -1             // reset voteFor
		rf.role = FOLLOWER          // convert to follower
		rf.persist(nil)             // currentTerm and voteFor are changed, so I need to save my states
		rf.ResetTimer()
		return
	}

	// Since follower has already accepted my snapshot, he should increment his commitIndex to snapshot's lastIncludedIndex
	// So I should update follower's nextIndex and matchIndex, too

	// If other InstallSnapshot RPC has already incremented nextIndex and its lastIncludedIndex is lager than me,
	// then my RPC must be delayed, so skip this
	if rf.matchIndex[server] >= args.LastIncludedIndex {
		return
	}

	// LastIncludedIndex must match
	rf.matchIndex[server] = args.LastIncludedIndex
	// So now we should sync logs after LastIncludedIndex
	rf.nextIndex[server] = args.LastIncludedIndex + 1

	// Snapshot has been synced, but we still need to sync entries after snapshot
	//rf.SendHeartbeats(server)
}
