package raft

import (
	"sync/atomic"
)

var serialNumber int64 = 0

//
// this function send AppendEntries RPCs to all its peers with no log
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// I don't think we need to wait for AppendEntries RPC to return, since we will handle the reply in SendAppendEntries
// and if the role changes, MainRoutine will know in the switch statement in the next iteration
//
func (rf *Raft) SendHeartbeats(term int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term != rf.currentTerm { // in case currentTerm is changed by other goroutines before sending RPC
		return
	}

	leaderId := rf.me              // current leader, which is me
	leaderCommit := rf.commitIndex // last log I committed
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		prevLogIndex := rf.nextIndex[idx] - 1     // index of log entry immediately preceding new ones (nextIndex-1)
		prevLogTerm := rf.logs[prevLogIndex].Term // term of prevLogIndex entry
		entries := make([]Log, 0)                 // heartbeat should carry no log, if not match, resending will carry logs
		id := atomic.AddInt64(&serialNumber, 1)   // get the RPC's serial number
		args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit, id}
		reply := AppendEntriesReply{}
		go rf.SendAppendEntries(idx, args, reply) // pass a copy instead of reference (I think args and reply may lost after it returns)
	}
}

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
	for idx, _ := range rf.peers {
		if idx == leaderId {
			continue // I already appended this shit
		}
		entries := rf.logs[rf.nextIndex[idx]:] // nextIndex's possible maximal value is len(rf.logs), since we just append a new log, it won't exceed bound
		entriesCopy := make([]Log, len(entries))
		copy(entriesCopy, entries)
		prevLogIndex := rf.nextIndex[idx] - 1     // index of log entry immediately preceding new ones (nextIndex-1)
		prevLogTerm := rf.logs[prevLogIndex].Term // term of prevLogIndex entry
		id := atomic.AddInt64(&serialNumber, 1)   // get the RPC's serial number
		args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entriesCopy, leaderCommit, id}
		reply := AppendEntriesReply{}
		go rf.SendAppendEntries(idx, args, reply) // pass a copy instead of reference (I think args and reply may lost after it returns)
	}
}

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

	if reply.Ignore {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm || rf.role != LEADER { // a long winding path of blood, sweat, tears and despair
		return
	}

	if reply.Term > rf.currentTerm { // I should no longer be the leader since my term is too old
		rf.currentTerm = reply.Term // set currentTerm = T
		rf.voteFor = -1             // reset voteFor
		rf.role = FOLLOWER          // convert to follower
		go rf.persist()             // currentTerm and voteFor are changed, so I need to save my states
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

	if rf.matchIndex[server] != len(rf.logs)-1 { // if not matched, keep sending AppendEntries to force it to match
		// since this is re-sending, you need to reset you arguments since they might change
		term := rf.currentTerm // my term
		leaderId := rf.me      // current leader, which is me
		nextIndex := rf.nextIndex[server]
		entries := rf.logs[nextIndex:] // send the log entry with nextIndex, since follower doesn't match, nextIndex won't exceed len(rf.log)
		entriesCopy := make([]Log, len(entries))
		copy(entriesCopy, entries)
		leaderCommit := rf.commitIndex
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.logs[prevLogIndex].Term
		id := atomic.AddInt64(&serialNumber, 1) // get the RPC's serial number
		args = AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entriesCopy, leaderCommit, id}
		reply = AppendEntriesReply{}
		go rf.SendAppendEntries(server, args, reply) // resend AppendEntries RPC
	}
}

//
// as a raft instance becomes a candidate, it asks all its peers to vote after it votes for itself
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// according to the result of the election, it decides whether it becomes the leader or convert to follower;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// This function quits only when I converts to follower or leader, or my election time expires so I should re-elect
//
func (rf *Raft) RunElection(term int) {

	rf.mu.Lock()                              // lock raft instance to prepare the RPC arguments
	rf.ResetTimer()                           // reset the election timer because when starting an election
	candidateId := rf.me                      // candidate id, which is me!
	lastLogIndex := len(rf.logs) - 1          // index of my last log entry
	lastLogTerm := rf.logs[lastLogIndex].Term // term of my last log entry
	rf.mu.Unlock()                            // unlock raft when RPC arguments are prepared

	//electionDone := make(chan int, 1)
	rf.upVote = 1   // how many servers agree to vote for me (initialized to 1 since I vote for myself!)
	rf.downVote = 0 // how many servers disagree to vote for me

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term != rf.currentTerm { // in case currentTerm is changed by other goroutines before sending RPC
		return
	}

	for idx, _ := range rf.peers {
		if idx == candidateId {
			continue // I don't have to vote for myself, I did this in my MainRoutine function
		}
		id := atomic.AddInt64(&serialNumber, 1) // get the RPC's serial number
		args := RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm, id}
		reply := RequestVoteReply{}
		go rf.SendRequestVote(idx, args, reply) // send RPC to each server
	}
}

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

	if reply.Ignore {
		return
	}

	rf.mu.Lock()         // add mutex lock before you access attributes of raft instance
	defer rf.mu.Unlock() // release mutex lock when the function quits

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
		go rf.persist()             // currentTerm and voteFor are changed, so I need to save my states
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
				go rf.SetCommitter()                 // set a committer to periodically check if the commitIndex can be incremented
				go rf.SendHeartbeats(rf.currentTerm) // upon election, send initial heartbeat to each server
			}
		} else { // this server agree to vote for me
			rf.downVote++
			if rf.downVote > len(rf.peers)/2 && reply.Term == rf.currentTerm { // I lost majority in this term
				rf.role = FOLLOWER // so I'm a follower now
			}
		}
	}
}

//
// this function find the appropriate nextIndex a raft instance should set according to AppendEntries RPC's reply
// Note: if rf.nextIndex[server]-1 == args.PrevLogIndex is not satisfied, which means in other RPCs nextIndex is already reset
// then this function should not be called because someone else already adjusted the value of nextIndex
//
func (rf *Raft) findNextIndex(args *AppendEntriesArgs, reply *AppendEntriesReply, server int) int {

	lastMatchedIdx := rf.findLastIndex(reply.ConflictTerm)

	// case 1: exists an entry in its log with ConflictTerm
	if lastMatchedIdx != -1 {
		return lastMatchedIdx + 1
	} else {
		// case 2: no entry with ConflictTerm (either ConflictTerm is none or I don't have a log entry with ConflictTerm)
		return reply.ConflictIndex
	}
}
