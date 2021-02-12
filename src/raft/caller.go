package raft

import (
	"fmt"
	"sync"
)

//
// this function send AppendEntries RPCs to all its peers with no log
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// I don't think we need to wait for AppendEntries RPC to return, since we will handle the reply in SendAppendEntries
// and if the role changes, MainRoutine will know in the switch statement in the next iteration
//
func (rf *Raft) SendHeartbeats() {
	fmt.Printf("[server%d] enters SendHeartbeats\n", rf.me)
	defer fmt.Printf("[Server%d] quits SendHeartbeats", rf.me)

	rf.mu.Lock()                   // lock raft instance to prepare the RPC arguments
	term := rf.currentTerm         // my term
	leaderId := rf.me              // current leader, which is me
	leaderCommit := rf.commitIndex // last log I committed
	rf.mu.Unlock()                 // unlock raft when RPC arguments are prepared

	statusMutex := sync.Mutex{}
	status := make([]int, len(rf.peers)) // whether each peer's RPC succeed or not, all initialized to 0 (SENDING)

	for idx, _ := range rf.peers {
		if idx == leaderId {
			status[idx] = SUCCESS // I don't have to send heartbeat to myself, so my RPC is successful
			continue
		}
		rf.mu.Lock()                              // lock raft instance to prepare the prevLogIndex
		prevLogIndex := rf.nextIndex[idx] - 1     // index of log entry immediately preceding new ones (nextIndex-1)
		prevLogTerm := rf.logs[prevLogIndex].Term // term of prevLogIndex entry
		entries := make([]Log, 0)                 // TODO: heartbeat's entry should be determined by nextIndex?
		rf.mu.Unlock()                            // unlock raft when prevLogIndex are prepared
		args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
		reply := AppendEntriesReply{-1, false}                                // if you see -1 in reply, then the receiver never receives the RPC
		go rf.SendAppendEntries(idx, args, reply, &status[idx], &statusMutex) // pass a copy instead of reference (I think args and reply may lost after it returns)
	}

	// no need to check RPC status since no need to retry for heartbeat, so it's fine if RPC failed
}

//
// this function send AppendEntries RPCs to all its peers with logs that need to be replicated
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// if majority of the cluster replied success, then the logs are regard as replicated
// then we commit these logs on the leader side, and followers will commit these in consequent AppendEntries RPCs
//
func (rf *Raft) RequestReplication(commandIndex int) {
	fmt.Printf("[Server%d] enters RequestReplication", rf.me)
	defer fmt.Printf("[Server%d] quits RequestReplication", rf.me)

	rf.mu.Lock()                   // lock raft instance to prepare the RPC arguments
	term := rf.currentTerm         // my term
	leaderId := rf.me              // current leader, which is me
	leaderCommit := rf.commitIndex // last log I committed
	rf.mu.Unlock()                 // unlock raft when RPC arguments are prepared

	statusMutex := sync.Mutex{}
	statuses := make([]int, len(rf.peers))                // whether each peer's RPC succeed or not, all initialized to 0 (SENDING)
	allArgs := make([]AppendEntriesArgs, len(rf.peers))   // slice to store all AppendEntries RPC argmuents
	allReply := make([]AppendEntriesReply, len(rf.peers)) // slice to store all AppendEntries RPC argmuents

	for idx, _ := range rf.peers {
		if idx == leaderId {
			statuses[idx] = SUCCESS // I don't have to send AppendEntries RPC to myself, so my RPC is successful
			continue                // I already appended this shit
		}
		rf.mu.Lock()                              // lock raft instance to prepare the prevLogIndex
		entries := rf.logs[rf.nextIndex[idx]:]    // nextIndex's possible maximal value is len(rf.logs), since we just append a new log, it won't exceed bound
		prevLogIndex := rf.nextIndex[idx] - 1     // index of log entry immediately preceding new ones (nextIndex-1)
		prevLogTerm := rf.logs[prevLogIndex].Term // term of prevLogIndex entry
		rf.mu.Unlock()                            // unlock raft when prevLogIndex are prepared
		args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
		reply := AppendEntriesReply{-1, false} // if you see -1 in reply, then the receiver never receives the RPC
		allArgs[idx] = args
		allReply[idx] = reply
		go rf.SendAppendEntries(idx, args, reply, &statuses[idx], &statusMutex) // pass a copy instead of reference (I think args and reply may lost after it returns)
	}

	// TODO: use a chan with buffer to communicate so we know which RPC we should retry
	//go func(status []int, allArgs []AppendEntriesArgs, allReply []AppendEntriesReply) {
	//	for { // since ultimately there will be election timeout, so this goroutine eventually quits
	//		time.Sleep(30 * time.Millisecond) // sleep a while to save CPU and check role later
	//		statusMutex.Lock()
	//		for i, status := range status {
	//			if status == FAIL { // if RPC failed, then retry this RPC until it succeed
	//				go rf.SendAppendEntries(i, allArgs[i], allReply[i], &statuses[i], &statusMutex) // retry with exactly the same args
	//				_ = i
	//			}
	//		}
	//		statusMutex.Unlock()
	//	}
	//}(statuses, allArgs, allReply)
}

//
// send AppendEntries RPC to a single server and handle the reply
//
func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply AppendEntriesReply, result *int, statusMutex *sync.Mutex) {
	fmt.Printf("[Server%d] enters SendAppendEntries with prevLogIndex %d in term %d\n", rf.me, args.PrevLogIndex, args.Term)
	defer fmt.Printf("[Server%d] quits SendAppendEntries with prevLogIndex %d in term %d\n", rf.me, args.PrevLogIndex, args.Term)

	success := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	//fmt.Printf("=================================[Server%d] SendAppendEntries Status Lock=================================\n", rf.me)
	statusMutex.Lock()
	if !success { // RPC failed
		*result = FAIL // set status to FAIL
		fmt.Printf("AppendEntries from LEADER %d to FOLLOWER %d RPC failed\n", rf.me, server)
		//fmt.Printf("=================================[Server%d] SendAppendEntries Status Unlock=================================\n", rf.me)
		statusMutex.Unlock()
		// TODO: this might be where deadlock occurs
		return
	} else {
		*result = SUCCESS
	}
	//fmt.Printf("=================================[Server%d] SendAppendEntries Status Unlock=================================\n", rf.me)
	statusMutex.Unlock()

	//fmt.Printf("=================================[Server%d] SendAppendEntries Lock=================================\n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer fmt.Printf("=================================[Server%d] SendAppendEntries Lock=================================\n", rf.me)
	defer rf.LogAppendEntriesSend(rf.me, server, &args, &reply)

	if args.Term != rf.currentTerm { // a long winding path of blood, sweat, tears and despair
		return
	}

	if reply.Term > rf.currentTerm { // I should no longer be the leader since my term is too old
		rf.currentTerm = reply.Term // set currentTerm = T
		rf.role = FOLLOWER          // convert to follower
		return
	}

	if reply.Success { // this follower's log now is matched to mine, so update the nextIndex and matchIndex
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries) // prevLog matches, and since success, entries just append also matches
		rf.nextIndex[server] = rf.matchIndex[server] + 1              // increment nextIndex by the number of entries just append
	} else { // this follower didn't catch up with my logs
		rf.nextIndex[server] = rf.nextIndex[server] - 1 // decrement nextIndex (since false means it doesn't match, matchIndex should still be 0)
	}

	if rf.matchIndex[server] != len(rf.logs)-1 { // if not matched, keep sending AppendEntries to force it to match
		// since this is re-sending, you need to reset you arguments since they might change
		term := rf.currentTerm // my term
		leaderId := rf.me      // current leader, which is me
		nextIndex := rf.nextIndex[server]
		entries := rf.logs[nextIndex:] // send the log entry with nextIndex, since follower doesn't match, nextIndex won't exceed len(rf.log)
		leaderCommit := rf.commitIndex
		prevLogIndex := rf.nextIndex[server] - 1
		prevLogTerm := rf.logs[prevLogIndex].Term
		args = AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
		reply = AppendEntriesReply{-1, false}                             // if you see -1 in reply, then the receiver never receives the RPC
		go rf.SendAppendEntries(server, args, reply, result, statusMutex) // resend AppendEntries RPC
	}
}

//
// as a raft instance becomes a candidate, it asks all its peers to vote after it votes for itself
// it send RPC request for each server in a separate goroutine and handle RPC reply in a same goroutine;
// according to the result of the election, it decides whether it becomes the leader or convert to follower;
// since labrpc guarantees that every RPC returns, we don't need to handle RPC timeout, etc.
// This function quits only when I converts to follower or leader, or my election time expires so I should re-elect
//
func (rf *Raft) RunElection() {
	fmt.Printf("[Server%d] enters RunElection\n", rf.me)
	defer fmt.Printf("[Server%d] quits RunElection\n", rf.me)

	rf.mu.Lock()                              // lock raft instance to prepare the RPC arguments
	rf.ResetTimer()                           // reset the election timer because when starting an election
	term := rf.currentTerm                    // my term
	candidateId := rf.me                      // candidate id, which is me!
	lastLogIndex := len(rf.logs) - 1          // index of my last log entry
	lastLogTerm := rf.logs[lastLogIndex].Term // term of my last log entry
	rf.mu.Unlock()                            // unlock raft when RPC arguments are prepared

	//electionDone := make(chan int, 1)
	upVote := 1   // how many servers agree to vote for me (initialized to 1 since I vote for myself!)
	downVote := 0 // how many servers disagree to vote for me

	for idx, _ := range rf.peers {
		if idx == candidateId {
			continue // I don't have to vote for myself, I did this in my MainRoutine function
		}
		args := RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
		reply := RequestVoteReply{-1, false}                        // if you see -1 in reply, then the receiver never receives the RPC
		go rf.SendRequestVote(idx, args, reply, &upVote, &downVote) // send RPC to each server
	}
}

//
// send RequestVote RPC to a single server and handle the reply
// also signal the voteDone cond var
//
func (rf *Raft) SendRequestVote(server int, args RequestVoteArgs, reply RequestVoteReply, upVote *int, downVote *int) {
	fmt.Printf("[Server%d] enters SendRequestVote in term %d\n", rf.me, args.Term)
	defer fmt.Printf("[Server%d] quits SendRequestVote in term %d\n", rf.me, args.Term)

	success := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !success { // RPC failed
		fmt.Printf("RequestVote from LEADER %d to FOLLOWER %d RPC failed\n", rf.me, server)
		return
	}
	// TODO: do we really have to discard reply with a different term that mismatches args' term?

	//fmt.Printf("=================================[Server%d] SendRequestVote Lock=================================\n", rf.me)
	rf.mu.Lock()         // add mutex lock before you access attributes of raft instance
	defer rf.mu.Unlock() // release mutex lock when the function quits
	//defer fmt.Printf("=================================[Server%d] SendRequestVote Unlock=================================\n", rf.me)
	defer rf.LogRequestVoteSend(rf.me, server, &args, &reply)

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
		rf.role = FOLLOWER          // convert to follower
		return
	}

	if rf.role == CANDIDATE { // if there is already a result, then I don't need to check this shit
		if reply.VoteGranted { // this server agree to vote for me
			*upVote++
			if *upVote > len(rf.peers)/2 && reply.Term == rf.currentTerm { // I won majority in this term
				rf.role = LEADER // so I'm a leader now
				// initialized the leader
				for idx, _ := range rf.nextIndex {
					rf.nextIndex[idx] = args.LastLogIndex + 1 // initialize nextIndex to leader last log index+1
					rf.matchIndex[idx] = 0                    // initialize matchIndex to 0
				}
				go rf.SetCommitter()   // set a committer to periodically check if the commitIndex can be incremented
				go rf.SendHeartbeats() // upon election, send initial heartbeat to each server
			}
		} else { // this server agree to vote for me
			*downVote++
			if *downVote > len(rf.peers)/2 && reply.Term == rf.currentTerm { // I lost majority in this term
				rf.role = FOLLOWER // so I'm a follower now
			}
		}
	}
}
