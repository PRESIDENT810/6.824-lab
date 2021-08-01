package raft

//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate who is requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry

	RPCID int64 // identifier for RPC
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm of the receiver, for the sender to update itself
	VoteGranted bool // true means candidate received vote

	// if this RPC is received before any RPC that was sent after it, then ignore it (the newer RPC will take care)
	Ignore bool
}

//
// AppendEntries RPC arguments structure
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderId     int   // to let receiver know who is the current leader
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; includes more than one for efficiency)
	LeaderCommit int   // leader's commitIndex

	RPCID int64 // identifier for RPC
}

//
// AppendEntries RPC reply structure
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// these two attributes are used for optimization when rejecting AppendEntries RPC
	ConflictTerm  int // the term of the conflicting entry
	ConflictIndex int // the first log entry index that conflict with the leader's

	// if this RPC is received before any RPC that was sent after it, then ignore it (the newer RPC will take care)
	Ignore bool
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.persist()

	if args.Term < rf.currentTerm { // my term is newer
		reply.Term = rf.currentTerm // return my current term to update the sender
		reply.Success = false       // reply false if term < currentTerm
		rf.persist()                // currentTerm is changed, so I need to save my states
		return
	}

	rf.ResetTimer() // reset timer upon AppendEntries (but if the term in arguments is outdated, you should not reset your timer!)

	if args.Term > rf.currentTerm { // my term is too old
		rf.currentTerm = args.Term // update my term first
		rf.voteFor = -1            // in this new term, I didn't vote for anyone
		rf.role = FOLLOWER         // convert myself to a follower, no matter what is my old role
		rf.persist()               // currentTerm and voteFor are changed, so I need to save my states
	}

	if rf.role == CANDIDATE { // we have same term, but I'm a candidate (impossible for two leaders at same term)
		rf.role = FOLLOWER // convert myself to a follower, since this term we have a leader
		rf.persist()
	}

	prevLogIndex := args.PrevLogIndex // use a local variable to store prevLogIndex for efficiency

	// case 1: I don't even have a entry at this index, so you should retry at my last entry's index
	if len(rf.logs) <= prevLogIndex { // I don't have this log for this index, you have to retry with my last index
		reply.Term = rf.currentTerm        // same as sender's term (if newer, I returned false already; if older, I updated already)
		reply.Success = false              // reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.ConflictTerm = -2            // -2 means I don't have this entry index
		reply.ConflictIndex = len(rf.logs) // you have to retry with my last existing log
		return
	}

	myTerm := rf.logs[prevLogIndex].Term // my log entry's term at caller's prevLogIndex

	// case 2: I have the log entry at this index, but with conflicting term
	if myTerm != args.PrevLogTerm {
		reply.Term = rf.currentTerm                     // same as sender's term (if newer, I returned false already; if older, I updated already)
		reply.Success = false                           // reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.ConflictTerm = myTerm                     // this is the term of my conflicting log entry
		reply.ConflictIndex = rf.findFirstIndex(myTerm) // find the index for the first log entry with the conflicting term
		return
	}

	// if an existing entry conflicts with a new one (args.entries), delete the existing entry and all that follow it
	// append any new entries not already in the log
	for i, _ := range args.Entries {
		insertIndex := i + prevLogIndex + 1 // i+prevLogIndex <= last index of log (if len(log) <= prevLogIndex, it returns already)
		if insertIndex == len(rf.logs) {    // no log at insertIndex, so we append a new entry
			rf.logs = append(rf.logs, args.Entries[i])
		} else if rf.logs[insertIndex] != args.Entries[i] { // log at insertIndex doesn't match our entry
			rf.logs = rf.logs[:insertIndex]            // delete the existing entry and all that follow it
			rf.logs = append(rf.logs, args.Entries[i]) // add current log
		} else { // log at insertIndex matches our entry
			continue
		}
	}

	rf.persist() // my logs are changed, so I need to save my states

	if args.LeaderCommit > rf.commitIndex { // rule 5 for AppendEntries RPC in figure 2
		rf.commitIndex = min(args.LeaderCommit, prevLogIndex+len(args.Entries))
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.persist()

	upToDate := true // is your log more up-to-date?
	myLastLogIndex := len(rf.logs) - 1
	if args.LastLogTerm != rf.logs[myLastLogIndex].Term { // we have different terms
		upToDate = args.LastLogTerm > rf.logs[myLastLogIndex].Term // you are up-to-date if your term is larger
	} else { // we have same terms
		upToDate = args.LastLogIndex >= myLastLogIndex // you are up-to-date if you log is longer
	}

	if args.Term > rf.currentTerm { // my term is too old
		rf.currentTerm = args.Term // update my term first
		rf.voteFor = -1            // in this new term, I didn't vote for anyone
		rf.role = FOLLOWER         // convert myself to a follower, no matter what is my old role
		rf.persist()               // currentTerm and voteFor are changed, so I need to save my states
		// remember when receiving RequestVote RPC you shouldn't reset the timer!!
	}

	if args.Term < rf.currentTerm { // my term is newer
		reply.Term = rf.currentTerm // return my current term to update the sender
		reply.VoteGranted = false   // the sender is too old, I don't vote for him
		return
	}

	voteSomeoneElse := !(rf.voteFor == -1 || rf.voteFor == args.CandidateId) // did I already vote for someone else?

	if (!voteSomeoneElse) && upToDate { // either I voted for nobody, or I voted for you already, and you log is more up-to-date than mine
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		if rf.voteFor != args.CandidateId {
			rf.persist() // voteFor are changed, so I need to save my states
		}
		rf.voteFor = args.CandidateId // change my voteFor to the candidate
		rf.ResetTimer()
		return
	}

	// I already voted someone else, or you are not at least as up-to-date
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}
