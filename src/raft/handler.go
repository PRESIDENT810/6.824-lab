package raft

//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int   // candidate's term
	CandidateId  int   // candidate who is requesting vote
	LastLogIndex int   // index of candidate's last log entry
	LastLogTerm  int   // term of candidate's last log entry
	ID           int64 // identifier of this RPC call
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm of the receiver, for the sender to update itself
	VoteGranted bool // true means candidate received vote
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
	ID           int64 // identifier of this RPC call
}

//
// AppendEntries RPC reply structure
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// these two attributes are used for optimization when rejecting AppendEntries RPC
	ConflictTerm int // the term of the conflicting entry
	TryNextIndex int // the index of log entry you should try in the next AppendEntries RPC
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	PrintLock("=================================[Server%d] AppendEntries handler lock=================================\n", rf.me)
	rf.mu.Lock()
	rf.LogAppendEntriesReceive(args, reply)
	defer rf.mu.Unlock()
	defer PrintLock("=================================[Server%d] AppendEntries handler Unlock=================================\n", rf.me)

	if args.Term < rf.currentTerm { // my term is newer
		reply.Term = rf.currentTerm // return my current term to update the sender
		reply.Success = false       // reply false if term < currentTerm
		go rf.persist()             // currentTerm is changed, so I need to save my states
		return
	}

	go rf.ResetTimer() // reset timer upon AppendEntries (but if the term in arguments is outdated, you should not reset your timer!)

	if args.Term > rf.currentTerm { // my term is too old
		rf.currentTerm = args.Term // update my term first
		rf.voteFor = -1            // in this new term, I didn't vote for anyone
		rf.role = FOLLOWER         // convert myself to a follower, no matter what is my old role
		go rf.persist()            // currentTerm and voteFor are changed, so I need to save my states
	}

	prevLogIndex := args.PrevLogIndex // use a local variable to store prevLogIndex for efficiency

	// case 1: I don't even have a entry at this index, so you should retry at my last entry's index
	if len(rf.logs) <= prevLogIndex { // I don't have this log for this index, you have to retry with my last index
		reply.Term = rf.currentTerm           // same as sender's term (if newer, I returned false already; if older, I updated already)
		reply.Success = false                 // reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.ConflictTerm = -2               // -2 means I don't have this entry index
		reply.TryNextIndex = len(rf.logs) - 1 // you have to retry with my last existing log
		return
	}

	myTerm := rf.logs[prevLogIndex].Term // my log entry's term at caller's prevLogIndex

	// case 2: you don't have entries with myTerm, so you should retry at the index of my last entry with term=myTerm-1
	if myTerm > args.PrevLogTerm { // my log's term is bigger, so you have to retry with the index of my last term
		reply.Term = rf.currentTerm                        // same as sender's term (if newer, I returned false already; if older, I updated already)
		reply.Success = false                              // reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.ConflictTerm = myTerm                        // this is the term of my conflicting log entry
		reply.TryNextIndex = rf.findFirstIndex(myTerm) - 1 // find the index for the first log entry with term smaller than myTerm
		return
	}

	// case 3: I don't have entries with you prevLogTerm, so you should retry at the index of your last entry with term=prevLogTerm-1
	if myTerm < args.PrevLogTerm { // our log mismatch (either I don't have this log or have a log with different term)
		reply.Term = rf.currentTerm // same as sender's term (if newer, I returned false already; if older, I updated already)
		reply.Success = false       // reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.ConflictTerm = myTerm // this is the term of my conflicting log entry
		reply.TryNextIndex = -2     // you have to find this index yourself, I cannot help you
		return
	}

	lastNewEntry := prevLogIndex

	// if an existing entry conflicts with a new one (args.entries), delete the existing entry and all that follow it
	// append any new entries not already in the log
	for i, _ := range args.Entries {
		insertIndex := i + prevLogIndex + 1 // i+prevLogIndex <= last index of log (if len(log) <= prevLogIndex, it returns already)
		if insertIndex == len(rf.logs) {    // no log at insertIndex, so we append a new entry
			rf.logs = append(rf.logs, args.Entries[i])
			lastNewEntry++
		} else if rf.logs[insertIndex] != args.Entries[i] { // log at insertIndex doesn't match our entry
			rf.logs = rf.logs[:insertIndex]            // delete the existing entry and all that follow it
			rf.logs = append(rf.logs, args.Entries[i]) // add current log
			lastNewEntry++
		} else { // log at insertIndex matches our entry
			continue
		}
	}

	go rf.persist() // my logs are changed, so I need to save my states

	if args.LeaderCommit > rf.commitIndex { // rule 5 for AppendEntries RPC in figure 2
		rf.commitIndex = min(args.LeaderCommit, lastNewEntry)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	PrintLock("=================================[Server%d] RequestVote handler lock=================================\n", rf.me)
	rf.mu.Lock()
	rf.LogRequestVoteReceive(args, reply)
	defer rf.mu.Unlock()
	defer PrintLock("=================================[Server%d] RequestVote handler unlock=================================\n", rf.me)

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
		go rf.persist()            // currentTerm and voteFor are changed, so I need to save my states
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
		rf.voteFor = args.CandidateId // change my voteFor to the candidate
		if rf.voteFor != args.CandidateId {
			go rf.persist() // voteFor are changed, so I need to save my states
		}
		go rf.ResetTimer()
		return
	}

	// I already voted someone else, or you are not at least as up-to-date
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}
