package raft

// RequestVoteArgs
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

// RequestVoteReply
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

// AppendEntriesArgs
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

// AppendEntriesReply
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

// InstallSnapshotArgs
//
// AppendEntries RPC arguments structure
// field names must start with capital letters!
//
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderID          int    // to let receiver know who is the current leader
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Snapshot          []byte // raw bytes of leader's snapshot

	RPCID int64 // identifier for RPC
}

// InstallSnapshotReply
//
// InstallSnapshot RPC reply structure
// field names must start with capital letters!
//
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.LogAppendEntriesIn(*args, *reply)
	defer rf.LogAppendEntriesOut(rf.me, *args, *reply)

	rf.persist(nil)

	if args.Term < rf.currentTerm { // my term is newer
		reply.Term = rf.currentTerm // return my current term to update the sender
		reply.Success = false       // reply false if term < currentTerm
		rf.persist(nil)             // currentTerm is changed, so I need to save my states
		return
	}

	rf.resetElectionTimer() // reset timer upon AppendEntries (but if the term in arguments is outdated, you should not reset your timer!)

	if args.Term > rf.currentTerm { // my term is too old
		rf.currentTerm = args.Term // update my term first
		rf.voteFor = -1            // in this new term, I didn't vote for anyone
		rf.role = FOLLOWER         // convert myself to a follower, no matter what is my old role
		rf.persist(nil)            // currentTerm and voteFor are changed, so I need to save my states
	}

	if rf.role == CANDIDATE { // we have same term, but I'm a candidate (impossible for two leaders at same term)
		rf.role = FOLLOWER // convert myself to a follower, since this term we have a leader
		rf.persist(nil)
	}

	prevLogIndex := args.PrevLogIndex // use a local variable to store prevLogIndex for efficiency

	// my snapshot includes all entries you send me, this packet may be outdated
	if rf.lastIncludedIndex >= len(args.Entries)+prevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	if prevLogIndex < rf.lastIncludedIndex {
		// leader没snapshot，发送AppendEntries，但是这个RPC延迟了
		// leader刚刚snapshot完，发现刚才的nextIndex现在在自己的snapshot里了，因此发送InstallSnapshot，
		// 这个snapshot要比nextIndex大，follower接受之后返回
		// 这时第一个AppendEntries到达follower处，产生prevLogIndex小于follower的lastIncludeIndex的情况
		// 这种情况直接返回就可以处理？因为后面的心跳也会进行同步的 所以那个被延迟的RPC不管他应该也是可以正常同步的吧
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}
	// The following code ensures that rf.lastIncludedIndex <= prevLogIndex

	// log         : 0 1 2 3 4 5 6 7 8
	// ActualIndex:              0 1 2
	// Snapshot:     |---------|
	// lastIncludedIndex = 5, prevLogIndex = 8, actualIndex = 2
	actualIndex := prevLogIndex - rf.lastIncludedIndex - 1

	// If prevLogIndex is in snapshot, prevLogIndex is guaranteed to match, so we don't need to check
	if rf.lastIncludedIndex != prevLogIndex {
		// Only this case we need to check whetner prevLogIndex has matching term

		// case 1: I don't even have an entry at this index, so you should retry at my last entry's index
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:   1 1 1 2 2 4 4 4
		// =========================
		// Actual:         0 1 2 3 4
		// FOLLOWER:       2 2
		// Snapshot: 1 1 1
		// prevLogIndex:       |
		// prevLogIndex=5, lastIncludedIndex=2, actualIndex=2,
		// follower doesn't have log entry here, so we should return ConflictTermIsNone
		// Following code should set nextIndex to ConflictIndex, which is the length of follower's logs (4)
		// AppendEntries RPC will handle when log entry at nextIndex is already compacted, so we don't need to worry about it here
		// Return value is ConflictTermIsNone
		if len(rf.logs) <= actualIndex { // I don't have this log for this index, and I don't know whether prevLogIndex has a matched term
			reply.Term = rf.currentTerm             // same as sender's term (if newer, I returned false already; if older, I updated already)
			reply.Success = false                   // reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
			reply.ConflictTerm = ConflictTermIsNone // I don't have this entry index
			// conflictIndex is original index, so it should be len(rf.logs) (2) + lastIncludedIndex (2) +1 = 5
			conflictIndex := len(rf.logs) + rf.lastIncludedIndex + 1
			reply.ConflictIndex = conflictIndex // you have to retry with my last existing log
			return
		}

		// case 2: I have the log entry at this index, but with conflicting term
		// INDEX:    0 1 2 3 4 5 6 7
		// =========================
		// LEADER:   1 1 1 2 2 4 4 4
		// =========================
		// Actual:         0 1 2 3 4
		// FOLLOWER:       2 2 3 3 3
		// Snapshot: 1 1 1
		// prevLogIndex:         |
		// prevLogIndex=6, lastIncludedIndex=2, actualIndex=3
		// myTerm = 3, so findFirstIndex should return first log entry with term = 3, which is firstIndex = 2,
		// so we should set ConflictIndex to firstIndex+lastIncludedIndex+1, lastIncludedIndex = 2, so ConflictIndex=5
		// which is the first log entry with term = 3 for original index
		myTerm := rf.logs[actualIndex].Term // my log entry's term at caller's prevLogIndex
		if myTerm != args.PrevLogTerm {
			reply.Term = rf.currentTerm                                 // same as sender's term (if newer, I returned false already; if older, I updated already)
			reply.Success = false                                       // reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
			reply.ConflictTerm = myTerm                                 // this is the term of my conflicting log entry (this term must be older otherwise return false already)
			firstIndex := rf.findFirstIndex(myTerm)                     // find the index for the first log entry with the conflicting term
			reply.ConflictIndex = firstIndex + rf.lastIncludedIndex + 1 // the corresponding original index
			return
		}

	}

	// case 3: I have the log entry at this index, and term matches
	// INDEX:    0 1 2 3 4 5 6 7
	// =========================
	// LEADER:   1 1 1 2 2 4 4 4
	// entries:              4 4
	// =========================
	// Actual:         0 1 2 3 4
	// FOLLOWER:       2 2 4
	// Snapshot: 1 1 1
	// prevLogIndex:       |
	// if an existing entry conflicts with a new one (args.entries), delete the existing entry and all that follow it
	// append any new entries not already in the log
	for i, _ := range args.Entries {
		insertIndex := i + prevLogIndex + 1 // i+prevLogIndex <= last index of log (if len(log) <= prevLogIndex, it returns already)
		// Suppose prevLogIndex = 5, lastIncludedIndex = 2
		// insertIndex should be 6, 7, and actualIndex should be 3, 4
		insertIndex = insertIndex - rf.lastIncludedIndex - 1
		if insertIndex == len(rf.logs) { // no log at insertIndex, so we append a new entry
			rf.logs = append(rf.logs, args.Entries[i])
		} else if rf.logs[insertIndex] != args.Entries[i] { // log at insertIndex doesn't match our entry
			rf.logs = rf.logs[:insertIndex]            // delete the existing entry and all that follow it
			rf.logs = append(rf.logs, args.Entries[i]) // add current log
		} else { // log at insertIndex matches our entry
			continue
		}
	}

	rf.persist(nil) // my logs are changed, so I need to save my states

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

	rf.LogRequestVoteIn(rf.me, *args, *reply)
	defer rf.LogRequestVoteOut(rf.me, *args, *reply)

	rf.persist(nil)

	upToDate := true // is your log more up-to-date?
	myLastLogActualIndex := len(rf.logs) - 1
	// log         : 0 1 2 3 4 5 6 7 8
	// ActualIndex:              0 1 2
	// Snapshot:     |---------|
	// myLastLogActualIndex = 2, lastIncludedIndex = 5, so myLastLogIndex should be 8
	myLastLogIndex := myLastLogActualIndex + rf.lastIncludedIndex + 1
	var myTerm int
	if myLastLogActualIndex == -1 {
		myTerm = rf.lastIncludeTerm
	} else {
		myTerm = rf.logs[myLastLogActualIndex].Term
	}
	if args.LastLogTerm != myTerm { // we have different terms
		upToDate = args.LastLogTerm > myTerm // you are up-to-date if your term is larger
	} else { // we have same terms
		upToDate = args.LastLogIndex >= myLastLogIndex // you are up-to-date if you log is longer
	}

	if args.Term > rf.currentTerm { // my term is too old
		rf.currentTerm = args.Term // update my term first
		rf.voteFor = -1            // in this new term, I didn't vote for anyone
		rf.role = FOLLOWER         // convert myself to a follower, no matter what is my old role
		rf.persist(nil)            // currentTerm and voteFor are changed, so I need to save my states
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
			rf.persist(nil) // voteFor are changed, so I need to save my states
		}
		rf.voteFor = args.CandidateId // change my voteFor to the candidate
		rf.resetElectionTimer()
		return
	}

	// I already voted someone else, or you are not at least as up-to-date
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}

//
// find the first index of the given term in a raft instance's log
// don't use lock here or everything will be fucked up
//
func (rf *Raft) findFirstIndex(term int) int {
	// Due to the existence of snapshot, the first index found here might not be the actual first index of given term,
	// but that doesn't matter, because all we need is to fast-forward the decrement of nextIndex, so this is still valid
	for i := 0; i < len(rf.logs); i++ {
		if rf.logs[i].Term == term {
			return i
		}
	}
	// we know term must come from one of my log entry, so it's impossible that we cannot find first log with this term
	panic("findFirstIndex fucked up!!!")
}

// InstallSnapshot
//
// InstallSnapshot RPC handler
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.LogInstallSnapshotIn(rf.me, *args, *reply)
	defer rf.LogInstallSnapshotOut(rf.me, *args, *reply)

	rf.persist(nil)

	if args.Term < rf.currentTerm { // my term is newer
		reply.Term = rf.currentTerm // return my current term to update the sender
		rf.persist(nil)             // currentTerm is changed, so I need to save my states
		return
	}

	rf.resetElectionTimer() // InstallSnapshot also comes from leader, so we should treat it like a AppendEntries RPC

	if args.Term > rf.currentTerm { // my term is too old
		rf.currentTerm = args.Term // update my term first
		rf.voteFor = -1            // in this new term, I didn't vote for anyone
		rf.role = FOLLOWER         // convert myself to a follower, no matter what is my old role
		rf.persist(nil)            // currentTerm and voteFor are changed, so I need to save my states
	}

	if rf.role == CANDIDATE { // we have same term, but I'm a candidate (impossible for two leaders at same term)
		rf.role = FOLLOWER // convert myself to a follower, since this term we have a leader
		rf.persist(nil)
	}

	reply.Term = rf.currentTerm

	// If I already have a longer snapshot, then ignore this one (possible this one is due to network delay)
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// If my snapshot is shorter than this one, then use this one because it includes more log entries,
	// and discard any existing with a smaller index

	// case 1: no existing log entry with same index as LastIncludedIndex in Arg with matched term (log too short)
	// INDEX:          0 1 2 3 4 5 6 7 8 9
	// ===================================
	// Actual:               0 1 2 3 4
	// ===================================
	// My entries:           2 2 3
	// My snapshot:    1 1 1
	// lastIncluded:       |
	// New Snapshot:   1 1 1 2 2 4 4 4
	// New lastIncluded:             |
	// len(rf.logs) = 3, lastIncludedIndex = 2, args.LastIncludedIndex = 7
	// There is an existing log entry with same index as snapshot's last included entry, and its index is actualLastIncludedIndex,
	// actualLastIncludedIndex = 7-2-1 = 4
	actualLastIncludedIndex := args.LastIncludedIndex - rf.lastIncludedIndex - 1
	if actualLastIncludedIndex >= len(rf.logs) {
		// Discard the entire log
		rf.logs = make([]Log, 0)

		// save the snapshot, discard any existing snapshot with a smaller index
		rf.snapshot = args.Snapshot
		// At this point, we are bound to use the given snapshot, so update last included index and term
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludeTerm = args.LastIncludedTerm

		// Increment commit index, then if applier sees the new commit index, it will increment apply index and apply snapshot
		// All contents in snapshot can be regarded as commited, because anything in snapshot can't be wrong
		rf.commitIndex = rf.lastIncludedIndex

		rf.persist(rf.snapshot) // my logs are changed, so I need to save my states
		return
	}

	// case 2: no existing log entry with same index as LastIncludedIndex in Arg with matched term (term doesn't match)
	// INDEX:          0 1 2 3 4 5 6 7 8 9
	// ===================================
	// Actual:               0 1 2 3 4 5
	// ===================================
	// My snapshot:    1 1 1
	// My entries:           2 2 3 3 3 3
	// lastIncluded:       |
	// New Snapshot:   1 1 1 2 2 4 4 4
	// New lastIncluded:             |
	// len(rf.logs) = 6, lastIncludedIndex = 2, args.LastIncludedIndex = 7
	// If we reach this far, we have len(rf.logs)+rf.lastIncludedIndex > args.LastIncludedIndex
	// There is an existing log entry with same index as snapshot's last included entry, and its index is actualLastIncludedIndex,
	// actualLastIncludedIndex = 7-2-1 = 4
	if rf.logs[actualLastIncludedIndex].Term != args.LastIncludedTerm {
		// My logs after actualLastIncludedIndex are wrong, so I should discard my entire log
		// My logs before actualLastIncludedIndex are reset to the new snapshot, so they must be correct
		// My logs after actualLastIncludedIndex are discarded, and the following AppendEntries RPC will take care of them
		rf.logs = make([]Log, 0)

		// If my snapshot is shorter than this one, then use this one because it includes more log entries
		// save the snapshot, discard any existing snapshot with a smaller index
		rf.snapshot = args.Snapshot
		// At this point, we are bound to use the given snapshot, so update last included index and term
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludeTerm = args.LastIncludedTerm

		// Increment commit index, then if applier sees the new commit index, it will increment apply index and apply snapshot
		// All contents in snapshot can be regarded as commited, because anything in snapshot can't be wrong
		rf.commitIndex = rf.lastIncludedIndex

		rf.persist(rf.snapshot) // my logs are changed, so I need to save my states
		return
	}

	// case 3: exists log entry with same index as LastIncludedIndex in Arg with matched term
	// INDEX:          0 1 2 3 4 5 6 7 8 9
	// ===================================
	// Actual:               0 1 2 3 4 5
	// ===================================
	// My snapshot:    1 1 1
	// My entries:           2 2 4 4 4
	// lastIncluded:       |
	// New Snapshot:   1 1 1 2 2 4 4 4
	// New lastIncluded:             |
	// len(rf.logs) = 5, lastIncludedIndex = 2, args.LastIncludedIndex = 7
	// If we reach this far, we have len(rf.logs)+rf.lastIncludedIndex > args.LastIncludedIndex
	// There is an existing log entry with same index as snapshot's last included entry, and its index is actualLastIncludedIndex,
	// actualLastIncludedIndex = 7-2-1 = 4
	rf.logs = rf.logs[actualLastIncludedIndex+1:]
	// Use a new slice so the old array can be released
	logCopy := make([]Log, len(rf.logs))
	copy(logCopy, rf.logs)
	rf.logs = logCopy

	// If my snapshot is shorter than this one, then use this one because it includes more log entries
	// save the snapshot, discard any existing snapshot with a smaller index
	rf.snapshot = args.Snapshot
	// At this point, we are bound to use the given snapshot, so update last included index and term
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludeTerm = args.LastIncludedTerm

	// Increment commit index, then if applier sees the new commit index, it will increment apply index and apply snapshot
	// All contents in snapshot can be regarded as commited, because anything in snapshot can't be wrong
	rf.commitIndex = rf.lastIncludedIndex

	rf.persist(rf.snapshot) // my logs are changed, so I need to save my states
}
